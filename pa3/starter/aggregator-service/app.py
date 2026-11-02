in_flight = {}     # orderId -> {"total": int, "results": {itemIndex: result}, "last": float}
finished = set()   # orderIds already completed or timed out (tombstones)
lock = threading.Lock()


def build_message(order_id, state, status):
    results = state["results"]
    missing = sorted(set(range(state["total"])) - set(results))
    return {
        "orderId": order_id,
        "correlationId": order_id,
        "status": status,
        "totalItems": state["total"],
        "receivedItems": len(results),
        "itemResults": [results[i] for i in sorted(results)],
        "missingItemIndexes": missing,
    }


def aggregate_result(ch, method, properties, body):
    to_publish = None
    try:
        result = json.loads(body)
        order_id = result['orderId']
        idx = int(result['itemIndex'])
        total = int(result['totalItems'])
        if not (0 <= idx < total):
            raise ValueError(f"itemIndex {idx} outside 0..{total - 1}")
    except (ValueError, KeyError, TypeError) as e:
        print(f"[Aggregator] Dropping bad result: {e!r} body={body[:200]!r}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    with lock:
        if order_id in finished:
            print(f"[Aggregator] Late result for finished order {order_id}, item {idx}; ignored")
        else:
            state = in_flight.setdefault(
                order_id, {"total": total, "results": {}, "last": 0.0})
            if idx in state["results"]:
                print(f"[Aggregator] Duplicate {order_id}/{idx}; ignored")
            else:
                state["results"][idx] = result
            state["last"] = time.monotonic()
            if len(state["results"]) == state["total"]:
                to_publish = build_message(order_id, state, "complete")
                del in_flight[order_id]
                finished.add(order_id)

    if to_publish:
        publish_completion(to_publish)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)
        expired = []
        with lock:
            now = time.monotonic()
            for order_id, state in list(in_flight.items()):
                if now - state["last"] > IDLE_TIMEOUT_SECONDS:
                    expired.append(build_message(order_id, state, "partial"))
                    del in_flight[order_id]
                    finished.add(order_id)
        for msg in expired:
            try:
                publish_completion(msg)
            except Exception as e:
                print(f"[Aggregator] Failed to publish partial for {msg['orderId']}: {e!r}")