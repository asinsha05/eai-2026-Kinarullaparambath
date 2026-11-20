# Capstone

An order-processing system of four services, built from my PA4 to PA7 work: intake, orchestrator, notifier and observer.

## How to run

    cd capstone
    docker compose up -d --build --wait
    cd tests && npm ci && cd ..
    node tests/stage.mjs 6

Stop and reset everything with `docker compose down -v`.

## Services

- intake (port 8800): detects the order format, translates it, prices it, publishes order.accepted, serves GET /orders/:cid and the DLQ endpoints.
- orchestrator (port 8802): runs the saga from saga.queue, with compensation and stuck-saga handling.
- notifier (port 8803): sends a notice for failed and compensated orders.
- observer (port 8801): stores the trace hops, serves GET /trace and GET /ops/metrics.

## Known limitations

- The advanced component (Temporal mode) is not implemented.
- I checked by hand: broker reconnect, restart-proof stores, a stuck saga with operator compensation, DLQ replay and five concurrent orders. I did not build the advanced component.
- The trace and the stores are JSON and JSONL files that are rewritten or appended to without any retention.

## Advanced component
Not implemented.