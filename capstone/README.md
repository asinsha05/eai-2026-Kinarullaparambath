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
- I did not check the step 6 requirements by hand: restart-proof stores, runtime reconnect and concurrency are untested by me.
- The trace and the stores are JSON and JSONL files that are rewritten or appended to without any retention.
- The failure-analysis.md document is still the template.

## Advanced component
Not implemented.