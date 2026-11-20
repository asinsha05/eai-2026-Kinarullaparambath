# Runbook

Quick commands: `curl -s localhost:8801/ops/metrics`, `curl -s localhost:8800/dlq`, `curl -s localhost:8802/sagas/stuck`, `docker compose ps`, `docker compose logs <service> --tail 50`, the RabbitMQ UI at http://localhost:15680 (user eai).

## dlq_not_empty

**Meaning:** at least one message failed MAX_RETRIES (3) times and was dead-lettered.
**Likely causes:** (1) a provider is down: the payment mock keeps failing the first saga step, or the notification mock fails notices. (2) a poison message that always fails.
**Diagnose:** `GET /dlq` shows `originQueue`, `attempts` and `lastError` for each entry. Then `GET :8801/trace/<correlationId>` and look for `saga.dead_letter` or `notify.dead_letter` and the hops before it. Check the provider's `/admin/logs?orderId=...`.
**Fix:** fix the cause first (for a mock: `POST /admin/config` with `{"authorize":{"failMode":"never"}}` on 4801, or 4804 for notices). Then `POST :8800/dlq/replay` with `{"correlationId":"<cid>"}`. Replaying before the cause is fixed only sends the message back to the DLQ after three more attempts.
**Verify:** `dlqDepth` in `/ops/metrics` is 0, the alert clears, and `GET /orders/<cid>` shows `completed`.

## invalid_not_empty

**Meaning:** a message could not be parsed or validated, so it went to `orders.invalid` instead of being retried.
**Likely causes:** (1) a publisher sent bytes that are not JSON or not a canonical order. (2) a message without a correlationId header.
**Diagnose:** open `orders.invalid` in the RabbitMQ UI and read its payload and headers (Get messages). If it had a correlationId, `GET /trace/<cid>` has an `invalid.quarantine` hop with the reason.
**Fix:** it is never replayed (R-4.6.7), because it would fail the same way. Find the sender and fix it, then publish a corrected message through intake. Purge the entry in the UI once understood.
**Verify:** `invalidDepth` is 0 and `invalid_not_empty` stops firing.

## saga_consumer_missing

**Meaning:** nobody is consuming `saga.queue`: orders are accepted but nothing runs.
**Likely causes:** (1) the orchestrator container is down or restarting. (2) it lost its broker connection and has not yet reconnected.
**Diagnose:** `docker compose ps` for cap-orchestrator, then `docker compose logs orchestrator --tail 50`; `GET :8802/health` answers 503 while it is not consuming.
**Fix:** `docker compose up -d orchestrator`. If it started but stays degraded, check that the broker is healthy and restart the orchestrator.
**Verify:** `saga.queue` shows 1 consumer in `/ops/metrics`, `ready` drains, and `GET :8802/health` answers 200.

## notifier_consumer_missing

**Meaning:** nobody is consuming `notifications.queue`: failure notices pile up.
**Likely causes:** (1) the notifier container is down. (2) its broker connection dropped.
**Diagnose:** `docker compose ps`, `docker compose logs notifier --tail 50`, `GET :8803/health`.
**Fix:** `docker compose up -d notifier`.
**Verify:** `notifications.queue` shows 1 consumer, the backlog drains, and the notices appear in the notification mock's `/admin/effects`.

## saga_consumer_lag

**Meaning:** more than 5 messages are ready in `saga.queue`: the orchestrator is behind.
**Threshold, justified:** 5 equals SAGA_CONCURRENCY, so a burst of 5 orders is processed at once and 5 ready messages is a normal burst. More than that means a whole batch is waiting behind running sagas, and a saga takes about one provider round trip per step (a slow provider at 2 s makes it 6 to 8 s).
**Likely causes:** (1) a slow provider, for example shipping with `delayMs` set. (2) a burst of orders, or one orchestrator that is stuck.
**Diagnose:** `GET /ops/metrics` for `ready` and `unacked`; `GET :8801/trace/<cid>` for a slow hop; the mocks' `/admin/config` for a `delayMs`.
**Fix:** remove the slowness at the provider (reset the mock) or wait out the burst. SAGA_CONCURRENCY can be raised if the providers can take it.
**Verify:** `ready` falls back to 0 and the alert clears.

## notifier_consumer_lag

**Meaning:** more than 5 failure notices are waiting in `notifications.queue`.
**Threshold, justified:** 5, because the notifier handles one notice at a time (prefetch 1) and each costs one HTTP call to the notification provider; with a 2.5 s timeout, 5 waiting notices is already more than 10 seconds behind.
**Likely causes:** (1) the notification provider is slow or failing, so each notice takes the retry path. (2) many orders failed at once.
**Diagnose:** `/ops/metrics`, `docker compose logs notifier`, and `GET :4804/admin/config` on the notification mock.
**Fix:** fix the provider; the retry path (1 s TTL) re-delivers by itself. Replay anything in the DLQ with `POST /dlq/replay`.
**Verify:** `ready` is 0 and each notice appears once in the mock's `/admin/effects`.

## stuck_sagas

**Meaning:** at least one saga is `needs_attention` or `compensation_failed`: an operator has to decide.
**Likely causes:** (1) the orchestrator was killed while a saga was running. (2) a compensation call (refund or release) failed.
**Diagnose:** `GET :8802/sagas/stuck` shows the completed and compensated steps. Then `GET :8801/trace/<cid>` and the mocks' `/admin/effects?orderId=...` to see what really happened; a shipment or notice that already went out cannot be undone.
**Fix:** if the provider failed, fix it first. Then `POST :8802/sagas/<cid>/compensate`; it runs only the compensations that are still outstanding and is safe to repeat.
**Verify:** the order is `compensated` in `GET /orders/<cid>`, the refund and release each appear once in `/admin/effects`, and `stuckSagas` is 0.