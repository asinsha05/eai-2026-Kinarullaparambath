# Failure analysis

Ten scenarios, each run against my own stack. The traces are the real output of GET /trace/<cid> from those runs (service, operation, status, detail). Scenarios 1 to 5 were produced by a script that sets the failure on a mock, submits a web order and reads the trace; scenarios 6 to 10 likewise, and number 10 publishes the bytes directly to the broker.

Alerts follow README 4.9. The traces show only the hops each run produced.

## 1. Payment declined

**Trigger:** payment answers 402 on every authorize call (POST :4801/admin/config), then a web order

**What the system does:** the trace of this run (GET /trace/215d4d24-3320-49b5-9e08-b7807acd714a):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator payment failed {"code": "payment_declined"}
orchestrator saga.outcome success {"state": "failed", "code": "payment_declined"}
notifier notify.consume success
notifier notify.send success
```

**Final state:** `{"correlationId":"215d4d24-3320-49b5-9e08-b7807acd714a","businessKey":"web:WEB-1112726387","orderId":"WEB-1112726387","source":"web","orderType":"standard","state":"failed","updatedAt":"2026-11-20T18:06:50.793Z","code":"payment_declined"}`

**Alert:** none, by design: nothing is stuck and nothing is in a queue, the order ends failed with the provider's code

**Operator action:** tell the customer; nothing to undo. GET /orders/<cid> shows the code.

## 2. Inventory unavailable

**Trigger:** inventory fails every reserve call, then a web order

**What the system does:** the trace of this run (GET /trace/e0195df2-e898-4079-89aa-1096c8cb9768):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator payment success
orchestrator inventory failed {"code": "inventory_unavailable"}
orchestrator payment_refund success
orchestrator saga.outcome success {"state": "compensated", "code": "inventory_unavailable"}
notifier notify.consume success
notifier notify.send success
```

**Final state:** `{"correlationId":"e0195df2-e898-4079-89aa-1096c8cb9768","businessKey":"web:WEB-428324170","orderId":"WEB-428324170","source":"web","orderType":"standard","state":"compensated","updatedAt":"2026-11-20T18:06:55.128Z","code":"inventory_unavailable"}`

**Alert:** none, by design: the saga compensated by itself and ended in a terminal state

**Operator action:** check POST :4801 /admin/effects shows one charge and one refund (it does: {"service":"payment","effects":[{"seq":1,"at":"2026-11-20T18:06:55.113Z","type":"charge","action":"authorize","orderId":"WEB-428324170","correlationId":"e0195df2-e898-4079-89aa-1096c8cb9768","causationId":"bee2ad7c-54d8-4be8-ac7d-da23d537646b","idempotencyKey":"authorize:web:WEB-428324170","amount":"90.30"},{"seq":2,"at":"2026-11-20T18:06:55.123Z","type":"refund","action":"refund","orderId":"WEB-428324170","correlationId":"e0195df2-e898-4079-89aa-1096c8cb9768","causationId":"c1cc9127-711b-4d5a-b0a5-65063026ce7a","idempotencyKey":"refund:web:WEB-428324170","amount":null}]})

## 3. Shipping timeout

**Trigger:** shipping delays 3500 ms, longer than REQUEST_TIMEOUT_MS (2500)

**What the system does:** the trace of this run (GET /trace/17f2afef-3629-4936-b310-1c7e7811681f):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator payment success
orchestrator inventory success
orchestrator shipping timeout {"code": "timeout"}
orchestrator inventory_release success
orchestrator payment_refund success
notifier notify.consume success
notifier notify.send success
orchestrator saga.outcome success {"state": "compensated", "code": "timeout"}
```

**Final state:** `{"correlationId":"17f2afef-3629-4936-b310-1c7e7811681f","businessKey":"web:WEB-3202524651","orderId":"WEB-3202524651","source":"web","orderType":"standard","state":"compensated","updatedAt":"2026-11-20T18:07:02.861Z","code":"timeout"}`

**Alert:** none, by design: the saga ended compensated with code timeout

**Operator action:** the shipment may still exist at the carrier: a delayed call performs its effect when the delay ends. Check :4803 /admin/effects before telling the customer.

## 4. A refund fails, then operator compensation

**Trigger:** the refund fails (500) after inventory failed, so the first compensation attempt cannot complete. State before the fix: `{"correlationId":"7bba1b3f-ea6a-422b-851b-2f0bfc18e48d","businessKey":"web:WEB-1197810772","orderId":"WEB-1197810772","source":"web","orderType":"standard","state":"compensation_failed","updatedAt":"2026-11-20T18:07:09.574Z","code":"service_unavailable"}`. Then the refund is fixed and POST :8802/sagas/<cid>/compensate runs.

**What the system does:** the trace of this run (GET /trace/7bba1b3f-ea6a-422b-851b-2f0bfc18e48d):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator payment success
orchestrator inventory failed {"code": "inventory_unavailable"}
orchestrator payment_refund failed {"code": "service_unavailable"}
orchestrator saga.outcome success {"state": "compensation_failed", "code": "service_unavailable"}
orchestrator saga.operator_compensate success
orchestrator payment_refund success
notifier notify.consume success
notifier notify.send success
orchestrator saga.outcome success {"state": "compensated", "code": "inventory_unavailable"}
```

**Final state:** `{"correlationId":"7bba1b3f-ea6a-422b-851b-2f0bfc18e48d","businessKey":"web:WEB-1197810772","orderId":"WEB-1197810772","source":"web","orderType":"standard","state":"compensated","updatedAt":"2026-11-20T18:07:15.611Z","code":"inventory_unavailable"}`

**Alert:** stuck_sagas fires while the order is compensation_failed (stuckSagas > 0 in /ops/metrics) and clears after the compensation

**Operator action:** fix the refund provider, then POST :8802/sagas/<cid>/compensate (answer: `{"correlationId":"7bba1b3f-ea6a-422b-851b-2f0bfc18e48d","state":"compensated"}`).

## 5. Payment outage before step 1 (retry, parked, replay)

**Trigger:** payment answers 503 to every authorize call. State after the retries: `{"correlationId":"96046649-6ba5-4ba5-ad1d-dadf91808673","businessKey":"web:WEB-967715300","orderId":"WEB-967715300","source":"web","orderType":"standard","state":"parked","updatedAt":"2026-11-20T18:07:19.857Z"}`. Then the provider is fixed and POST :8800/dlq/replay is called.

**What the system does:** the trace of this run (GET /trace/96046649-6ba5-4ba5-ad1d-dadf91808673):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator payment failed {"code": "service_unavailable"}
orchestrator saga.retry retried {"code": "service_unavailable"}
orchestrator saga.consume success
orchestrator payment failed {"code": "service_unavailable"}
orchestrator saga.retry retried {"code": "service_unavailable"}
orchestrator saga.consume success
orchestrator payment failed {"code": "service_unavailable"}
orchestrator saga.dead_letter dead_lettered {"lastError": "payment 503"}
intake dlq.replay success {"originQueue": "saga.queue"}
orchestrator saga.consume success
orchestrator payment success
orchestrator inventory success
orchestrator shipping success
orchestrator notification success
orchestrator saga.outcome success {"state": "completed"}
```

**Final state:** `{"correlationId":"96046649-6ba5-4ba5-ad1d-dadf91808673","businessKey":"web:WEB-967715300","orderId":"WEB-967715300","source":"web","orderType":"standard","state":"completed","updatedAt":"2026-11-20T18:07:29.937Z"}`

**Alert:** dlq_not_empty fires while the order is parked, and clears after the replay

**Operator action:** fix the provider, then POST :8800/dlq/replay with the correlationId (answer: `{"replayed":1,"remaining":0}`).

## 6. A failure-notice delivery outage

**Trigger:** the order fails (payment 402) and the notification provider answers 503 to every notice. GET /dlq then shows: `{"count":1,"entries":[{"messageId":"ce445e5b-7513-4b29-8962-afaafa9e8663","correlationId":"c4663dbb-59f2-461d-be7a-2077a96b0aa0","originQueue":"notifications.queue","attempts":3,"lastError":"notification 503","deadLetteredAt":"2026-11-20T18:09:12.508Z"}]}`. Then the provider is fixed and the entry is replayed.

**What the system does:** the trace of this run (GET /trace/c4663dbb-59f2-461d-be7a-2077a96b0aa0):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator payment failed {"code": "payment_declined"}
orchestrator saga.outcome success {"state": "failed", "code": "payment_declined"}
notifier notify.consume success
notifier notify.send failed {"error": "notification 503"}
notifier notify.retry retried {"error": "notification 503"}
notifier notify.consume success
notifier notify.send failed {"error": "notification 503"}
notifier notify.retry retried {"error": "notification 503"}
notifier notify.consume success
notifier notify.send failed {"error": "notification 503"}
notifier notify.dead_letter dead_lettered {"lastError": "notification 503"}
intake dlq.replay success {"originQueue": "notifications.queue"}
notifier notify.consume success
notifier notify.send success
```

**Final state:** `{"correlationId":"c4663dbb-59f2-461d-be7a-2077a96b0aa0","businessKey":"web:WEB-884619044","orderId":"WEB-884619044","source":"web","orderType":"standard","state":"failed","updatedAt":"2026-11-20T18:09:10.481Z","code":"payment_declined"}`

**Alert:** dlq_not_empty fires while the notice sits in the DLQ, and clears after the replay

**Operator action:** fix the notification provider, then POST :8800/dlq/replay with the correlationId (answer: `{"replayed":1,"remaining":0}`). The notice is at-least-once: a second one can be sent if the provider had already sent it.

## 7. The orchestrator crashes mid-saga

**Trigger:** a web order while shipping is slow (6 s), then docker kill cap-orchestrator one second later and docker start. GET :8802/sagas/stuck then shows: `{"sagas":[{"correlationId":"49469b28-770b-4c9b-a7dc-4e8202065260","businessKey":"web:WEB-2883615959","state":"needs_attention","completedSteps":["payment","inventory"],"compensatedSteps":[],"since":"2026-11-20T18:09:29.566Z"}]}`

**What the system does:** the trace of this run (GET /trace/49469b28-770b-4c9b-a7dc-4e8202065260):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator payment success
orchestrator inventory success
orchestrator saga.consume success
orchestrator saga.dedupe skipped {"reason": "redelivery"}
orchestrator saga.outcome success {"republished": true}
orchestrator saga.operator_compensate success
orchestrator inventory_release success
orchestrator payment_refund success
notifier notify.consume success
notifier notify.send success
orchestrator saga.outcome success {"state": "compensated", "code": "operator_compensated"}
```

**Final state:** `{"correlationId":"49469b28-770b-4c9b-a7dc-4e8202065260","businessKey":"web:WEB-2883615959","orderId":"WEB-2883615959","source":"web","orderType":"standard","state":"compensated","updatedAt":"2026-11-20T18:09:54.381Z","code":"operator_compensated"}`

**Alert:** stuck_sagas fires while the order is needs_attention, and clears after the operator compensation

**Operator action:** check the mocks' /admin/effects (the shipment may exist), then POST :8802/sagas/<cid>/compensate (answer: `{"correlationId":"49469b28-770b-4c9b-a7dc-4e8202065260","state":"compensated"}`). Nothing was re-run automatically.

## 8. A broker restart

**Trigger:** docker restart cap-rabbitmq. After 45 s the health endpoints answer intake 200, orchestrator 200, notifier 200, observer 200 without any container restart. Then a new web order is submitted.

**What the system does:** the trace of this run (GET /trace/e218929b-566b-4f9e-a4aa-217e801f13f8):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator payment success
orchestrator inventory success
orchestrator shipping success
orchestrator notification success
orchestrator saga.outcome success {"state": "completed"}
```

**Final state:** `{"correlationId":"e218929b-566b-4f9e-a4aa-217e801f13f8","businessKey":"web:WEB-188715666","orderId":"WEB-188715666","source":"web","orderType":"standard","state":"completed","updatedAt":"2026-11-20T18:10:43.054Z"}`

**Alert:** while the broker is down saga_consumer_missing and notifier_consumer_missing would fire, but the observer cannot read the broker either, so /ops/metrics answers 503 at that moment; after the reconnect no alert fires

**Operator action:** none needed: every service reconnects by itself within seconds. Check docker compose ps shows no restarts and GET /orders/<cid> reaches completed.

## 9. A duplicate submission with a new Idempotency-Key

**Trigger:** the same order (same orderId and source) is submitted twice with two different keys. The first correlationId is b2a9efe2-388c-48c0-9eac-6b66acecd75c; the second trace below belongs to the duplicate.

**What the system does:** the trace of this run (GET /trace/5fb4b2fc-8204-46b6-a04e-e11dc1b2f609):

```
intake intake.receive success
intake intake.translate success
intake intake.enrich success
intake intake.publish success
orchestrator saga.consume success
orchestrator saga.dedupe duplicate {"duplicateOf": "b2a9efe2-388c-48c0-9eac-6b66acecd75c"}
```

**Final state:** `{"correlationId":"5fb4b2fc-8204-46b6-a04e-e11dc1b2f609","businessKey":"web:WEB-DUP26955","orderId":"WEB-DUP26955","source":"web","orderType":"standard","state":"duplicate","updatedAt":"2026-11-20T18:10:54.290Z","duplicateOf":"b2a9efe2-388c-48c0-9eac-6b66acecd75c"}`

**Alert:** none, by design: a duplicate is a terminal, expected state

**Operator action:** none: the second order is marked duplicate with duplicateOf pointing at the first (b2a9efe2-388c-48c0-9eac-6b66acecd75c), and no step ran. A customer who orders again after a decline must use a new orderId.

## 10. A malformed message

**Trigger:** the bytes `{ not json` are published to orders.exchange with rabbitmqadmin.

**What the system does:** the orchestrator parses the message inside its try, sees it is not JSON, and publishes the original bytes and headers to orders.invalid.exchange. It acks the message and never retries it. A message without a correlationId has no trace; if it had one, a `invalid.quarantine` hop with status `quarantined` would appear in GET /trace/<cid>. Observed right after: `invalidDepth=1, invalid_not_empty firing=True`.

**Final state:** the message sits in the queue orders.invalid with its original bytes. No order exists, so there is no GET /orders entry.

**Alert:** invalid_not_empty fires (invalidDepth > 0).

**Operator action:** read the message in the RabbitMQ UI (http://localhost:15680, queue orders.invalid, Get messages), find the sender and fix it. It is never replayed: it would fail the same way. Purge the queue once understood.

