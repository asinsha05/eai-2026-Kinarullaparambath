# Patterns

The integration patterns this system really uses, and the line where each one happens.

| Pattern | Problem | Where | Why |
|---|---|---|---|
| Normalizer | Three order formats reach one endpoint | intake/src/translate.ts:193 | One POST /orders for web, mobile and b2b; the format is detected from the content type and the bytes |
| Message Translator | Each source names and encodes its fields differently | intake/src/translate.ts:109 | Every source is translated to the canonical order (my PA4 translators, now taking bytes) |
| Content Enricher | The orders carry no prices | intake/src/translate.ts:229 | Prices come from the pricing API, so a client cannot choose its own price |
| Canonical Data Model | Four services would otherwise share three formats | intake/src/server.ts:97 | Everything after intake speaks one order, validated against canonical/order.schema.json |
| Idempotent Receiver | A client retry must not create a second order | intake/src/server.ts:129 | The same Idempotency-Key and body give the same stored answer and no second event |
| Process Manager (Saga) | Four providers must succeed or be undone together | orchestrator/src/server.ts:239 | The orchestrator runs the steps in order and decides what happens on failure |
| Compensating Transaction | A failed step must undo the completed ones | orchestrator/src/server.ts:189 | Release and refund run in reverse order, with idempotency keys |
| Retry with a delay queue | A temporary failure of the first step should be retried, not lost | orchestrator/src/server.ts:341 | A nack dead-letters into a one-second TTL queue and back to saga.queue |
| Dead Letter Channel | A message that keeps failing must not block the queue | orchestrator/src/server.ts:350 | After MAX_RETRIES the message goes to orders.dlq with originQueue and lastError, ready to replay |
| Invalid Message Channel | A message that can never succeed must not be retried | orchestrator/src/server.ts:226 | Unparseable or invalid messages go to orders.invalid at once and are never replayed |