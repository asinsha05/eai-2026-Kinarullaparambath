/**
 * PA5 inventory-service — EMPTY. Student implements everything below.
 *
 * Same pattern as payment-service (see ../../payment-service/src/server.ts
 * for the worked reference implementation of the connect/consume/retry/DLQ
 * scaffolding) with these differences:
 *   - Consumes from `inventory.queue`, not `payments.queue`.
 *   - Uses INVENTORY_FAIL_RATE, not PAYMENT_FAIL_RATE.
 *   - Publishes result events to `results.inventory`, not `results.payment`.
 *   - Simulates a stock check instead of a payment validation — mechanically
 *     identical, just a different `details.message`.
 *
 * Build:
 *   1. connectWithRetry(process.env.RABBITMQ_URL) from ../../shared/rabbit,
 *      then channel.prefetch(1).
 *   2. Declare the dead letter channel and the invalid message channel,
 *      exactly as README.md "Declare what you publish to" specifies — the
 *      same declarations payment-service makes.
 *   3. channel.consume("inventory.queue", async (msg) => { ... }).
 *   4. Read correlationId from msg.properties.headers?.correlationId and
 *      retryCount from getRetryCount(msg) (also in ../../shared/rabbit).
 *   5. Parse the body inside your try block. Roll a random number against
 *      INVENTORY_FAIL_RATE — read it with `?? "10"`, not `|| 10`, or a
 *      configured 0 becomes 10. On success, ack and publish a result event
 *      to results.inventory with the same correlationId header, persistent;
 *      on failure, throw so the catch block (copy payment-service's) can
 *      handle it.
 *   6. In the catch block, classify first: a permanent failure (at minimum,
 *      a body that is not valid JSON) goes to orders.invalid.exchange with
 *      its original bytes and headers, persistent, and is acked — never
 *      retried. Anything else is temporary: on MAX_RETRIES exceeded, publish
 *      to process.env.DLQ_EXCHANGE (fanout, routing key "", headers plus
 *      originQueue, persistent), then channel.ack(msg) — ack, never leave it
 *      both in the consumer queue and the DLQ. Otherwise:
 *      channel.nack(msg, false, false) (never requeue=true — that skips the
 *      retry delay entirely).
 *
 * The line below only keeps the container process alive so `docker compose
 * up --wait` succeeds against this untouched scaffold — delete it once your
 * own channel.consume(...) registration is what keeps the process alive.
 */

// import { connectWithRetry, getRetryCount } from "../../shared/rabbit";

setInterval(() => {}, 1 << 30);
console.log("[inventory-service] scaffold running, no consumer implemented yet");
