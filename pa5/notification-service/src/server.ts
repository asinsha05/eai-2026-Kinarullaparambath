/**
 * PA5 notification-service — EMPTY. Student implements everything below.
 *
 * Build:
 *   1. connectWithRetry(process.env.RABBITMQ_URL) from ../../shared/rabbit,
 *      then channel.prefetch(1).
 *   2. Declare the dead letter channel and the invalid message channel,
 *      exactly as README.md "Declare what you publish to" specifies — the
 *      same declarations payment-service makes.
 *   3. On startup, load /data/processed-ids.json (create it as `[]` if it
 *      does not exist yet — this file, and /data/notification.log, are
 *      bind-mounted so they survive `docker compose restart
 *      notification-service`; see ../../docs/adr-004.md for why this is a
 *      file and not an in-memory Set).
 *   4. channel.consume("notifications.queue", async (msg) => { ... }):
 *        - correlationId = msg.properties.headers?.correlationId
 *        - If correlationId is already in the processed-ids array: ack
 *          silently, log "duplicate skipped", do NOT touch
 *          notification.log. This is the idempotent-receiver requirement.
 *          The retry path returns a message to this queue only, but
 *          delivery is still at-least-once: an ack lost after the log line
 *          was written, or a producer that publishes twice, brings the same
 *          order back — and the public tests send you exactly that.
 *        - Parse the body inside your try block. A body that is not valid
 *          JSON is permanent: publish it to orders.invalid.exchange with its
 *          original bytes and headers, persistent, ack it, never retry it.
 *        - Otherwise: append one JSON line to /data/notification.log —
 *            {"correlationId":"...","orderId":"...","customerEmail":"...","timestamp":"...","message":"Order received"}
 *          (order is the canonical order from the message body — see
 *          ../../shared/canonical-order.ts for its shape), add
 *          correlationId to processed-ids and persist the array, ack, then
 *          publish a result event to results.notification with the same
 *          correlationId header, persistent.
 *   5. Same classify-then-retry/DLQ catch block as payment-service on any
 *      thrown error.
 *
 * The line below only keeps the container process alive so `docker compose
 * up --wait` succeeds against this untouched scaffold — delete it once your
 * own channel.consume(...) registration is what keeps the process alive.
 */

// import { connectWithRetry, getRetryCount } from "../../shared/rabbit";

setInterval(() => {}, 1 << 30);
console.log("[notification-service] scaffold running, no consumer implemented yet");
