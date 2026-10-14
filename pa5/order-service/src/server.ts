/**
 * PA5 order-service — EMPTY. Student implements everything below.
 *
 * Build:
 *   1. POST /orders
 *        - Body: a canonical order (../../canonical/order.schema.json,
 *          typed here as CanonicalOrder). It already has an orderId — you
 *          are not the translator, PA4 was. You do NOT add a
 *          `correlationId` field to the order object itself: the schema's
 *          additionalProperties is false, so the order body is republished
 *          exactly as received. Generate a correlationId (crypto.randomUUID()
 *          — built into Node 20, no dependency needed) and carry it ONLY in
 *          the AMQP message's custom `headers.correlationId` — see
 *          ../../docs/adr-004.md for why not the native `properties.correlationId`.
 *        - Publish to the `orders.exchange` fanout exchange:
 *            channel.publish("orders.exchange", "", Buffer.from(JSON.stringify(order)), {
 *              headers: { correlationId },
 *              contentType: "application/json",
 *              persistent: true,
 *            });
 *          (exchange first, routing key second — an empty string routing
 *          key for a fanout exchange, which ignores it anyway. persistent:
 *          the queues are durable, but a non-persistent message in a durable
 *          queue is still lost when the broker restarts.)
 *        - Keep the order in memory, keyed by correlationId, so GET below
 *          can find it again.
 *        - Respond 201 with { correlationId, status: "accepted" }.
 *   2. GET /orders/:correlationId
 *        - 200 with { correlationId, order } if found, 404 otherwise.
 *   3. POST /dlq/replay — send every message in `orders.dlq` back to the
 *      consumer that failed it:
 *        - Take messages one at a time with channel.get("orders.dlq").
 *        - The `originQueue` header says which consumer queue it came from.
 *          Republish the original bytes to `orders.return.exchange` (direct)
 *          with that queue name as the routing key — so only that consumer
 *          sees it again, not all three.
 *        - Headers: withoutRetryHistory(msg.properties.headers) from
 *          ../../shared/rabbit keeps correlationId and drops x-death, so the
 *          replayed message gets a fresh set of attempts. persistent: true.
 *        - Ack each message from orders.dlq only after republishing it.
 *        - Respond 200 with { replayed: <number of messages sent back> }.
 *   4. GET /health — 200 with { status: "ok" }.
 *   5. Connect to RabbitMQ with connectWithRetry from ../../shared/rabbit
 *      before accepting traffic that needs to publish, and declare the dead
 *      letter channel (README.md, "Declare what you publish to") — replay
 *      reads from orders.dlq, and channel.get on a queue nobody has
 *      declared yet closes the channel with NOT_FOUND.
 *
 * The listener below is scaffold plumbing only, so `docker compose up
 * --wait` succeeds against this file untouched — it is not part of the
 * assignment and you should delete it once your own app.listen(...) call
 * is what keeps the process alive.
 */

// import express from "express";
// import { connectWithRetry } from "../../shared/rabbit";
// import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;

// Placeholder so `docker compose up --wait` (no HTTP routes exist yet) does
// not exit immediately with an empty scaffold — remove once app.listen(...)
// is in place.
setInterval(() => {}, 1 << 30);
console.log(`[order-service] scaffold running, no routes implemented yet (would listen on ${PORT})`);
