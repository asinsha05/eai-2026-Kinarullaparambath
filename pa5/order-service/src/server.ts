/**
 * PA5 order-service — accepts orders over HTTP, publishes them to the
 * orders.exchange fanout, and replays dead-lettered messages.
 */

import crypto from "node:crypto";
import express from "express";
import { connectWithRetry, withoutRetryHistory } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const ORDERS_EXCHANGE = "orders.exchange";
const DLQ_EXCHANGE = "orders.dlq.exchange";
const DLQ_QUEUE = "orders.dlq";
const RETURN_EXCHANGE = "orders.return.exchange";

// Orders kept in memory, keyed by correlationId.
const orders = new Map<string, CanonicalOrder>();

// A handful of typeof checks, not a schema library.
function looksLikeOrder(body: unknown): body is CanonicalOrder {
  if (typeof body !== "object" || body === null || Array.isArray(body)) return false;
  const o = body as Record<string, unknown>;
  return (
    typeof o.orderId === "string" &&
    typeof o.customer === "object" &&
    o.customer !== null &&
    Array.isArray(o.items) &&
    o.items.length > 0
  );
}async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue(DLQ_QUEUE, { durable: true });
  await channel.bindQueue(DLQ_QUEUE, DLQ_EXCHANGE, "");

  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  app.post("/orders", (req, res) => {
    const order: unknown = req.body;
    if (!looksLikeOrder(order)) {
      res.status(400).json({ error: "body is not a canonical order" });
      return;
    }

    // The correlation id lives in a header, never on the order body.
    const correlationId = crypto.randomUUID();

    channel.publish(ORDERS_EXCHANGE, "", Buffer.from(JSON.stringify(order)), {
      headers: { correlationId },
      contentType: "application/json",
      persistent: true,
    });

    orders.set(correlationId, order);
    res.status(201).json({ correlationId, status: "accepted" });
  });

  app.get("/orders/:correlationId", (req, res) => {
    const order = orders.get(req.params.correlationId);
    if (!order) {
      res.status(404).json({ error: "not found" });
      return;
    }
    res.status(200).json({ correlationId: req.params.correlationId, order });
  });  app.post("/dlq/replay", async (_req, res) => {
    try {
      let replayed = 0;
      const skipped: any[] = [];

      // One message at a time; ack only after republishing.
      for (;;) {
        const msg = await channel.get(DLQ_QUEUE, { noAck: false });
        if (!msg) break;

        const originQueue = msg.properties.headers?.["originQueue"] as string | undefined;
        if (!originQueue) {
          skipped.push(msg);
          continue;
        }

        // Direct return exchange, routing key = origin queue; no x-death.
        channel.publish(RETURN_EXCHANGE, originQueue, msg.content, {
          headers: withoutRetryHistory(msg.properties.headers),
          contentType: msg.properties.contentType,
          persistent: true,
        });
        channel.ack(msg);
        replayed++;
      }

      for (const msg of skipped) channel.nack(msg, false, true);

      res.status(200).json({ replayed });
    } catch (err) {
      console.error("[order-service] replay failed", err);
      res.status(500).json({ error: "replay failed" });
    }
  });

  app.listen(PORT, () => {
    console.log(`[order-service] listening on ${PORT}`);
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});