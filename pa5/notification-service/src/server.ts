/**
 * PA5 notification-service — idempotent receiver.
 * The correlationId is the idempotency key. Processed ids are persisted in
 * /data/processed-ids.json (bind-mounted), so dedup survives a restart.
 */

import { appendFileSync, existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.NOTIFICATION_FAIL_RATE ?? "0");

const DATA_DIR = "/data";
const PROCESSED_FILE = `${DATA_DIR}/processed-ids.json`;
const LOG_FILE = `${DATA_DIR}/notification.log`;

class PermanentError extends Error {}

// Loaded from disk at startup: a restart must not forget what was processed.
function loadProcessed(): Set<string> {
  try {
    if (existsSync(PROCESSED_FILE)) {
      const parsed = JSON.parse(readFileSync(PROCESSED_FILE, "utf8"));
      if (Array.isArray(parsed)) return new Set(parsed.map(String));
    }
  } catch (err) {
    console.warn(`[Notification] could not read ${PROCESSED_FILE}: ${(err as Error).message}`);
  }
  return new Set();
}

// Write to a temp file then rename, so a crash never leaves half a file.
function persistProcessed(ids: Set<string>): void {
  const tmp = `${PROCESSED_FILE}.tmp`;
  writeFileSync(tmp, JSON.stringify([...ids]), "utf8");
  renameSync(tmp, PROCESSED_FILE);
}

async function main(): Promise<void> {
  mkdirSync(DATA_DIR, { recursive: true });
  const processed = loadProcessed();
  if (!existsSync(PROCESSED_FILE)) persistProcessed(processed);

  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  console.log(`[Notification] Consuming from ${QUEUE}, ${processed.size} ids already processed`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(`[Notification] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      if (!correlationId) {
        throw new PermanentError("missing correlationId header");
      }

      // Parsed INSIDE the try: invalid JSON throws a SyntaxError (permanent).
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;

      if (typeof order !== "object" || order === null || Array.isArray(order)) {
        throw new PermanentError("body is not an order object");
      }

      // Idempotent receiver: seen before -> ack silently, no log line.
      if (processed.has(correlationId)) {
        channel.ack(msg);
        console.log(`[Notification] duplicate skipped ${correlationId}`);
        return;
      }

      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Notification gateway unavailable (simulated)");
      }

      const line = {
        correlationId,
        orderId: order.orderId,
        customerEmail: order.customer?.email,
        timestamp: new Date().toISOString(),
        message: "Order received",
      };
      appendFileSync(LOG_FILE, JSON.stringify(line) + "\n", "utf8");

      processed.add(correlationId);
      persistProcessed(processed);

      const result = {
        correlationId,
        source: "notification",
        status: "success",
        timestamp: new Date().toISOString(),
        details: { message: "Notification sent" },
      };
      channel.publish(RESULTS_EXCHANGE, "", Buffer.from(JSON.stringify(result)), {
        headers: { correlationId },
        contentType: "application/json",
        persistent: true,
      });
      channel.ack(msg);
      console.log(`[Notification] Done ${correlationId}`);
    } catch (err) {
      if (err instanceof SyntaxError || err instanceof PermanentError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> INVALID (no retry): ${(err as Error).message}`);
        return;
      }

      // Retry / DLQ logic (copied from payment-service).
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Notification] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);