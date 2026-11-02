/**
 * PA5 inventory-service — same pattern as payment-service.
 */

import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "inventory.queue";
const RESULTS_EXCHANGE = "results.inventory";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
// `?? "10"`, not `|| 10`: a configured 0 must stay 0.
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.INVENTORY_FAIL_RATE ?? "10");

// A failure that will repeat identically on every attempt: never retried.
class PermanentError extends Error {}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  // Same properties as in every other service (PRECONDITION_FAILED otherwise).
  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  console.log(`[Inventory] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(`[Inventory] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      if (!correlationId) {
                throw new PermanentError("missing correlationId header");
      }

      // Parsed INSIDE the try: invalid JSON throws a SyntaxError (permanent).
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;

      if (typeof order !== "object" || order === null || Array.isArray(order)) {
        throw new PermanentError("body is not an order object");
      }

      // Temporary failure: simulated.
      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Inventory system unavailable (simulated)");
      }

      const result = {
        correlationId,
        source: "inventory",
        status: "success",
        timestamp: new Date().toISOString(),
        details: { message: "Stock reserved" },
      };
      channel.publish(RESULTS_EXCHANGE, "", Buffer.from(JSON.stringify(result)), {
        headers: { correlationId },
        contentType: "application/json",
        persistent: true,
      });
      channel.ack(msg);
      console.log(`[Inventory] Done ${correlationId}`);
    } catch (err) {
      if (err instanceof SyntaxError || err instanceof PermanentError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Inventory] -> INVALID (no retry): ${(err as Error).message}`);
        return;
      }

      // Retry / DLQ logic (copied from payment-service).
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Inventory] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Inventory] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);