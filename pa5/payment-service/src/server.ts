/**
 * PA5 payment-service — the reference consumer.
 */

import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "payments.queue";
const RESULTS_EXCHANGE = "results.payment";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
// `?? "20"`, not `|| 20`: Number("0") is 0, and `0 || 20` is 20.
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.PAYMENT_FAIL_RATE ?? "20");

// A failure that will repeat identically on every attempt: never retried.
class PermanentError extends Error {}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  // Declare the two channels this consumer publishes to. Properties must be
  // exactly the same in every service (PRECONDITION_FAILED otherwise).
  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");

  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  console.log(`[Payment] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(`[Payment] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      // Permanent: without a correlation id the lifecycle cannot be traced.
      if (!correlationId) {
        throw new PermanentError("missing correlationId header");
      }

      // Parsed INSIDE the try: invalid JSON throws a SyntaxError (permanent).
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;

      // Permanent: valid JSON that is not an order object.
      if (typeof order !== "object" || order === null || Array.isArray(order)) {
        throw new PermanentError("body is not an order object");
      }

      // Temporary failure: simulated, may succeed on the next attempt.
      if (Math.random() * 100 < FAIL_RATE) {
        throw new Error("Payment provider unavailable (simulated)");
      }

      // Success: publish the result first, then ack.
      const result = {
        correlationId,
        source: "payment",
        status: "success",
        timestamp: new Date().toISOString(),
        details: { message: "Payment accepted" },
      };
      channel.publish(RESULTS_EXCHANGE, "", Buffer.from(JSON.stringify(result)), {
        headers: { correlationId },
        contentType: "application/json",
        persistent: true,
      });
      channel.ack(msg);
      console.log(`[Payment] Done ${correlationId}`);
    } catch (err) {
      // Classify: permanent failures never succeed, however often retried.
      if (err instanceof SyntaxError || err instanceof PermanentError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Payment] -> INVALID (no retry): ${(err as Error).message}`);
        return;
      }

      // Retry / DLQ logic (provided). Do not modify.
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          // originQueue tells a later replay which queue to send it back to.
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Payment] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Payment] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);