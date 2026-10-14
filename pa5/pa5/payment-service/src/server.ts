/**
 * PA5 payment-service — the reference consumer. Connection, consume setup,
 * and the retry/DLQ logic are given below and work as-is. Three TODOs are
 * yours: declare the two channels this consumer publishes to, classify a
 * failure as permanent or temporary, and the payment simulation itself.
 * Study this file, then replicate the pattern in inventory-service and
 * notification-service.
 */

import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "payments.queue";
const RESULTS_EXCHANGE = "results.payment";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
// `?? "20"`, not `|| 20`: Number("0") is 0, and `0 || 20` is 20 — a
// configured fail rate of zero would silently become the default.
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const FAIL_RATE = Number(process.env.PAYMENT_FAIL_RATE ?? "20");

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  // TODO (student): declare the dead letter channel and the invalid message
  // channel before you consume anything — the exact names and properties are
  // in README.md, "Declare what you publish to". assertExchange, assertQueue
  // and bindQueue are idempotent, so every service may declare the same
  // channel, as long as the properties match exactly (otherwise RabbitMQ
  // refuses with PRECONDITION_FAILED). Until DLQ_EXCHANGE exists, the first
  // message that reaches the DLQ publish below closes this channel with
  // NOT_FOUND and takes the service down.

  console.log(`[Payment] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(`[Payment] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;

      // TODO (student): Implement payment validation logic
      // - Simulate success/failure based on FAIL_RATE (a random number 0-100;
      //   below FAIL_RATE means failure)
      // - On success: ack the message and publish a result event to
      //   RESULTS_EXCHANGE — headers: { correlationId }, contentType:
      //   "application/json", persistent: true, body shape:
      //     { correlationId, source: "payment", status: "success", timestamp, details: { message } }
      // - On failure: throw an Error to trigger the retry/DLQ logic below
      //
      // `order` (typed CanonicalOrder, from ../../shared/canonical-order) is
      // available here if your validation wants to look at it (e.g. reject
      // orders over some amount) — this simulation does not require it.

      throw new Error("Not implemented — replace this with your logic");
    } catch (err) {
      // TODO (student): classify the failure before you retry it. A permanent
      // failure never succeeds, however often it is retried — at minimum, a
      // body that is not valid JSON (JSON.parse above throws a SyntaxError).
      // Publish it to INVALID_EXCHANGE with the original bytes and headers,
      // persistent: true, ack it, and return: no retry, no DLQ. Anything else
      // is temporary and falls through to the retry/DLQ logic below.

      // Retry / DLQ logic (provided — study this for the other two services).
      // Do not modify.
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
