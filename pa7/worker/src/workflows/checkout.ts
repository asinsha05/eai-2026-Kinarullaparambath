import {
  proxyActivities,
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
} from "@temporalio/workflow";
import type * as activities from "../activities";
import type { CanonicalOrder, CheckoutResult, TraceItem } from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

function toCents(price: string): number {
  const [whole = "0", frac = ""] = price.split(".");
  return parseInt(whole, 10) * 100 + parseInt((frac + "00").slice(0, 2), 10);
}

function formatCents(cents: number): string {
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

async function step(
  trace: TraceItem[],
  name: string,
  fn: () => Promise<unknown>,
): Promise<string | null> {
  const start = new Date();
  let code: string | null = null;
  let status = "success";
  try {
    await fn();
  } catch (err) {
    if (err instanceof ActivityFailure && err.cause instanceof TimeoutFailure) {
      status = "timeout";
      code = "timeout";
    } else if (err instanceof ActivityFailure && err.cause instanceof ApplicationFailure) {
      status = "failed";
      code = err.cause.type ?? "unknown";
    } else {
      status = "failed";
      code = "unknown";
    }
  }
  const end = new Date();
  trace.push({
    step: name,
    status,
    startedAt: start.toISOString(),
    finishedAt: end.toISOString(),
    durationMs: end.getTime() - start.getTime(),
  });
  return code;
}

export async function checkout(order: CanonicalOrder): Promise<CheckoutResult> {
  const { orderId, items, customer } = order;
  const trace: TraceItem[] = [];

  const cents = items.reduce((sum, i) => sum + toCents(i.unitPrice) * i.quantity, 0);
  const amount = formatCents(cents);

  const payFail = await step(trace, "payment", () => act.authorizePayment(orderId, amount));
  if (payFail) return { orderId, status: "failed", code: payFail, trace };

  let reserved = false;
  const later: [string, () => Promise<unknown>][] = [
    ["inventory", () => act.reserveInventory(orderId, items)],
    ["shipping", () => act.createShipment(orderId)],
    ["notification", () => act.sendNotification(orderId, customer.email)],
  ];

  for (const [name, fn] of later) {
    const fail = await step(trace, name, fn);
    if (!fail) {
      if (name === "inventory") reserved = true;
      continue;
    }

    let compensated = true;
    if (reserved) {
      const relFail = await step(trace, "inventory_release", () => act.releaseInventory(orderId));
      if (relFail) compensated = false;
    }
    const refFail = await step(trace, "payment_refund", () => act.refundPayment(orderId));
    if (refFail) compensated = false;

    return compensated
      ? { orderId, status: "compensated", code: fail, trace }
      : { orderId, status: "failed", code: "compensation_failed", trace };
  }

  return { orderId, status: "completed", trace };
}
