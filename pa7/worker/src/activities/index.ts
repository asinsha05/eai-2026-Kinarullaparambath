import { ApplicationFailure } from "@temporalio/activity";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

async function post(url: string, body: unknown): Promise<void> {
  let res: Response;
  try {
    res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (e) {
    throw ApplicationFailure.retryable(
      `no answer from ${url}: ${(e as Error).message}`,
      "network_error",
    );
  }
  if (res.ok) return;

  let code = `http_${res.status}`;
  try {
    const data: any = await res.json();
    code = data.code ?? data.error ?? code;
  } catch {
    /* body was not JSON */
  }

  if (res.status >= 400 && res.status < 500) {
    throw ApplicationFailure.nonRetryable(`${url} answered ${res.status}`, code);
  }
  throw ApplicationFailure.retryable(`${url} answered ${res.status}`, code);
}

export async function authorizePayment(orderId: string, amount: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/authorize`, { orderId, amount });
}

export async function reserveInventory(orderId: string, items: CanonicalItem[]): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/reserve`, { orderId, items });
}

export async function createShipment(orderId: string): Promise<void> {
  await post(`${SHIPPING_URL}/shipping/create`, { orderId });
}

export async function sendNotification(orderId: string, recipient: string): Promise<void> {
  await post(`${NOTIFICATION_URL}/notification/send`, { orderId, recipient });
}

export async function refundPayment(orderId: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/refund`, { orderId });
}

export async function releaseInventory(orderId: string): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/release`, { orderId });
}
