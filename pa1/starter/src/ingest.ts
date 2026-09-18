/**
 * PA1 — legacy file ingestion.
 *
 * You are reading two files that a system you do not control exports for you:
 *
 *   data/orders-20260901.txt   fixed-width, CP1257 ("windows-1257")
 *   data/customers.csv         semicolon-separated, UTF-8
 *
 * Everything you need is in the Node standard library. No parsing, CSV or
 * encoding dependency is used here.
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

export interface Order {
  orderId: string;
  customerId: string;
  customerName: string;
  orderDate: string;
  amount: string;
  currency: string;
}

export interface RejectedRecord {
  line: number;
  raw: string;
  reason: string;
}

export interface Report {
  orders: Order[];
  rejected: RejectedRecord[];
  unmatchedCustomers: string[];
}

export interface IngestOptions {
  ordersPath: string;
  customersPath: string;
}

export const ORDER_LAYOUT = {
  orderId: [0, 10],
  customerId: [10, 20],
  customerName: [20, 52],
  orderDate: [52, 62],
  amount: [62, 74],
  currency: [74, 77],
} as const;

export const ORDER_LINE_LENGTH = 77;

const PA1_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export const DEFAULT_ORDERS_PATH = path.join(PA1_ROOT, "data", "orders-20260901.txt");
export const DEFAULT_CUSTOMERS_PATH = path.join(PA1_ROOT, "data", "customers.csv");
export const OUTPUT_PATH = path.join(PA1_ROOT, "out", "report.json");

export function decodeOrderFile(bytes: Buffer): string {
  const decoder = new TextDecoder("windows-1257");
  return decoder.decode(bytes);
}

export function toIsoDate(ddmmyyyy: string): string {
  const [day, month, year] = ddmmyyyy.split(".");
  return `${year}-${month}-${day}`;
}

export function toDecimalString(amount: string): string {
  return amount.trim().replace(",", ".");
}

export function parseCustomers(csv: string): Map<string, string> {
  const lines = csv.split(/\r\n|\r|\n/).filter((l) => l.length > 0);
  const map = new Map<string, string>();

  for (const line of lines.slice(1)) {
    const parts = line.split(";");
    const customerId = parts[0]?.trim();
    const fullName = parts[1]?.trim();
    if (customerId) {
      map.set(customerId, fullName ?? "");
    }
  }

  return map;
}

export function ingest(options: IngestOptions): Report {
  const orderBytes = readFileSync(options.ordersPath);
  const orderText = decodeOrderFile(orderBytes);

  const lines = orderText.split(/\r\n|\r|\n/);
  if (lines.length > 0 && lines[lines.length - 1] === "") {
    lines.pop();
  }

  const orders: Order[] = [];
  const rejected: RejectedRecord[] = [];

  lines.forEach((line, index) => {
    const lineNumber = index + 1;

    if (line.length !== ORDER_LINE_LENGTH) {
      rejected.push({
        line: lineNumber,
        raw: line,
        reason: `expected ${ORDER_LINE_LENGTH} characters, got ${line.length}`,
      });
      return;
    }

    const [oS, oE] = ORDER_LAYOUT.orderId;
    const [cS, cE] = ORDER_LAYOUT.customerId;
    const [nS, nE] = ORDER_LAYOUT.customerName;
    const [dS, dE] = ORDER_LAYOUT.orderDate;
    const [aS, aE] = ORDER_LAYOUT.amount;
    const [curS, curE] = ORDER_LAYOUT.currency;

    const orderId = line.slice(oS, oE).trim();
    const customerId = line.slice(cS, cE).trim();
    const customerName = line.slice(nS, nE).trim();
    const rawDate = line.slice(dS, dE).trim();
    const rawAmount = line.slice(aS, aE).trim();
    const currency = line.slice(curS, curE).trim();

    orders.push({
      orderId,
      customerId,
      customerName,
      orderDate: toIsoDate(rawDate),
      amount: toDecimalString(rawAmount),
      currency,
    });
  });

  const customersCsv = readFileSync(options.customersPath, "utf8");
  const customers = parseCustomers(customersCsv);

  const seenCustomerIds = new Set(orders.map((o) => o.customerId));
  const unmatchedCustomers = [...customers.keys()].filter(
    (id) => !seenCustomerIds.has(id),
  );

  return { orders, rejected, unmatchedCustomers };
}

export function main(): void {
  const report = ingest({
    ordersPath: DEFAULT_ORDERS_PATH,
    customersPath: DEFAULT_CUSTOMERS_PATH,
  });

  mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  writeFileSync(OUTPUT_PATH, JSON.stringify(report, null, 2) + "\n", "utf8");

  console.log(
    `wrote ${OUTPUT_PATH}\n` +
      `  ${report.orders.length} orders\n` +
      `  ${report.rejected.length} rejected\n` +
      `  ${report.unmatchedCustomers.length} customers with no order`,
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main();
}
