/**
 * PA4 — three sources to one canonical order model.
 * Translator + content enricher + content filter.
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { XMLParser } from "fast-xml-parser";

// ---------------------------------------------------------------- contract --

export interface Address {
  street: string;
  city: string;
  postalCode: string;
  country: string;
}

export interface CanonicalCustomer {
  name: string;
  email: string;
  address: Address;
}

export interface CanonicalItem {
  productId: string;
  productName: string;
  quantity: number;
  /** Decimal STRING, e.g. "24.99" — see toDecimalAmount() below. Never a number. */
  unitPrice: string;
  currency: string;
  taxRate: number;
}

export type OrderStatus = "new" | "processing" | "shipped" | "delivered";

export interface CanonicalOrder {
  orderId: string;
  orderType: "standard" | "express" | "b2b";
  source: "web" | "mobile" | "b2b";
  receivedAt: string;
  orderDate: string;
  customer: CanonicalCustomer;
  items: CanonicalItem[];
  currency: string;
  status: OrderStatus;
}

export type TransformWarningCode = "UNKNOWN_PRODUCT" | "PRICING_API_ERROR";

export interface TransformWarning {
  code: TransformWarningCode;
  productId: string;
  message: string;
}

export interface TransformResult {
  order: CanonicalOrder | null;
  warnings: TransformWarning[];
}

export interface TranslateOptions {
  pricingBaseUrl: string;
  apiKey?: string;
  now?: () => Date;
}

export const DEFAULT_PRICING_API_KEY = "pa4-pricing-key-2026";

// ------------------------------------------------------------------- paths --

const PA4_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export const DEFAULT_WEB_ORDER_PATH = path.join(PA4_ROOT, "data", "web-order.json");
export const DEFAULT_MOBILE_ORDER_PATH = path.join(PA4_ROOT, "data", "mobile-order.json");
export const DEFAULT_B2B_ORDER_PATH = path.join(PA4_ROOT, "data", "b2b-order.xml");
export const DEFAULT_PRICING_BASE_URL = "http://localhost:4100";

const OUT_DIR = path.join(PA4_ROOT, "out");

// ------------------------------------------------------------- raw shapes --

export interface WebOrderInput {
  orderId: string;
  orderType: string;
  customer: {
    name: string;
    email: string;
    address: Address;
    payment: {
      method: string;
      cardHolder: string;
      cardNumber: string;
      expiryMonth: number;
      expiryYear: number;
    };
  };
  items: Array<{ productId: string; productName: string; quantity: number }>;
  orderDate: string;
  status: string;
  currency: string;
}

export interface MobileOrderInput {
  oid: string;
  ot: string;
  cust_name: string;
  cust_email: string;
  addr: string;
  items: Array<{ pid: string; pname: string; qty: number }>;
  ts: number;
  st: number;
  cur: number;
  pm: string;
  pan: string;
  pexp: string;
}

export interface ParsedB2BOrder {
  PurchaseOrder: {
    "@_orderId": string;
    "@_orderType": string;
    "@_orderDate": string;
    BuyerParty: {
      Name: string;
      ContactEmail: string;
      ShipToAddress: {
        "@_country": string;
        Street: string;
        City: string;
        PostalCode: string;
      };
      [key: string]: unknown;
    };
    LineItems: {
      "@_currency": string;
      LineItem: unknown;
    };
    Status: string;
    [key: string]: unknown;
  };
}

const xmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });

// ------------------------------------------------------------------ helpers --

export function normalizeProductId(source: "web" | "mobile" | "b2b", rawId: string): string {
  const id = String(rawId).trim();
  if (source === "mobile") return `PROD-${id}`;
  if (source === "b2b") return id.replace(/^SKU-/, "");
  return id;
}

export function toDecimalAmount(price: number): string {
  return Number(price).toFixed(2);
}

export type EnrichResult =
  | { status: "ok"; unitPrice: string; currency: string; taxRate: number; productName: string }
  | { status: "unknown_product" }
  | { status: "error"; httpStatus: number };

export async function enrichFromPricing(
  productId: string,
  options: TranslateOptions,
): Promise<EnrichResult> {
  const base = options.pricingBaseUrl.replace(/\/+$/, "");
  const url = `${base}/pricing/${encodeURIComponent(productId)}`;
  try {
    const res = await fetch(url, {
      headers: { "X-API-Key": options.apiKey ?? DEFAULT_PRICING_API_KEY },
    });
    if (res.status === 404) return { status: "unknown_product" };
    if (!res.ok) return { status: "error", httpStatus: res.status };
    const body = (await res.json()) as {
      unitPrice: number | string;
      currency: string;
      taxRate: number;
      productName: string;
    };
    const price = Number(body.unitPrice);
    if (!Number.isFinite(price)) return { status: "error", httpStatus: res.status };
    return {
      status: "ok",
      unitPrice: toDecimalAmount(price),
      currency: body.currency,
      taxRate: Number(body.taxRate),
      productName: body.productName,
    };
  } catch {
    // network failure or unreadable response: report it, never throw
    return { status: "error", httpStatus: 0 };
  }
}

export async function enrichItems(
  lines: Array<{ productId: string; productName: string; quantity: number }>,
  options: TranslateOptions,
): Promise<{ items: CanonicalItem[]; warnings: TransformWarning[] }> {
  const items: CanonicalItem[] = [];
  const warnings: TransformWarning[] = [];

  for (const line of lines) {
    const result = await enrichFromPricing(line.productId, options);
    if (result.status === "ok") {
      items.push({
        productId: line.productId,
        productName: line.productName,
        quantity: line.quantity,
        unitPrice: result.unitPrice,
        currency: result.currency,
        taxRate: result.taxRate,
      });
    } else if (result.status === "unknown_product") {
      warnings.push({
        code: "UNKNOWN_PRODUCT",
        productId: line.productId,
        message: `Pricing API does not know product ${line.productId}; line item dropped.`,
      });
    } else {
      warnings.push({
        code: "PRICING_API_ERROR",
        productId: line.productId,
        message: `Pricing API failed for ${line.productId} (HTTP ${result.httpStatus}); line item dropped.`,
      });
    }
  }

  return { items, warnings };
}

export function epochSecondsToIso(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toISOString();
}

export function mapCurrencyCode(numericCode: number): string {
  const codes: Record<number, string> = { 978: "EUR", 840: "USD", 826: "GBP" };
  return codes[numericCode] ?? String(numericCode);
}

export function mapMobileStatus(code: number): OrderStatus {
  const statuses: Record<number, OrderStatus> = {
    1: "new",
    2: "processing",
    3: "shipped",
    4: "delivered",
  };
  return statuses[code] ?? "new";
}

export function mapB2BStatus(raw: string): OrderStatus {
  const value = String(raw).trim().toLowerCase();
  const allowed: OrderStatus[] = ["new", "processing", "shipped", "delivered"];
  return allowed.includes(value as OrderStatus) ? (value as OrderStatus) : "new";
}

export function parseMobileAddress(addr: string): Address {
  const [street = "", city = "", postalCode = "", country = ""] = addr
    .split(", ")
    .map((part) => part.trim());
  return { street, city, postalCode, country };
}

export function decodeB2BXmlBytes(bytes: Buffer): string {
  return new TextDecoder("windows-1257").decode(bytes);
}

// small private helpers
function toOrderType(raw: unknown, fallback: CanonicalOrder["orderType"]): CanonicalOrder["orderType"] {
  const v = String(raw ?? "").trim().toLowerCase();
  return v === "standard" || v === "express" || v === "b2b" ? v : fallback;
}

function toWebStatus(raw: unknown): OrderStatus {
  return mapB2BStatus(String(raw ?? ""));
}

function buildResult(
  base: Omit<CanonicalOrder, "items" | "receivedAt">,
  enriched: { items: CanonicalItem[]; warnings: TransformWarning[] },
  options: TranslateOptions,
): TransformResult {
  if (enriched.items.length === 0) {
    return { order: null, warnings: enriched.warnings };
  }
  const order: CanonicalOrder = {
    orderId: base.orderId,
    orderType: base.orderType,
    source: base.source,
    receivedAt: (options.now?.() ?? new Date()).toISOString(),
    orderDate: base.orderDate,
    customer: base.customer,
    items: enriched.items,
    currency: base.currency,
    status: base.status,
  };
  return { order, warnings: enriched.warnings };
}

// -------------------------------------------------------------- translators --

export async function translateWeb(
  webOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const src = JSON.parse(readFileSync(webOrderPath, "utf8")) as WebOrderInput;

  // Filter: customer is built field by field, so customer.payment is never copied.
  const customer: CanonicalCustomer = {
    name: src.customer.name,
    email: src.customer.email,
    address: {
      street: src.customer.address.street,
      city: src.customer.address.city,
      postalCode: src.customer.address.postalCode,
      country: src.customer.address.country,
    },
  };

  const lines = src.items.map((i) => ({
    productId: normalizeProductId("web", i.productId),
    productName: i.productName,
    quantity: Number(i.quantity),
  }));
  const enriched = await enrichItems(lines, options);

  return buildResult(
    {
      orderId: String(src.orderId),
      orderType: toOrderType(src.orderType, "standard"),
      source: "web",
      orderDate: new Date(src.orderDate).toISOString(),
      customer,
      currency: src.currency,
      status: toWebStatus(src.status),
    },
    enriched,
    options,
  );
}

export async function translateMobile(
  mobileOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const src = JSON.parse(readFileSync(mobileOrderPath, "utf8")) as MobileOrderInput;

  // Filter: pm / pan / pexp are simply never read.
  const customer: CanonicalCustomer = {
    name: src.cust_name,
    email: src.cust_email,
    address: parseMobileAddress(src.addr),
  };

  const lines = src.items.map((i) => ({
    productId: normalizeProductId("mobile", i.pid),
    productName: i.pname,
    quantity: Number(i.qty),
  }));
  const enriched = await enrichItems(lines, options);

  return buildResult(
    {
      orderId: String(src.oid),
      orderType: toOrderType(src.ot, "standard"),
      source: "mobile",
      orderDate: epochSecondsToIso(src.ts),
      customer,
      currency: mapCurrencyCode(src.cur),
      status: mapMobileStatus(src.st),
    },
    enriched,
    options,
  );
}

export async function translateB2B(
  b2bOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const bytes = readFileSync(b2bOrderPath);
  const parsed = xmlParser.parse(decodeB2BXmlBytes(bytes)) as ParsedB2BOrder;
  const po = parsed.PurchaseOrder;
  const buyer = po.BuyerParty;
  const ship = buyer.ShipToAddress;

  // Filter: only named fields are read; PaymentDetails and unknown elements are ignored.
  const customer: CanonicalCustomer = {
    name: String(buyer.Name).trim(),
    email: String(buyer.ContactEmail).trim(),
    address: {
      street: String(ship.Street).trim(),
      city: String(ship.City).trim(),
      postalCode: String(ship.PostalCode).trim(),
      country: String(ship["@_country"]).trim(),
    },
  };

    const rawItems = po.LineItems?.LineItem;
  const itemList: Array<Record<string, unknown>> = Array.isArray(rawItems)
    ? rawItems
    : rawItems
      ? [rawItems as Record<string, unknown>]
      : [];

  // UnitListPrice is deliberately never read.
  const lines = itemList
    .filter((li) => li["@_sku"] !== undefined)
    .map((li) => ({
      productId: normalizeProductId("b2b", String(li["@_sku"])),
      productName: String(li["Description"] ?? "").trim(),
      quantity: Number(li["@_quantity"]),
    }));
  const enriched = await enrichItems(lines, options);

  return buildResult(
    {
      orderId: String(po["@_orderId"]),
      orderType: toOrderType(po["@_orderType"], "b2b"),
      source: "b2b",
      orderDate: new Date(po["@_orderDate"]).toISOString(),
      customer,
      currency: String(po.LineItems["@_currency"]),
      status: mapB2BStatus(po.Status),
    },
    enriched,
    options,
  );
}

// -------------------------------------------------------------------- main --

export async function main(): Promise<void> {
  const options: TranslateOptions = { pricingBaseUrl: DEFAULT_PRICING_BASE_URL };

  const results = {
    web: await translateWeb(DEFAULT_WEB_ORDER_PATH, options),
    mobile: await translateMobile(DEFAULT_MOBILE_ORDER_PATH, options),
    b2b: await translateB2B(DEFAULT_B2B_ORDER_PATH, options),
  };

  mkdirSync(OUT_DIR, { recursive: true });
  for (const [name, result] of Object.entries(results)) {
    const outPath = path.join(OUT_DIR, `${name}.json`);
    writeFileSync(outPath, JSON.stringify(result, null, 2) + "\n", "utf8");
    console.log(`wrote ${outPath}`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}