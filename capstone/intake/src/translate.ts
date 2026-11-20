import { XMLParser, XMLValidator } from "fast-xml-parser";

export interface Address { street: string; city: string; postalCode: string; country: string }
export type OrderStatus = "new" | "processing" | "shipped" | "delivered";
export interface CanonicalItem {
  productId: string; productName: string; quantity: number;
  unitPrice: string; currency: string; taxRate: number;
}
export interface CanonicalOrder {
  orderId: string;
  orderType: "standard" | "express" | "b2b";
  source: "web" | "mobile" | "b2b";
  receivedAt: string;
  orderDate: string;
  customer: { name: string; email: string; address: Address };
  items: CanonicalItem[];
  currency: string;
  status: OrderStatus;
}
export interface Line { productId: string; productName: string; quantity: number }
export interface Draft { base: Omit<CanonicalOrder, "items" | "receivedAt">; lines: Line[] }
export interface PricingOptions { baseUrl: string; apiKey: string; timeoutMs: number }

export class HttpProblem extends Error {
  constructor(public status: number, public code: string, message: string) { super(message); }
}
export class UnknownProducts extends Error {
  constructor(public productIds: string[]) { super("unknown_product"); }
}
export class PricingUnavailable extends Error {}

const bad = (message: string) => new HttpProblem(400, "validation_error", message);
function need(v: unknown, what: string): any {
  if (v === undefined || v === null || v === "") throw bad(`missing ${what}`);
  return v;
}
const str = (v: unknown, what: string): string => String(need(v, what)).trim();
const isObj = (v: unknown): v is Record<string, any> => typeof v === "object" && v !== null && !Array.isArray(v);

// ---- helpers carried over from PA4 ----
export function normalizeProductId(source: "web" | "mobile" | "b2b", rawId: string): string {
  const id = String(rawId).trim();
  if (source === "mobile") return `PROD-${id}`;
  if (source === "b2b") return id.replace(/^SKU-/, "");
  return id;
}
export const toDecimalAmount = (price: number): string => Number(price).toFixed(2);
const epochSecondsToIso = (s: number): string => new Date(s * 1000).toISOString();
function mapCurrencyCode(n: number): string {
  const codes: Record<number, string> = { 978: "EUR", 840: "USD", 826: "GBP" };
  return codes[n] ?? String(n);
}
function mapMobileStatus(code: number): OrderStatus {
  const m: Record<number, OrderStatus> = { 1: "new", 2: "processing", 3: "shipped", 4: "delivered" };
  return m[code] ?? "new";
}
function mapStatus(raw: string): OrderStatus {
  const v = String(raw).trim().toLowerCase();
  return (["new", "processing", "shipped", "delivered"] as string[]).includes(v) ? (v as OrderStatus) : "new";
}
function parseMobileAddress(addr: string): Address {
  const [street = "", city = "", postalCode = "", country = ""] = addr.split(", ").map((p) => p.trim());
  return { street, city, postalCode, country };
}
function toOrderType(raw: unknown, fallback: CanonicalOrder["orderType"]): CanonicalOrder["orderType"] {
  const v = String(raw ?? "").trim().toLowerCase();
  return v === "standard" || v === "express" || v === "b2b" ? v : fallback;
}
const iso = (v: unknown, what: string): string => {
  const d = new Date(str(v, what));
  if (Number.isNaN(d.getTime())) throw bad(`invalid ${what}`);
  return d.toISOString();
};

// ---- the three translators: parsed input -> draft (no pricing yet) ----
function draftWeb(src: any): Draft {
  const c = need(src.customer, "customer");
  const a = need(c.address, "customer.address");
  const items = need(src.items, "items");
  if (!Array.isArray(items) || items.length === 0) throw bad("items must be a non-empty array");
  // Filter: the customer is built field by field, so customer.payment is never copied.
  return {
    base: {
      orderId: str(src.orderId, "orderId"),
      orderType: toOrderType(src.orderType, "standard"),
      source: "web",
      orderDate: iso(src.orderDate, "orderDate"),
      customer: {
        name: str(c.name, "customer.name"),
        email: str(c.email, "customer.email"),
        address: {
          street: str(a.street, "address.street"),
          city: str(a.city, "address.city"),
          postalCode: str(a.postalCode, "address.postalCode"),
          country: str(a.country, "address.country"),
        },
      },
      currency: str(src.currency, "currency"),
      status: mapStatus(src.status),
    },
    lines: items.map((i: any) => ({
      productId: normalizeProductId("web", str(i?.productId, "productId")),
      productName: str(i?.productName, "productName"),
      quantity: Number(need(i?.quantity, "quantity")),
    })),
  };
}

function draftMobile(src: any): Draft {
  const items = need(src.items, "items");
  if (!Array.isArray(items) || items.length === 0) throw bad("items must be a non-empty array");
  // Filter: pm / pan / pexp are never read.
  return {
    base: {
      orderId: str(src.oid, "oid"),
      orderType: toOrderType(src.ot, "standard"),
      source: "mobile",
      orderDate: epochSecondsToIso(Number(need(src.ts, "ts"))),
      customer: {
        name: str(src.cust_name, "cust_name"),
        email: str(src.cust_email, "cust_email"),
        address: parseMobileAddress(str(src.addr, "addr")),
      },
      currency: mapCurrencyCode(Number(need(src.cur, "cur"))),
      status: mapMobileStatus(Number(src.st)),
    },
    lines: items.map((i: any) => ({
      productId: normalizeProductId("mobile", str(i?.pid, "pid")),
      productName: str(i?.pname, "pname"),
      quantity: Number(need(i?.qty, "qty")),
    })),
  };
}

const xmlParser = new XMLParser({
  ignoreAttributes: false, attributeNamePrefix: "@_",
  parseTagValue: false, parseAttributeValue: false, trimValues: true,
});

function xmlText(bytes: Buffer, contentType: string): string {
  let label: string | undefined;
  const m = /charset\s*=\s*"?([^";\s]+)"?/i.exec(contentType); // RFC 7303: the charset parameter wins
  if (m) label = m[1];
  if (!label) {
    const head = bytes.subarray(0, 200).toString("latin1");
    const d = /<\?xml[^>]*encoding\s*=\s*["']([^"']+)["']/i.exec(head);
    if (d) label = d[1];
  }
  try { return new TextDecoder(label ?? "utf-8").decode(bytes); }
  catch { throw bad(`unsupported XML encoding ${label}`); }
}

function draftB2B(bytes: Buffer, contentType: string): Draft {
  const text = xmlText(bytes, contentType);
  if (XMLValidator.validate(text) !== true) throw bad("XML is not well-formed");
  const parsed: any = xmlParser.parse(text);
  const po = need(parsed?.PurchaseOrder, "PurchaseOrder");
  const buyer = need(po.BuyerParty, "BuyerParty");
  const ship = need(buyer.ShipToAddress, "ShipToAddress");
  const li = need(po.LineItems, "LineItems");
  const raw = li.LineItem;
  const list: any[] = Array.isArray(raw) ? raw : raw ? [raw] : [];
  if (list.length === 0) throw bad("no LineItem");
  // Filter: only named fields are read; PaymentDetails and UnitListPrice are ignored.
  return {
    base: {
      orderId: str(po["@_orderId"], "orderId"),
      orderType: toOrderType(po["@_orderType"], "b2b"),
      source: "b2b",
      orderDate: iso(po["@_orderDate"], "orderDate"),
      customer: {
        name: str(buyer.Name, "Name"),
        email: str(buyer.ContactEmail, "ContactEmail"),
        address: {
          street: str(ship.Street, "Street"),
          city: str(ship.City, "City"),
          postalCode: str(ship.PostalCode, "PostalCode"),
          country: str(ship["@_country"], "country"),
        },
      },
      currency: str(li["@_currency"], "currency"),
      status: mapStatus(String(po.Status ?? "")),
    },
    lines: list.map((x: any) => ({
      productId: normalizeProductId("b2b", str(x["@_sku"], "sku")),
      productName: String(x.Description ?? "").trim(),
      quantity: Number(need(x["@_quantity"], "quantity")),
    })),
  };
}

// ---- the Normalizer's detector (R-4.1.1 to R-4.1.5) ----
export function detectAndDraft(contentType: string | undefined, body: Buffer): Draft {
  const mt = (contentType ?? "").split(";")[0].trim().toLowerCase();
  if (mt === "application/xml" || mt === "text/xml") return draftB2B(body, contentType!);
  if (mt === "application/json") {
    let j: unknown;
    try { j = JSON.parse(body.toString("utf8")); } catch { throw bad("body is not valid JSON"); }
    if (isObj(j) && "orderId" in j && isObj(j.customer)) return draftWeb(j);
    if (isObj(j) && "oid" in j) return draftMobile(j);
    throw bad("body matches neither the web nor the mobile shape");
  }
  throw new HttpProblem(415, "unsupported_media_type", "use application/json or application/xml");
}

// ---- enrichment from pricing ----
type Looked =
  | { status: "ok"; unitPrice: string; currency: string; taxRate: number }
  | { status: "unknown_product" }
  | { status: "error" };

async function lookup(id: string, o: PricingOptions): Promise<Looked> {
  try {
    const res = await fetch(`${o.baseUrl.replace(/\/+$/, "")}/pricing/${encodeURIComponent(id)}`, {
      headers: { "X-API-Key": o.apiKey },
      signal: AbortSignal.timeout(o.timeoutMs),
    });
    if (res.status === 404) return { status: "unknown_product" };
    if (!res.ok) return { status: "error" };
    const b: any = await res.json();
    const p = Number(b.unitPrice);
    if (!Number.isFinite(p) || typeof b.currency !== "string") return { status: "error" };
    return { status: "ok", unitPrice: toDecimalAmount(p), currency: b.currency, taxRate: Number(b.taxRate) };
  } catch {
    return { status: "error" };
  }
}

export async function enrich(lines: Line[], o: PricingOptions): Promise<CanonicalItem[]> {
  const ids = [...new Set(lines.map((l) => l.productId))];
  const found = new Map<string, Looked>(
    await Promise.all(ids.map(async (id) => [id, await lookup(id, o)] as [string, Looked])),
  );
  if ([...found.values()].some((r) => r.status === "error")) throw new PricingUnavailable();
  const unknown = ids.filter((id) => found.get(id)!.status === "unknown_product");
  if (unknown.length > 0) throw new UnknownProducts(unknown);
  return lines.map((l) => {
    const r = found.get(l.productId) as Extract<Looked, { status: "ok" }>;
    return {
      productId: l.productId, productName: l.productName, quantity: l.quantity,
      unitPrice: r.unitPrice, currency: r.currency, taxRate: r.taxRate,
    };
  });
}
