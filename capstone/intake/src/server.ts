import crypto from "node:crypto";
import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import express, { type Response } from "express";
import type { Channel, ConsumeMessage } from "amqplib";
import { startService } from "../../shared/service";
import { emitHop, newId, type Hop } from "../../shared/trace";
import { registerDlq } from "./dlq";
import {
  HttpProblem, PricingUnavailable, UnknownProducts, detectAndDraft, enrich,
  type CanonicalOrder,
} from "./translate";

const require = createRequire(import.meta.url);
const addFormats = require("ajv-formats");

const DATA_DIR = process.env.DATA_DIR ?? "/data";
const IDEM_FILE = `${DATA_DIR}/idempotency-store.json`;
const ORDERS_FILE = `${DATA_DIR}/orders.json`;
const SCHEMA_FILE = process.env.CANONICAL_SCHEMA ?? "/canonical/order.schema.json";
const ITEMS_MAX = Number(process.env.ORDER_ITEMS_MAX ?? "50");
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? "2500");
const pricing = {
  baseUrl: process.env.PRICING_URL ?? "http://pricing:4100",
  apiKey: process.env.PRICING_API_KEY ?? "pa4-pricing-key-2026",
  timeoutMs: TIMEOUT_MS,
};
const KEY_RE = /^[A-Za-z0-9._:-]{1,128}$/;

mkdirSync(DATA_DIR, { recursive: true });

// ---- stores: JSON files, written atomically (R-3.5.3) ----
function loadJson<T>(p: string, fallback: T): T {
  try { return JSON.parse(readFileSync(p, "utf8")) as T; } catch { return fallback; }
}
function saveJson(p: string, v: unknown): void {
  const tmp = `${p}.tmp`;
  writeFileSync(tmp, JSON.stringify(v), "utf8");
  renameSync(tmp, p);
}

interface IdemRecord {
  bodySha256: string;
  state: "in_flight" | "done";
  response?: { status: number; body: unknown };
  correlationId?: string;
  createdAt: string;
}
const idem: Record<string, IdemRecord> = loadJson(IDEM_FILE, {});
const orders: Record<string, Record<string, unknown>> = loadJson(ORDERS_FILE, {});
// A crash can leave a request in flight. Free its key: a retry gets a new
// correlationId, and the saga's businessKey dedupe catches a double publish.
for (const k of Object.keys(idem)) if (idem[k].state === "in_flight") delete idem[k];
saveJson(IDEM_FILE, idem);
saveJson(ORDERS_FILE, orders);

// ---- the order lifecycle (README 4.4): intake follows the outcome events ----
const TRANSITIONS: Record<string, string[]> = {
  accepted: ["processing", "parked", "completed", "failed", "compensated", "compensation_failed", "needs_attention", "duplicate"],
  processing: ["parked", "completed", "failed", "compensated", "compensation_failed", "needs_attention"],
  parked: ["processing", "completed", "failed", "compensated", "compensation_failed", "needs_attention"],
  compensation_failed: ["compensated", "compensation_failed"],
  needs_attention: ["compensated", "compensation_failed"],
};

function onOutcome(msg: ConsumeMessage, ch: Channel): void {
  try {
    const env = JSON.parse(msg.content.toString("utf8"));
    const view = orders[String(env?.correlationId)];
    if (view && typeof env.state === "string") {
      const allowed = TRANSITIONS[String(view.state)] ?? [];
      // R-4.4.10: events may arrive late or twice; a move that is not allowed is ignored.
      if (allowed.includes(env.state)) {
        view.state = env.state;
        if (env.code) view.code = env.code; else delete view.code;
        if (env.duplicateOf) view.duplicateOf = env.duplicateOf; else delete view.duplicateOf;
        view.updatedAt = new Date().toISOString();
        saveJson(ORDERS_FILE, orders);
      }
    }
  } catch (err) {
    console.warn("[intake] ignoring an unreadable outcome event:", (err as Error).message);
  }
  try { ch.ack(msg); } catch { /* redelivered */ }
}

// ---- canonical schema ----
const schema = JSON.parse(readFileSync(SCHEMA_FILE, "utf8"));
const dialect = String(schema.$schema ?? "");
const AjvClass = dialect.includes("2020-12")
  ? require("ajv/dist/2020")
  : dialect.includes("2019-09")
    ? require("ajv/dist/2019")
    : require("ajv");
const ajv = new AjvClass({ allErrors: true, strict: false });
addFormats(ajv);
const validateOrder = ajv.compile(schema);

const sha256 = (b: Buffer) => crypto.createHash("sha256").update(b).digest("hex");
const problem = (res: Response, status: number, code: string, message: string, retry = false) => {
  if (retry) res.set("Retry-After", "1");
  return res.status(status).json({ code, message });
};
const hop = (h: Omit<Hop, "service">): string => emitHop(() => rt.confirmChannel(), { service: "intake", ...h });

const rt = startService({
  name: "intake",
  queues: { "intake.outcome": onOutcome },
  async setup(ch) {
    await ch.assertQueue("intake.outcome", { durable: true });
    await ch.bindQueue("intake.outcome", "orders.outcome", "#");
  },
  configure(app) {
    // Keep the raw bytes: the idempotency check hashes them, and b2b XML is not UTF-8.
    app.use(express.raw({ type: () => true, limit: "1mb" }));

    app.post("/orders", async (req, res) => {
      const t0 = new Date();
      const key = req.header("idempotency-key");
      let held = false;
      const release = () => { if (held) { delete idem[key!]; saveJson(IDEM_FILE, idem); held = false; } };
      try {
        if (!key || !KEY_RE.test(key)) {
          return problem(res, 400, "validation_error", "Idempotency-Key is missing or malformed");
        }
        const body = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
        const sha = sha256(body);

        const existing = idem[key];
        if (existing) {
          if (existing.bodySha256 !== sha) {
            return problem(res, 409, "idempotency_payload_mismatch", "this key was used with a different body");
          }
          if (existing.state === "in_flight") {
            return problem(res, 409, "idempotency_conflict", "the first request is still being processed");
          }
          return res.status(existing.response!.status).json(existing.response!.body);
        }

        // Detect + translate. No await until the record below exists, so two
        // requests with one key cannot both get past the lookup above.
        let draft;
        try {
          draft = detectAndDraft(req.header("content-type"), body);
        } catch (e) {
          if (e instanceof HttpProblem) return problem(res, e.status, e.code, e.message);
          return problem(res, 400, "validation_error", "the body could not be translated");
        }
        if (draft.lines.length > ITEMS_MAX) {
          return problem(res, 400, "validation_error", `more than ${ITEMS_MAX} items`);
        }
        const t1 = new Date();

        const correlationId = crypto.randomUUID();
        idem[key] = { bodySha256: sha, state: "in_flight", correlationId, createdAt: new Date().toISOString() };
        held = true;
        saveJson(IDEM_FILE, idem);

        const receiveHop = newId(), translateHop = newId(), enrichHop = newId(), publishHop = newId();
        hop({ hopId: receiveHop, correlationId, causationId: null, operation: "intake.receive", status: "success", startedAt: t0, finishedAt: t0 });
        hop({ hopId: translateHop, correlationId, causationId: receiveHop, operation: "intake.translate", status: "success", startedAt: t0, finishedAt: t1 });

        let items;
        const e0 = new Date();
        try {
          items = await enrich(draft.lines, { ...pricing, headers: { "X-Correlation-Id": correlationId, "X-Causation-Id": enrichHop } });
          hop({ hopId: enrichHop, correlationId, causationId: translateHop, operation: "intake.enrich", status: "success", startedAt: e0 });
        } catch (e) {
          hop({ hopId: enrichHop, correlationId, causationId: translateHop, operation: "intake.enrich", status: "failed", startedAt: e0, detail: { code: e instanceof UnknownProducts ? "unknown_product" : "pricing_unavailable" } });
          if (e instanceof UnknownProducts) {
            const out = { code: "unknown_product", productIds: e.productIds };
            idem[key] = { bodySha256: sha, state: "done", response: { status: 422, body: out }, createdAt: idem[key].createdAt };
            held = false;
            saveJson(IDEM_FILE, idem);
            return res.status(422).json(out);
          }
          release();
          if (e instanceof PricingUnavailable) return problem(res, 503, "pricing_unavailable", "pricing is unavailable", true);
          throw e;
        }

        const order: CanonicalOrder = { ...draft.base, receivedAt: new Date().toISOString(), items };
        if (!validateOrder(order)) {
          release();
          return problem(res, 400, "validation_error", ajv.errorsText(validateOrder.errors));
        }

        const businessKey = `${order.source}:${order.orderId}`;
        // Register the order BEFORE publishing: the saga's events may arrive before the confirm does.
        orders[correlationId] = {
          correlationId, businessKey, orderId: order.orderId, source: order.source,
          orderType: order.orderType, state: "accepted", updatedAt: new Date().toISOString(),
        };
        saveJson(ORDERS_FILE, orders);

        const eventId = newId();
        const p0 = new Date();
        const confirm = rt.confirmChannel();
        try {
          if (!confirm) throw new Error("no broker channel");
          await Promise.race([
            new Promise<void>((resolve, reject) => {
              confirm.publish("orders.exchange", "", Buffer.from(JSON.stringify(order)), {
                persistent: true,
                contentType: "application/json",
                messageId: eventId,
                headers: { correlationId, causationId: publishHop, businessKey },
              }, (err) => (err ? reject(err) : resolve()));
            }),
            new Promise<void>((_, reject) => setTimeout(() => reject(new Error("publish confirm timeout")), 5000)),
          ]);
        } catch {
          delete orders[correlationId];
          saveJson(ORDERS_FILE, orders);
          hop({ hopId: publishHop, correlationId, causationId: enrichHop, operation: "intake.publish", status: "failed", startedAt: p0 });
          release();
          return problem(res, 503, "broker_unavailable", "the broker did not confirm the order", true);
        }
        hop({ hopId: publishHop, correlationId, causationId: enrichHop, operation: "intake.publish", status: "success", messageId: eventId, startedAt: p0 });

        const out = { correlationId, status: "accepted" };
        idem[key] = { bodySha256: sha, state: "done", response: { status: 202, body: out }, correlationId, createdAt: idem[key].createdAt };
        held = false;
        saveJson(IDEM_FILE, idem);
        return res.status(202).json(out);
      } catch (err) {
        console.error("[intake] POST /orders failed:", err);
        release();
        if (!res.headersSent) return problem(res, 500, "internal_error", "unexpected error");
      }
    });

    registerDlq(app, () => rt, hop);

    app.get("/orders/:correlationId", (req, res) => {
      const view = orders[req.params.correlationId];
      if (!view) return res.status(404).json({ code: "not_found" });
      return res.status(200).json(view);
    });
  },
});
