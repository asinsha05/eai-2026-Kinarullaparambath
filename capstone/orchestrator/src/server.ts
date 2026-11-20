import { mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import type { Channel, ConsumeMessage, Options } from "amqplib";
import { startService } from "../../shared/service";
import { getRetryCount, withoutRetryHistory } from "../../shared/rabbit";
import { emitHop, newId, type Hop } from "../../shared/trace";

const require = createRequire(import.meta.url);
const addFormats = require("ajv-formats");

const env = process.env;
const DATA_DIR = env.DATA_DIR ?? "/data";
const STORE_FILE = `${DATA_DIR}/saga-store.json`;
const SCHEMA_FILE = "/canonical/order.schema.json";
const TIMEOUT_MS = Number(env.REQUEST_TIMEOUT_MS ?? "2500");
const MAX_RETRIES = Number(env.MAX_RETRIES ?? "3");
const CONCURRENCY = Number(env.SAGA_CONCURRENCY ?? "5");
const URLS = {
  payment: env.PAYMENT_URL!,
  inventory: env.INVENTORY_URL!,
  shipping: env.SHIPPING_URL!,
  notification: env.NOTIFICATION_URL!,
};
const RUN_ID = newId(); // a new id at every start: how a crash is detected (R-4.7.3)

// ---------------------------------------------------------------- types ----
interface Order {
  orderId: string;
  orderType: string;
  source: string;
  customer: { email: string };
  items: Array<{ unitPrice: string; quantity: number }>;
}
interface StepRec { step: string; status: string; at: string; code?: string }
interface Envelope {
  correlationId: string; businessKey: string; orderId: string; source: string; orderType: string;
  recipient: string; state: string; code?: string; duplicateOf?: string; occurredAt: string;
}
interface Saga {
  businessKey: string;
  state: string;
  steps: StepRec[];
  compensations: StepRec[];
  operatorCompensation?: { at: string; state: string };
  outcome?: { routingKey: string; envelope: Envelope };
  owner: string; // RUN_ID while running here, "retry" while waiting in the retry path
  updatedAt: string;
}
interface Store { sagas: Record<string, Saga>; businessKeys: Record<string, string> }
interface CallResult { kind: "ok" | "permanent" | "temporary"; status?: number; code: string; timeout?: boolean }

// ---------------------------------------------------------------- store ----
mkdirSync(DATA_DIR, { recursive: true });
function load(): Store {
  try {
    const s = JSON.parse(readFileSync(STORE_FILE, "utf8"));
    return { sagas: s.sagas ?? {}, businessKeys: s.businessKeys ?? {} };
  } catch {
    return { sagas: {}, businessKeys: {} };
  }
}
const store = load();
function save(): void {
  const tmp = `${STORE_FILE}.tmp`;
  writeFileSync(tmp, JSON.stringify(store), "utf8");
  renameSync(tmp, STORE_FILE);
}
save();
const touch = (rec: Saga) => { rec.updatedAt = new Date().toISOString(); };

// ---------------------------------------------------------------- schema ---
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

// ---------------------------------------------------------------- helpers --
const hop = (h: Omit<Hop, "service">): string => emitHop(() => rt.confirmChannel(), { service: "orchestrator", ...h });
const lastHop = new Map<string, string>(); // in memory: only used as a causation hint
const inProgress = new Set<string>();

function ack(ch: Channel, msg: ConsumeMessage): void {
  try { ch.ack(msg); } catch { /* channel gone: the message will be redelivered */ }
}

function publishConfirmed(exchange: string, key: string, content: Buffer, options: Options.Publish): Promise<void> {
  const ch = rt.confirmChannel();
  if (!ch) return Promise.reject(new Error("no broker channel"));
  return Promise.race([
    new Promise<void>((resolve, reject) => {
      ch.publish(exchange, key, content, options, (err) => (err ? reject(err) : resolve()));
    }),
    new Promise<void>((_, reject) => setTimeout(() => reject(new Error("publish confirm timeout")), 5000)),
  ]);
}

function publishEvent(routingKey: string, envelope: Envelope, cid: string, causationId: string): Promise<void> {
  return publishConfirmed("orders.outcome", routingKey, Buffer.from(JSON.stringify(envelope)), {
    persistent: true,
    contentType: "application/json",
    messageId: newId(),
    headers: { correlationId: cid, causationId },
  });
}

function envelopeOf(cid: string, bk: string, order: Order, state: string, extra: { code?: string; duplicateOf?: string } = {}): Envelope {
  const e: Envelope = {
    correlationId: cid, businessKey: bk, orderId: order.orderId, source: order.source,
    orderType: order.orderType, recipient: order.customer.email, state, occurredAt: new Date().toISOString(),
  };
  if (extra.code) e.code = extra.code;
  if (extra.duplicateOf) e.duplicateOf = extra.duplicateOf;
  return e;
}

function toCents(price: string): number {
  const m = /^(\d+)(?:\.(\d{1,2}))?$/.exec(String(price).trim());
  if (!m) throw new Error(`bad price ${price}`);
  return parseInt(m[1], 10) * 100 + parseInt(((m[2] ?? "") + "00").slice(0, 2), 10);
}
function amountOf(order: Order): string {
  let cents = 0;
  for (const it of order.items) cents += toCents(it.unitPrice) * Number(it.quantity);
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

// ---------------------------------------------------------------- calls ----
interface CallParams { cid: string; hopId: string; orderId: string; bk: string; amount?: string; items?: unknown; recipient?: string }

async function call(step: string, p: CallParams): Promise<CallResult> {
  const specs: Record<string, { url: string; body: unknown; key?: string }> = {
    payment: { url: `${URLS.payment}/payment/authorize`, body: { orderId: p.orderId, amount: p.amount }, key: `authorize:${p.bk}` },
    inventory: { url: `${URLS.inventory}/inventory/reserve`, body: { orderId: p.orderId, items: p.items }, key: `reserve:${p.bk}` },
    shipping: { url: `${URLS.shipping}/shipping/create`, body: { orderId: p.orderId } },
    notification: { url: `${URLS.notification}/notification/send`, body: { orderId: p.orderId, recipient: p.recipient } },
    inventory_release: { url: `${URLS.inventory}/inventory/release`, body: { orderId: p.orderId }, key: `release:${p.bk}` },
    payment_refund: { url: `${URLS.payment}/payment/refund`, body: { orderId: p.orderId }, key: `refund:${p.bk}` },
  };
  const spec = specs[step];
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "X-Correlation-Id": p.cid,
    "X-Causation-Id": p.hopId,
  };
  if (spec.key) headers["Idempotency-Key"] = spec.key;
  try {
    const r = await fetch(spec.url, {
      method: "POST", headers, body: JSON.stringify(spec.body), signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (r.ok) {
      await r.text().catch(() => "");
      return { kind: "ok", status: r.status, code: "ok" };
    }
    const data = (await r.json().catch(() => ({}))) as { code?: string };
    const code = data.code ?? (r.status >= 500 ? "service_unavailable" : "downstream_error");
    return { kind: r.status >= 400 && r.status < 500 ? "permanent" : "temporary", status: r.status, code };
  } catch (e) {
    const name = (e as Error)?.name;
    if (name === "TimeoutError" || name === "AbortError") return { kind: "temporary", code: "timeout", timeout: true };
    return { kind: "temporary", code: "unreachable" };
  }
}

// ---------------------------------------------------------------- outcomes -
async function conclude(
  cid: string, rec: Saga, order: Order, state: string, code: string | undefined,
  causeHop: string, ch: Channel, msg: ConsumeMessage,
): Promise<void> {
  const routingKey = `order.${state}`;
  const envelope = envelopeOf(cid, rec.businessKey, order, state, { code });
  rec.state = state;
  rec.outcome = { routingKey, envelope };
  rec.owner = RUN_ID;
  touch(rec);
  save(); // R-4.2.18: persist the state together with its outcome, then publish, then ack
  const outHop = newId();
  await publishEvent(routingKey, envelope, cid, outHop);
  hop({ hopId: outHop, correlationId: cid, causationId: causeHop, operation: "saga.outcome", status: "success", detail: { state, code } });
  lastHop.set(cid, outHop);
  ack(ch, msg);
}

async function compensate(
  cid: string, rec: Saga, orderId: string, bk: string, causeHop: string,
): Promise<{ ok: boolean; code?: string; lastHop: string }> {
  const completed = (s: string) => rec.steps.some((x) => x.step === s && x.status === "success");
  const done = (c: string) => rec.compensations.some((x) => x.step === c && x.status === "success");
  const plan = [["inventory_release", "inventory"], ["payment_refund", "payment"]].filter(([c, s]) => completed(s) && !done(c));
  let prev = causeHop;
  for (const [comp] of plan) {
    const hopId = newId();
    const c0 = new Date();
    const r = await call(comp, { cid, hopId, orderId, bk });
    const ok = r.kind === "ok";
    hop({ hopId, correlationId: cid, causationId: prev, operation: comp, status: ok ? "success" : r.timeout ? "timeout" : "failed", startedAt: c0, detail: ok ? undefined : { code: r.code } });
    rec.compensations.push(ok ? { step: comp, status: "success", at: new Date().toISOString() }
      : { step: comp, status: "failed", at: new Date().toISOString(), code: r.code });
    touch(rec);
    save();
    if (!ok) return { ok: false, code: r.code, lastHop: hopId };
    prev = hopId;
  }
  return { ok: true, lastHop: prev };
}

async function markStuck(cid: string, rec: Saga, order: Order, bk: string, causeHop: string): Promise<void> {
  const stuckHop = hop({ correlationId: cid, causationId: causeHop, operation: "saga.stuck", status: "failed", detail: { reason: "steps were running under an orchestrator run that no longer exists" } });
  const envelope = envelopeOf(cid, bk, order, "needs_attention");
  rec.state = "needs_attention";
  rec.outcome = { routingKey: "order.needs_attention", envelope };
  rec.owner = RUN_ID;
  touch(rec);
  save();
  lastHop.set(cid, stuckHop);
  await publishEvent("order.needs_attention", envelope, cid, stuckHop);
}

// ---------------------------------------------------------------- consumer -
async function quarantine(msg: ConsumeMessage, ch: Channel, cid: string | undefined, t0: Date, reason: string): Promise<void> {
  await publishConfirmed("orders.invalid.exchange", "", msg.content, {
    persistent: true,
    contentType: msg.properties.contentType,
    messageId: newId(),
    headers: msg.properties.headers,
  });
  if (cid) {
    const pub = msg.properties.headers?.causationId;
    hop({ correlationId: cid, causationId: typeof pub === "string" ? pub : null, operation: "invalid.quarantine", status: "quarantined", startedAt: t0, detail: { reason } });
  }
  ack(ch, msg);
}

async function onMessage(msg: ConsumeMessage, ch: Channel): Promise<void> {
  const t0 = new Date();
  const h = msg.properties.headers ?? {};
  const cid = typeof h.correlationId === "string" && h.correlationId ? h.correlationId : undefined;
  const publisherHop = typeof h.causationId === "string" ? h.causationId : null;
  const attempt = getRetryCount(msg) + 1;

  // R-4.2.2: parse INSIDE the try; a message that cannot be processed is permanent.
  let order: Order;
  try {
    if (!cid) throw new Error("missing correlationId header");
    const parsed = JSON.parse(msg.content.toString("utf8"));
    if (!validateOrder(parsed)) throw new Error("not a canonical order");
    order = parsed as Order;
  } catch (e) {
    await quarantine(msg, ch, cid, t0, (e as Error).message);
    return;
  }
  const id = cid as string;
  const bk = typeof h.businessKey === "string" && h.businessKey ? h.businessKey : `${order.source}:${order.orderId}`;
  const consumeHop = hop({ correlationId: id, causationId: publisherHop, operation: "saga.consume", status: "success", attempt, messageId: msg.properties.messageId, startedAt: t0 });

  let rec = store.sagas[id];
  let wasParked = false;
  if (rec) {
    if (rec.outcome) {
      // R-4.2.4: a redelivery of a finished saga: republish the stored outcome, run nothing.
      const dh = hop({ correlationId: id, causationId: consumeHop, operation: "saga.dedupe", status: "skipped", detail: { reason: "redelivery" } });
      const oh = newId();
      await publishEvent(rec.outcome.routingKey, rec.outcome.envelope, id, oh);
      hop({ hopId: oh, correlationId: id, causationId: dh, operation: "saga.outcome", status: "success", detail: { republished: true } });
      ack(ch, msg);
      return;
    }
    const retryable = rec.state === "parked" || (rec.state === "processing" && rec.owner === "retry");
    if (!retryable) {
      if (rec.owner === RUN_ID) { ack(ch, msg); return; } // still running in this process
      await markStuck(id, rec, order, bk, consumeHop); // R-4.2.5, R-4.7.1: never re-run automatically
      ack(ch, msg);
      return;
    }
    wasParked = rec.state === "parked";
    rec.state = "processing";
    rec.owner = RUN_ID;
    touch(rec);
    save();
  } else {
    // No await between this check and the record below: no second saga for one business key (R-4.10.3).
    const first = store.businessKeys[bk];
    if (first && first !== id) {
      const dup: Saga = {
        businessKey: bk, state: "duplicate", steps: [], compensations: [], owner: RUN_ID, updatedAt: new Date().toISOString(),
        outcome: { routingKey: "order.duplicate", envelope: envelopeOf(id, bk, order, "duplicate", { duplicateOf: first }) },
      };
      store.sagas[id] = dup;
      save();
      const dh = hop({ correlationId: id, causationId: consumeHop, operation: "saga.dedupe", status: "duplicate", detail: { duplicateOf: first } });
      await publishEvent("order.duplicate", dup.outcome!.envelope, id, dh);
      ack(ch, msg);
      return;
    }
    rec = { businessKey: bk, state: "processing", steps: [], compensations: [], owner: RUN_ID, updatedAt: new Date().toISOString() };
    store.sagas[id] = rec;
    store.businessKeys[bk] = id;
    save();
  }

  // R-4.2.7: the semantic lock, before the first step (again after a replay from the DLQ).
  if (!store.sagas[id].outcome && (!wasParked || true)) {
    await publishEvent("order.processing", envelopeOf(id, bk, order, "processing"), id, consumeHop);
  }

  const names = order.orderType === "b2b" ? ["inventory", "shipping", "notification"] : ["payment", "inventory", "shipping", "notification"];
  let prev = consumeHop;
  for (const step of names) {
    const hopId = newId();
    const s0 = new Date();
    const r = await call(step, {
      cid: id, hopId, orderId: order.orderId, bk,
      amount: step === "payment" ? amountOf(order) : undefined,
      items: step === "inventory" ? order.items : undefined,
      recipient: order.customer.email,
    });
    const ok = r.kind === "ok";
    hop({ hopId, correlationId: id, causationId: prev, operation: step, status: ok ? "success" : r.timeout ? "timeout" : "failed", attempt, startedAt: s0, detail: ok ? undefined : { code: r.code } });
    if (ok) {
      rec.steps.push({ step, status: "success", at: new Date().toISOString() });
      touch(rec);
      save();
      prev = hopId;
      continue;
    }
    const isFirst = step === names[0];

    if (isFirst && r.kind === "temporary") {
      // R-4.2.12: nothing is known (the provider may have charged): retry with the same key.
      const lastError = r.status ? `${step} ${r.status}` : r.code;
      if (attempt < MAX_RETRIES) {
        rec.state = "processing";
        rec.owner = "retry";
        touch(rec);
        save();
        hop({ correlationId: id, causationId: consumeHop, operation: "saga.retry", status: "retried", attempt, detail: { code: r.code } });
        try { ch.nack(msg, false, false); } catch { /* gone */ }
        return;
      }
      rec.state = "parked";
      rec.owner = RUN_ID;
      touch(rec);
      save();
      const dl = hop({ correlationId: id, causationId: hopId, operation: "saga.dead_letter", status: "dead_lettered", attempt, detail: { lastError } });
      await publishConfirmed("orders.dlq.exchange", "", msg.content, {
        persistent: true,
        contentType: "application/json",
        messageId: newId(),
        headers: {
          ...withoutRetryHistory(msg.properties.headers),
          originQueue: "saga.queue", lastError, attempts: attempt,
          deadLetteredAt: new Date().toISOString(), correlationId: id, causationId: dl, businessKey: bk,
        },
      });
      await publishEvent("order.parked", envelopeOf(id, bk, order, "parked"), id, dl);
      ack(ch, msg);
      return;
    }

    rec.steps.push({ step, status: r.timeout ? "timeout" : "failed", at: new Date().toISOString(), code: r.code });
    touch(rec);
    save();
    if (isFirst) {
      await conclude(id, rec, order, "failed", r.code, hopId, ch, msg); // R-4.2.15: a 4xx, nothing to compensate
      return;
    }
    const comp = await compensate(id, rec, order.orderId, bk, hopId);
    if (comp.ok) await conclude(id, rec, order, "compensated", r.code, comp.lastHop, ch, msg);
    else await conclude(id, rec, order, "compensation_failed", comp.code, comp.lastHop, ch, msg);
    return;
  }
  await conclude(id, rec, order, "completed", undefined, prev, ch, msg);
}

// ---------------------------------------------------------------- service --
const rt = startService({
  name: "orchestrator",
  prefetch: CONCURRENCY,
  queues: { "saga.queue": onMessage },
  configure(app) {
    app.get("/sagas/stuck", (_req, res) => {
      const sagas = Object.entries(store.sagas)
        .filter(([, s]) => s.state === "needs_attention" || s.state === "compensation_failed")
        .map(([correlationId, s]) => ({
          correlationId,
          businessKey: s.businessKey,
          state: s.state,
          completedSteps: s.steps.filter((x) => x.status === "success").map((x) => x.step),
          compensatedSteps: s.compensations.filter((x) => x.status === "success").map((x) => x.step),
          since: s.updatedAt,
        }));
      res.status(200).json({ sagas });
    });

    app.post("/sagas/:correlationId/compensate", async (req, res) => {
      const id = req.params.correlationId;
      const rec = store.sagas[id];
      if (!rec) { res.status(404).json({ code: "not_found" }); return; }
      if (rec.state === "compensated" && rec.operatorCompensation) {
        res.status(200).json({ correlationId: id, state: "compensated" });
        return;
      }
      if (inProgress.has(id)) { res.status(409).json({ code: "compensation_in_progress" }); return; }
      if (rec.state !== "needs_attention" && rec.state !== "compensation_failed") {
        res.status(409).json({ code: "saga_not_stuck" });
        return;
      }
      inProgress.add(id);
      try {
        const prevState = rec.state;
        const prevEnv = rec.outcome!.envelope;
        const opHop = hop({ correlationId: id, causationId: lastHop.get(id) ?? null, operation: "saga.operator_compensate", status: "success" });
        const comp = await compensate(id, rec, prevEnv.orderId, rec.businessKey, opHop);
        const failing = [...rec.steps].reverse().find((x) => x.status !== "success");
        let state: string;
        let code: string | undefined;
        if (comp.ok) {
          state = "compensated";
          code = prevState === "needs_attention" ? "operator_compensated" : (failing?.code ?? "operator_compensated");
        } else {
          state = "compensation_failed";
          code = comp.code;
        }
        const envelope: Envelope = { ...prevEnv, state, code, occurredAt: new Date().toISOString() };
        delete envelope.duplicateOf;
        rec.operatorCompensation = { at: new Date().toISOString(), state };
        rec.state = state;
        rec.outcome = { routingKey: `order.${state}`, envelope };
        touch(rec);
        save();
        const outHop = newId();
        await publishEvent(`order.${state}`, envelope, id, outHop);
        hop({ hopId: outHop, correlationId: id, causationId: comp.lastHop, operation: "saga.outcome", status: "success", detail: { state, code } });
        lastHop.set(id, outHop);
        res.status(200).json({ correlationId: id, state });
      } catch (err) {
        console.error("[orchestrator] operator compensation failed:", err);
        if (!res.headersSent) res.status(500).json({ code: "internal_error" });
      } finally {
        inProgress.delete(id);
      }
    });
  },
});
