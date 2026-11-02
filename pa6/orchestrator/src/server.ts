/**
 * PA6 orchestrator scaffold — ported from the practice-03-orchestration JS lab.
 *
 * This scaffold is intentionally incomplete. `GET /health`, request validation
 * against the canonical order schema, the Idempotency-Key contract shell, and
 * restart-safe file persistence are already wired up. What is NOT implemented
 * is the actual saga:
 *
 *   1) payment authorize
 *   2) inventory reserve
 *   3) shipping create
 *   4) notification send
 *
 * with strict sequencing, trace recording, timeout handling, compensation
 * (in reverse order of completed steps), and the full idempotent-replay
 * policy. See the TODO block inside the POST /checkout handler.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type Request, type Response } from "express";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import type { CanonicalOrder } from "./canonical-order";

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");

// ---------------------------------------------------------------- contract --
// These shapes mirror grading/schema/*.json. Keep them in sync if you change
// either side — the tests validate persisted files against those schemas.

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | "completed" | "failed" | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(normalized).digest("hex");
  return `sha256:${hash}`;
}

// The request body is a canonical order. docker-compose.yml mounts the
// course's canonical/ folder read-only at /canonical, so the orchestrator
// validates against the very file PA4's output was validated against.
const CANONICAL_SCHEMA_PATH = "/canonical/order.schema.json";

if (!fs.existsSync(CANONICAL_SCHEMA_PATH)) {
  throw new Error(
    `${CANONICAL_SCHEMA_PATH} not found. docker-compose.yml mounts ../canonical ` +
      "from your repository root: copy canonical/ from eai-2026 next to pa6/ " +
      '(root README, "Starting an assignment"), then run docker compose up again.',
  );
}

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validateCanonicalOrder = ajv.compile(JSON.parse(fs.readFileSync(CANONICAL_SCHEMA_PATH, "utf8")));

function validateCheckoutPayload(payload: unknown): string | null {
  if (validateCanonicalOrder(payload)) {
    return null;
  }
  return `Request body is not a canonical order: ${ajv.errorsText(validateCanonicalOrder.errors, { dataVar: "body" })}`;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = sagaStore?.sagas?.[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

const MAX_ITEMS = 50; // must match maxItems for the items array in openapi.yaml

interface OrderItem {
  unitPrice: string;
  quantity: number;
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    return `{${Object.keys(obj)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${stableStringify(obj[k])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function stableHash(payload: unknown): string {
  return `sha256:${crypto.createHash("sha256").update(stableStringify(payload)).digest("hex")}`;
}

function toCents(price: string): number {
  const m = /^(\d+)(?:\.(\d{1,2}))?$/.exec(String(price).trim());
  if (!m) {
    throw new Error(`Bad price: ${price}`);
  }
  return parseInt(m[1] as string, 10) * 100 + parseInt(((m[2] ?? "") + "00").slice(0, 2), 10);
}

function formatCents(cents: number): string {
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

function computeAmount(items: OrderItem[]): string {
  let total = 0;
  for (const item of items) {
    total += toCents(item.unitPrice) * Number(item.quantity);
  }
  return formatCents(total);
}

interface CallResult {
  status: "success" | "failed" | "timeout";
  code?: string;
  message?: string;
}

async function callDownstream(step: string, url: string, body: unknown, trace: TraceItem[]): Promise<CallResult> {
  const started = new Date();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.requestTimeoutMs);
  const result: CallResult = { status: "failed", code: "downstream_error" };
  try {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (r.ok) {
      await r.text().catch(() => "");
      result.status = "success";
      delete result.code;
    } else {
      const data = (await r.json().catch(() => ({}))) as { code?: string; message?: string };
      result.code = data.code ?? "downstream_error";
      result.message = data.message;
    }
  } catch (e: unknown) {
    if (e instanceof Error && e.name === "AbortError") {
      result.status = "timeout";
      result.code = "timeout";
    } else {
      result.code = "downstream_error";
    }
  } finally {
    clearTimeout(timer);
  }
  const finished = new Date();
  trace.push({
    step,
    status: result.status,
    startedAt: started.toISOString(),
    finishedAt: finished.toISOString(),
    durationMs: finished.getTime() - started.getTime(),
  });
  return result;
}

function finalize(
  idempotencyKey: string,
  orderId: string,
  requestHash: string,
  state: SagaState,
  httpStatus: number,
  response: Record<string, unknown>,
  trace: TraceItem[],
): void {
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  if (!sagaStore.sagas) {
    sagaStore.sagas = {};
  }
  sagaStore.sagas[orderId] = { idempotencyKey, state, steps: trace, updatedAt: nowIso() };
  writeJsonFile(SAGA_STORE_PATH, sagaStore);

  const idemStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idemStore.records) {
    idemStore.records = {};
  }
  idemStore.records[idempotencyKey] = { requestHash, state, httpStatus, response, updatedAt: nowIso() };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idemStore);
}

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key");
  if (!idempotencyKey) {
    res.status(400).json({ code: "validation_error", message: "Idempotency-Key header is required" });
    return;
  }

  const validationError = validateCheckoutPayload(req.body);
  if (validationError) {
    res.status(400).json({ code: "validation_error", message: validationError });
    return;
  }

  const order = req.body as CanonicalOrder;
  const orderId = order.orderId;
  const items = (order as unknown as { items: OrderItem[] }).items;
  const email = (order as unknown as { customer: { email: string } }).customer.email;

  if (items.length > MAX_ITEMS) {
    res.status(400).json({
      code: "validation_error",
      message: `Request body has ${items.length} items; at most ${MAX_ITEMS} are allowed`,
    });
    return;
  }

  const requestHash = stableHash(req.body);
  const idempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idempotencyStore.records) {
    idempotencyStore.records = {};
  }

  const existing = idempotencyStore.records[idempotencyKey];
  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }
    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this Idempotency-Key is still being processed",
      });
      return;
    }
    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  // Mark in-flight (synchronously, so a second request cannot slip past).
  idempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: "in_progress",
    httpStatus: 202,
    response: { orderId, status: "in_progress" },
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);

  const trace: TraceItem[] = [];

  try {
    const amount = computeAmount(items);
    const sagaSteps = [
      { step: "payment", url: `${config.paymentUrl}/payment/authorize`, body: { orderId, amount } },
      { step: "inventory", url: `${config.inventoryUrl}/inventory/reserve`, body: { orderId, items } },
      { step: "shipping", url: `${config.shippingUrl}/shipping/create`, body: { orderId } },
      { step: "notification", url: `${config.notificationUrl}/notification/send`, body: { orderId, recipient: email } },
    ];

    let completedCount = 0;
    let failure: CallResult | null = null;

    for (const s of sagaSteps) {
      const r = await callDownstream(s.step, s.url, s.body, trace);
      if (r.status === "success") {
        completedCount += 1;
      } else {
        failure = r;
        break;
      }
    }

    if (!failure) {
      const response = { orderId, status: "completed", trace };
      finalize(idempotencyKey, orderId, requestHash, "completed", 200, response, trace);
      res.status(200).json(response);
      return;
    }

    // Compensate in reverse order. Only inventory and payment have compensations.
    let compensationFailed = false;
    if (completedCount >= 2) {
      const r = await callDownstream(
        "inventory_release",
        `${config.inventoryUrl}/inventory/release`,
        { orderId },
        trace,
      );
      if (r.status !== "success") {
        compensationFailed = true;
      }
    }
    if (completedCount >= 1) {
      const r = await callDownstream("payment_refund", `${config.paymentUrl}/payment/refund`, { orderId }, trace);
      if (r.status !== "success") {
        compensationFailed = true;
      }
    }

    const timedOut = failure.status === "timeout";
    let state: SagaState;
    let httpStatus: number;
    let code: string;
    if (compensationFailed) {
      state = "failed";
      httpStatus = 422;
      code = "compensation_failed";
    } else {
      state = completedCount === 0 ? "failed" : "compensated";
      httpStatus = timedOut ? 504 : 422;
      code = failure.code ?? "downstream_error";
    }

    const response = {
      orderId,
      status: state,
      code,
      message: failure.message ?? `Saga stopped: ${code}`,
      trace,
    };
    finalize(idempotencyKey, orderId, requestHash, state, httpStatus, response, trace);
    res.status(httpStatus).json(response);
  } catch (err: unknown) {
    const response = {
      orderId,
      status: "failed",
      code: "internal_error",
      message: err instanceof Error ? err.message : "Unexpected error",
      trace,
    };
    finalize(idempotencyKey, orderId, requestHash, "failed", 500, response, trace);
    res.status(500).json(response);
  }
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});
