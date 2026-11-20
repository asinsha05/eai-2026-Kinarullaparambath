import { appendFileSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import type { Channel, ConsumeMessage, Options } from "amqplib";
import { startService } from "../../shared/service";
import { getRetryCount, withoutRetryHistory } from "../../shared/rabbit";
import { emitHop, newId, type Hop } from "../../shared/trace";

const env = process.env;
const DATA_DIR = env.DATA_DIR ?? "/data";
const PROCESSED_FILE = `${DATA_DIR}/processed-ids.json`;
const LOG_FILE = `${DATA_DIR}/notification.log`;
const NOTIFICATION_URL = env.NOTIFICATION_URL!;
const TIMEOUT_MS = Number(env.REQUEST_TIMEOUT_MS ?? "2500");
const MAX_RETRIES = Number(env.MAX_RETRIES ?? "3");

mkdirSync(DATA_DIR, { recursive: true });
function load(): Set<string> {
  try {
    const a = JSON.parse(readFileSync(PROCESSED_FILE, "utf8"));
    if (Array.isArray(a)) return new Set(a.map(String));
  } catch { /* first start */ }
  return new Set();
}
const processed = load();
function persist(): void {
  const tmp = `${PROCESSED_FILE}.tmp`;
  writeFileSync(tmp, JSON.stringify([...processed]), "utf8");
  renameSync(tmp, PROCESSED_FILE);
}
persist();

const hop = (h: Omit<Hop, "service">): string => emitHop(() => rt.confirmChannel(), { service: "notifier", ...h });

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

const ack = (ch: Channel, m: ConsumeMessage) => { try { ch.ack(m); } catch { /* redelivered */ } };

async function onMessage(msg: ConsumeMessage, ch: Channel): Promise<void> {
  const t0 = new Date();
  const h = (msg.properties.headers ?? {}) as Record<string, any>;
  const cid = typeof h.correlationId === "string" && h.correlationId ? h.correlationId : undefined;
  const publisherHop = typeof h.causationId === "string" ? h.causationId : null;
  const attempt = getRetryCount(msg) + 1;

  let env2: any;
  try {
    if (!cid) throw new Error("missing correlationId header");
    env2 = JSON.parse(msg.content.toString("utf8"));
    if (!env2 || typeof env2 !== "object" || typeof env2.orderId !== "string" || typeof env2.recipient !== "string") {
      throw new Error("not an outcome envelope");
    }
  } catch (e) {
    await publishConfirmed("orders.invalid.exchange", "", msg.content, {
      persistent: true, contentType: msg.properties.contentType, messageId: newId(), headers: msg.properties.headers,
    });
    if (cid) hop({ correlationId: cid, causationId: publisherHop, operation: "invalid.quarantine", status: "quarantined", startedAt: t0, detail: { reason: (e as Error).message } });
    ack(ch, msg);
    return;
  }
  const id = cid as string;
  console.log(`[notifier] got ${env2.state} event for ${id} (attempt ${attempt})`);
  const consumeHop = hop({ correlationId: id, causationId: publisherHop, operation: "notify.consume", status: "success", attempt, messageId: msg.properties.messageId, startedAt: t0 });

  if (processed.has(id)) {
    hop({ correlationId: id, causationId: consumeHop, operation: "notify.duplicate", status: "duplicate" });
    ack(ch, msg);
    return;
  }

  const sendHop = newId();
  const s0 = new Date();
  let failure: string | undefined;
  const reason = String(env2.code ?? env2.state ?? "unknown");
  try {
    const r = await fetch(`${NOTIFICATION_URL}/notification/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Correlation-Id": id, "X-Causation-Id": sendHop },
      body: JSON.stringify({ orderId: env2.orderId, recipient: env2.recipient, kind: "order_not_completed", reason }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!r.ok) failure = `notification ${r.status}`;
    else await r.text().catch(() => "");
  } catch (e) {
    const n = (e as Error)?.name;
    failure = n === "TimeoutError" || n === "AbortError" ? "timeout" : "unreachable";
  }

  if (!failure) {
    hop({ hopId: sendHop, correlationId: id, causationId: consumeHop, operation: "notify.send", status: "success", attempt, startedAt: s0 });
    appendFileSync(LOG_FILE, JSON.stringify({ correlationId: id, orderId: env2.orderId, recipient: env2.recipient, kind: "order_not_completed", reason, timestamp: new Date().toISOString() }) + "\n", "utf8");
    processed.add(id);
    persist();
    ack(ch, msg);
    return;
  }

  console.log(`[notifier] send failed for ${id}: ${failure}`);
  hop({ hopId: sendHop, correlationId: id, causationId: consumeHop, operation: "notify.send", status: failure === "timeout" ? "timeout" : "failed", attempt, startedAt: s0, detail: { error: failure } });
  if (attempt < MAX_RETRIES) {
    hop({ correlationId: id, causationId: consumeHop, operation: "notify.retry", status: "retried", attempt, detail: { error: failure } });
    try { ch.nack(msg, false, false); } catch { /* gone */ }
    return;
  }
  const dl = hop({ correlationId: id, causationId: sendHop, operation: "notify.dead_letter", status: "dead_lettered", attempt, detail: { lastError: failure } });
  await publishConfirmed("orders.dlq.exchange", "", msg.content, {
    persistent: true,
    contentType: "application/json",
    messageId: newId(),
    headers: {
      ...withoutRetryHistory(msg.properties.headers),
      originQueue: "notifications.queue", lastError: failure, attempts: attempt,
      deadLetteredAt: new Date().toISOString(), correlationId: id, causationId: dl,
    },
  });
  ack(ch, msg);
}

const rt = startService({
  name: "notifier",
  queues: { "notifications.queue": onMessage },
});