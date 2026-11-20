import type { Express } from "express";
import type { Channel, ConsumeMessage } from "amqplib";
import type { Runtime } from "../../shared/service";
import { withoutRetryHistory } from "../../shared/rabbit";
import { newId, type Hop } from "../../shared/trace";

type HopFn = (h: Omit<Hop, "service">) => string;
const DEFAULT_ATTEMPTS = Number(process.env.MAX_RETRIES ?? "3");

let lock: Promise<unknown> = Promise.resolve();
function serial<T>(fn: () => Promise<T>): Promise<T> {
  const run = lock.then(fn, fn);
  lock = run.catch(() => {});
  return run;
}

async function drain(ch: Channel): Promise<ConsumeMessage[]> {
  const out: ConsumeMessage[] = [];
  for (;;) {
    const m = await ch.get("orders.dlq", { noAck: false });
    if (!m) break;
    out.push(m);
  }
  return out;
}

const headersOf = (m: ConsumeMessage): Record<string, any> => (m.properties.headers ?? {}) as Record<string, any>;

function entryOf(m: ConsumeMessage) {
  const h = headersOf(m);
  return {
    messageId: String(m.properties.messageId ?? ""),
    correlationId: String(h.correlationId ?? ""),
    originQueue: String(h.originQueue ?? ""),
    attempts: Number(h.attempts ?? DEFAULT_ATTEMPTS),
    lastError: String(h.lastError ?? ""),
    deadLetteredAt: String(h.deadLetteredAt ?? new Date().toISOString()),
  };
}

function publishConfirmed(rt: Runtime, exchange: string, key: string, content: Buffer, options: Record<string, unknown>): Promise<void> {
  const ch = rt.confirmChannel();
  if (!ch) return Promise.reject(new Error("no broker channel"));
  return Promise.race([
    new Promise<void>((resolve, reject) => {
      ch.publish(exchange, key, content, options, (err) => (err ? reject(err) : resolve()));
    }),
    new Promise<void>((_, reject) => setTimeout(() => reject(new Error("publish confirm timeout")), 5000)),
  ]);
}

export function registerDlq(app: Express, getRt: () => Runtime, hop: HopFn): void {
  app.get("/dlq", async (_req, res) => {
    const ch = getRt().channel();
    if (!ch) { res.status(503).json({ code: "broker_unavailable" }); return; }
    try {
      const out = await serial(async () => {
        const msgs = await drain(ch);
        const entries = msgs.map(entryOf);
        for (const m of msgs) ch.nack(m, false, true);
        return entries;
      });
      res.status(200).json({ count: out.length, entries: out });
    } catch (err) {
      console.error("[intake] GET /dlq failed:", err);
      res.status(500).json({ code: "internal_error" });
    }
  });

  app.post("/dlq/replay", async (req, res) => {
    const rt = getRt();
    const ch = rt.channel();
    if (!ch) { res.status(503).json({ code: "broker_unavailable" }); return; }
    let want: string | undefined;
    try {
      const raw = Buffer.isBuffer(req.body) ? req.body.toString("utf8").trim() : "";
      const body = raw ? JSON.parse(raw) : {};
      if (body && typeof body.correlationId === "string") want = body.correlationId;
    } catch {
      res.status(400).json({ code: "validation_error", message: "body must be JSON" });
      return;
    }
    try {
      const result = await serial(async () => {
        const msgs = await drain(ch);
        let replayed = 0;
        for (const m of msgs) {
          const h = headersOf(m);
          const cid = String(h.correlationId ?? "");
          const origin = String(h.originQueue ?? "");
          if ((want && cid !== want) || !origin) { ch.nack(m, false, true); continue; }
          const replayHop = newId();
          const t0 = new Date();
          try {
            await publishConfirmed(rt, "orders.return.exchange", origin, m.content, {
              persistent: true,
              contentType: m.properties.contentType ?? "application/json",
              messageId: newId(),
              headers: { ...withoutRetryHistory(h), correlationId: cid, causationId: replayHop },
            });
            ch.ack(m);
            replayed++;
            hop({ hopId: replayHop, correlationId: cid, causationId: typeof h.causationId === "string" ? h.causationId : null, operation: "dlq.replay", status: "success", startedAt: t0, detail: { originQueue: origin } });
          } catch {
            ch.nack(m, false, true);
          }
        }
        return { replayed, remaining: msgs.length - replayed };
      });
      res.status(200).json(result);
    } catch (err) {
      console.error("[intake] POST /dlq/replay failed:", err);
      res.status(500).json({ code: "internal_error" });
    }
  });
}
