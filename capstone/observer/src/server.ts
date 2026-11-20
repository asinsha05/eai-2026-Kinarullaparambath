import { appendFileSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import type { Channel, ConsumeMessage } from "amqplib";
import { startService } from "../../shared/service";

const env = process.env;
const DATA_DIR = env.DATA_DIR ?? "/data";
const FILE = `${DATA_DIR}/trace-store.jsonl`;
const MGMT = env.RABBITMQ_MGMT_URL ?? "http://rabbitmq:15672";
const MGMT_AUTH = "Basic " + Buffer.from(`${env.RABBITMQ_MGMT_USER ?? "eai"}:${env.RABBITMQ_MGMT_PASSWORD ?? "eai-capstone"}`).toString("base64");
const ORCH = env.ORCHESTRATOR_URL ?? "http://orchestrator:8802";
const SAGA_LAG = Number(env.SAGA_LAG_THRESHOLD ?? "5");
const NOTIFIER_LAG = Number(env.NOTIFIER_LAG_THRESHOLD ?? "5");

// ---- the trace store: JSONL on the volume, indexed by correlationId in memory ----
const byCid = new Map<string, any[]>();
const seen = new Set<string>();
function addHop(h: any): void {
  seen.add(h.hopId);
  const list = byCid.get(h.correlationId);
  if (list) list.push(h); else byCid.set(h.correlationId, [h]);
}
mkdirSync(DATA_DIR, { recursive: true });
if (existsSync(FILE)) {
  for (const line of readFileSync(FILE, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try {
      const h = JSON.parse(line);
      if (h && typeof h.hopId === "string" && typeof h.correlationId === "string") addHop(h);
    } catch { /* a torn last line after a kill: skip it */ }
  }
}

function onHop(msg: ConsumeMessage, ch: Channel): void {
  try {
    const h = JSON.parse(msg.content.toString("utf8"));
    if (h && typeof h.hopId === "string" && typeof h.correlationId === "string" && !seen.has(h.hopId)) {
      appendFileSync(FILE, JSON.stringify(h) + "\n", "utf8");
      addHop(h);
    }
  } catch { /* an unreadable hop is dropped: a lost hop never blocks the pipeline */ }
  try { ch.ack(msg); } catch { /* redelivered */ }
}

// ---- metrics ----
const QUEUES = ["saga.queue", "notifications.queue", "orders.dlq", "orders.invalid", "trace.store"];

async function unackedByQueue(): Promise<Record<string, number>> {
  try {
    const r = await fetch(`${MGMT}/api/queues`, { headers: { Authorization: MGMT_AUTH }, signal: AbortSignal.timeout(2000) });
    if (!r.ok) return {};
    const arr = (await r.json()) as Array<{ name: string; messages_unacknowledged?: number }>;
    const out: Record<string, number> = {};
    for (const q of arr) out[q.name] = Number(q.messages_unacknowledged ?? 0);
    return out;
  } catch {
    return {};
  }
}

async function stuckCount(): Promise<number> {
  try {
    const r = await fetch(`${ORCH}/sagas/stuck`, { signal: AbortSignal.timeout(2000) });
    if (!r.ok) return 0;
    const b: any = await r.json();
    return Array.isArray(b.sagas) ? b.sagas.length : 0;
  } catch {
    return 0;
  }
}

const rt = startService({
  name: "observer",
  prefetch: 20,
  queues: { "trace.store": onHop },
  configure(app) {
    app.get("/trace/:correlationId", (req, res) => {
      const hops = byCid.get(req.params.correlationId);
      if (!hops || hops.length === 0) { res.status(404).json({ code: "not_found" }); return; }
      const sorted = [...hops].sort((a, b) => String(a.startedAt).localeCompare(String(b.startedAt)));
      res.status(200).json({ correlationId: req.params.correlationId, hops: sorted });
    });

    app.get("/ops/metrics", async (_req, res) => {
      const ch = rt.channel();
      if (!ch) { res.status(503).json({ code: "broker_unavailable" }); return; }
      try {
        const unacked = await unackedByQueue();
        const queues: Record<string, { ready: number; unacked: number; consumers: number }> = {};
        for (const name of QUEUES) {
          const q = await ch.checkQueue(name); // exact ready count and consumer count
          queues[name] = { ready: q.messageCount, unacked: unacked[name] ?? 0, consumers: q.consumerCount };
        }
        const dlqDepth = queues["orders.dlq"].ready + queues["orders.dlq"].unacked;
        const invalidDepth = queues["orders.invalid"].ready + queues["orders.invalid"].unacked;
        const stuckSagas = await stuckCount();
        const alert = (key: string, value: number, firing: boolean, threshold: number | null) => ({ key, firing, value, threshold });
        const alerts = [
          alert("dlq_not_empty", dlqDepth, dlqDepth > 0, null),
          alert("invalid_not_empty", invalidDepth, invalidDepth > 0, null),
          alert("saga_consumer_missing", queues["saga.queue"].consumers, queues["saga.queue"].consumers === 0, null),
          alert("notifier_consumer_missing", queues["notifications.queue"].consumers, queues["notifications.queue"].consumers === 0, null),
          alert("saga_consumer_lag", queues["saga.queue"].ready, queues["saga.queue"].ready > SAGA_LAG, SAGA_LAG),
          alert("notifier_consumer_lag", queues["notifications.queue"].ready, queues["notifications.queue"].ready > NOTIFIER_LAG, NOTIFIER_LAG),
          alert("stuck_sagas", stuckSagas, stuckSagas > 0, null),
        ];
        res.status(200).json({ generatedAt: new Date().toISOString(), queues, dlqDepth, invalidDepth, stuckSagas, alerts });
      } catch (err) {
        console.error("[observer] metrics failed:", err);
        res.status(503).json({ code: "broker_unavailable" });
      }
    });
  },
});
