import express, { type Express } from "express";
import amqp, { type Channel, type ConfirmChannel, type ChannelModel, type ConsumeMessage } from "amqplib";

export type Handler = (msg: ConsumeMessage, ch: Channel) => Promise<void> | void;

export interface Runtime {
  channel(): Channel | undefined;
  confirmChannel(): ConfirmChannel | undefined;
  connected(): boolean;
}

export interface ServiceOptions {
  name: string;
  queues: Record<string, Handler>;
  prefetch?: number;
  configure?: (app: Express, rt: Runtime) => void;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function startService(opts: ServiceOptions): Runtime {
  const url = process.env.RABBITMQ_URL!;
  const port = Number(process.env.PORT ?? 3000);
  let ch: Channel | undefined;
  let confirmCh: ConfirmChannel | undefined;
  const consumers: Record<string, number> = {};
  for (const q of Object.keys(opts.queues)) consumers[q] = 0;

  const rt: Runtime = {
    channel: () => ch,
    confirmChannel: () => confirmCh,
    connected: () => ch !== undefined,
  };

  async function connectLoop(): Promise<void> {
    for (;;) {
      let conn: ChannelModel | undefined;
      try {
        conn = await amqp.connect(url);
        const c = conn;
        const channel = await c.createChannel();
        const confirm = await c.createConfirmChannel();
        c.on("error", (e: Error) => console.error(`[${opts.name}] broker error: ${e.message}`));
        for (const x of [channel, confirm]) {
          x.on("error", (e: Error) => console.error(`[${opts.name}] channel error: ${e.message}`));
          // A dead channel means a half-working service: drop the connection and reconnect.
          x.on("close", () => { c.close().catch(() => {}); });
        }
        await channel.prefetch(opts.prefetch ?? 1);
        for (const [queue, handler] of Object.entries(opts.queues)) {
          await channel.consume(queue, (msg) => {
            if (!msg) return;
            Promise.resolve(handler(msg, channel)).catch((err) => {
              console.error(`[${opts.name}] handler failed on ${queue}:`, err);
              try { channel.nack(msg, false, false); } catch { /* channel gone */ }
            });
          });
          consumers[queue] = 1;
        }
        c.on("close", () => {
          console.warn(`[${opts.name}] broker connection closed; reconnecting`);
          ch = undefined;
          confirmCh = undefined;
          for (const q of Object.keys(consumers)) consumers[q] = 0;
          void connectLoop();
        });
        ch = channel;
        confirmCh = confirm;
        console.log(`[${opts.name}] connected, consuming: ${Object.keys(opts.queues).join(", ") || "nothing"}`);
        return;
      } catch (err) {
        console.warn(`[${opts.name}] broker connect failed: ${(err as Error).message}; retrying`);
        try { await conn?.close(); } catch { /* ignore */ }
        await sleep(2000);
      }
    }
  }

  const app = express();
  app.get("/health", (_req, res) => {
    const ok = ch !== undefined && Object.values(consumers).every((n) => n > 0);
    res.status(ok ? 200 : 503).json({
      status: ok ? "ok" : "degraded",
      checks: { broker: ch ? "up" : "down", consumers: { ...consumers } },
    });
  });
  opts.configure?.(app, rt);
  app.listen(port, () => console.log(`[${opts.name}] listening on ${port}`));
  void connectLoop();
  return rt;
}
