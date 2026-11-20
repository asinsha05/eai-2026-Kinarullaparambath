import crypto from "node:crypto";
import type { ConfirmChannel } from "amqplib";

export interface Hop {
  hopId?: string;
  correlationId: string;
  causationId: string | null;
  service: string;
  operation: string;
  status: string;
  attempt?: number;
  messageId?: string;
  startedAt?: Date;
  finishedAt?: Date;
  detail?: Record<string, unknown>;
}

export const newId = (): string => crypto.randomUUID();

// Fire and forget: a lost hop must never fail the business operation (R-4.8.2).
export function emitHop(channel: () => ConfirmChannel | undefined, h: Hop): string {
  const hopId = h.hopId ?? newId();
  const started = h.startedAt ?? new Date();
  const finished = h.finishedAt ?? new Date();
  const rec: Record<string, unknown> = {
    hopId,
    correlationId: h.correlationId,
    causationId: h.causationId,
    service: h.service,
    operation: h.operation,
    status: h.status,
    attempt: h.attempt ?? 1,
    startedAt: started.toISOString(),
    finishedAt: finished.toISOString(),
    durationMs: Math.max(0, finished.getTime() - started.getTime()),
  };
  if (h.messageId) rec.messageId = h.messageId;
  if (h.detail) rec.detail = h.detail;
  try {
    channel()?.publish("trace.events", "", Buffer.from(JSON.stringify(rec)), {
      persistent: true,
      contentType: "application/json",
      messageId: newId(),
    }, () => {});
  } catch {
    /* ignore */
  }
  return hopId;
}
