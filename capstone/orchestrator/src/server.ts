import { startService } from "../../shared/service";
startService({ name: "orchestrator", queues: { "saga.queue": (msg, ch) => { ch.ack(msg); } } });
