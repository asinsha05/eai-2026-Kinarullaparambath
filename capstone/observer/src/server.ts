import { startService } from "../../shared/service";
startService({ name: "observer", queues: { "trace.store": (msg, ch) => { ch.ack(msg); } } });
