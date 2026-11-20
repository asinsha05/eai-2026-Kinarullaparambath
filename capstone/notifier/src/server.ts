import { startService } from "../../shared/service";
startService({ name: "notifier", queues: { "notifications.queue": (msg, ch) => { ch.ack(msg); } } });
