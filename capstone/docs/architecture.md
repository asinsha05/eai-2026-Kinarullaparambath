# Architecture

## System context

Orders arrive from three kinds of client, an operator watches the system, and the system calls five providers.

```mermaid
flowchart LR
  web["Web client JSON"] -->|"POST /orders"| sys
  mobile["Mobile client JSON"] -->|"POST /orders"| sys
  b2b["B2B partner XML windows-1257"] -->|"POST /orders"| sys
  op["Operator"] -->|"trace, metrics, dlq, sagas"| sys
  sys["Capstone: intake, orchestrator, notifier, observer"]
  sys -->|"price lookup"| pricing["Pricing API"]
  sys -->|"authorize, refund"| payment["Payment API"]
  sys -->|"reserve, release"| inventory["Inventory API"]
  sys -->|"create"| shipping["Shipping API"]
  sys -->|"send"| notification["Notification API"]
```

## Integration architecture

```mermaid
flowchart TB
  client([Client]) -->|"POST /orders"| intake
  intake -->|"enrich"| pricing["Pricing API"]
  intake -->|"order.accepted"| ox["orders.exchange"]
  ox --> sq["saga.queue"]
  sq --> orch["orchestrator"]
  sq -. "nack" .-> rx["orders.retry.exchange"]
  rx --> srq["saga.retry.queue"]
  srq -. "TTL 1 s" .-> rtx["orders.return.exchange"]
  rtx --> sq
  orch --> providers["payment, inventory, shipping, notification"]
  orch -->|"order.* events"| oo["orders.outcome"]
  oo -->|"order.failed, order.compensated"| nq["notifications.queue"]
  oo --> iq["intake.outcome"]
  iq --> intake
  nq --> notifier
  notifier -->|"notice"| providers
  orch -->|"after MAX_RETRIES"| dx["orders.dlq.exchange"]
  notifier -->|"after MAX_RETRIES"| dx
  dx --> dlq["orders.dlq"]
  orch -->|"unparseable"| ix["orders.invalid.exchange"]
  ix --> inv["orders.invalid"]
  intake -. "hop" .-> te["trace.events"]
  orch -. "hop" .-> te
  notifier -. "hop" .-> te
  te --> ts["trace.store"]
  ts --> observer
```

The solid arrows are the business flow; the dotted ones are the retry path (a nack goes through `orders.retry.exchange` into a one-second delay queue and back to `saga.queue`) and the wire tap into `trace.events`. The notifier has the same retry path through `notifications.retry.queue`.