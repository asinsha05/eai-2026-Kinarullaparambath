# AI-usage note (PA7)

I used Claude (Anthropic) as an assistant for this assignment.

What the AI did: it wrote the code for worker/src/activities/index.ts and worker/src/workflows/checkout.ts, and it drafted the text of docs/adr-006.md. The ADR's PA6 facts come from the PA6 server.ts output that I showed it (callDownstream, finalize, the idempotency store, the compensation code).

What I did: I set up the repository and the Docker stack, pasted the code into place, rebuilt and ran the public tests (13 of 13 pass), and ran the crash demo myself. I slowed shipping to 2000 ms, started a checkout with the key crash-3, and killed and restarted pa7-worker. I read the payment and shipping mock logs: one payment authorize and two shipping create calls for that run. The ADR's crash paragraph describes those results.

Not done: I did not run the PA6 kill test by hand. The ADR's description of what PA6 leaves behind comes from reading its code, not from running it.
