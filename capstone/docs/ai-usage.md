# AI usage

## Tools used

Claude (Claude Sonnet 5.5) in the Claude desktop app, as a chat assistant. GitHub Desktop and Visual Studio Code for editing and pushing. No other AI tool.

## What for

The four service skeletons and the shared runtime, adapting my PA4 translators to the intake, the saga consumer based on my PA6 orchestrator, the notifier, the DLQ endpoints, the observer, and the text of the documents in docs/. I ran everything myself in Docker and Terminal.

## What I changed or rejected

Pasting long files into Terminal failed several times. The notifier stayed a two-line placeholder that acked every message until I put the real file in through VS Code; stage 4 failed because of it and its message pointed straight at the problem. The first intake had no /dlq route until dlq.ts was added (the tests showed `Cannot GET /dlq`). I decided not to build the advanced component (Temporal mode): my PA7 code exists, but it is not ported, and README.md says so.

## What I verified, and how

I ran the public tests stage by stage (`node tests/stage.mjs 0` to `5`); stages 0 to 5 pass, 28 of 28 tests. I read the failure messages and the notifier's log when stage 4 failed. I did not run the hand checks for step 6 of the brief, so restart-proof stores, runtime reconnect and concurrency are unverified by me.

## Dependencies beyond the allow-list

None beyond the allow-list: amqplib, express, fast-xml-parser, ajv and ajv-formats. tsx runs the TypeScript directly, as in my earlier assignments, and is development tooling.