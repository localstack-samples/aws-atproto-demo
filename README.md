# ATProto Firehose Demo: Ingesting a Decentralized Social Stream on AWS

A small, fully offline demo of an event-driven pattern for ingesting the AT Protocol firehose on AWS: a mock Jetstream-style event emitter feeds an ingest process, which archives raw events to S3 and fans them out via EventBridge to independent, single-purpose consumers (analytics,
moderation, notifications).

The demo has two interchangeable ways to do that ingestion, each its own
standalone, independently deployable stack. One stack delivers near-real-time updates while the other polls for updates on a periodic
basis:

- **`infra-fargate/`** — an always-on Fargate task holds the firehose
  WebSocket open indefinitely. This delivers near-real-time updates but
  requires more infrastructure (a VPC, a container) and will incur more
  costs.
- **`infra-poller/`** — a scheduled Lambda that wakes up every couple of
  minutes, connects just long enough to catch up on what's new, and exits.
  This is cheaper and simpler to operate but comes with latency based on
  the poll interval.

Both publish to the exact same EventBridge bus, with the exact same event shape, so the three fan-out Lambdas (analytics, moderation, notifications) are completely unaware of which one is running. See
[Two ways to ingest](#two-ways-to-ingest) below for the tradeoffs, or jump straight to whichever [Quickstart](#quickstart) matches your use case.

Everything can be run on [LocalStack](https://www.localstack.cloud/) via
[`lstk`](https://github.com/localstack/lstk) without ever hitting real AWS
resources. That is the intended path for learning the architecture and
running the demo end-to-end offline. See
[LocalStack vs. real AWS](#localstack-vs-real-aws) for when to move
workloads to a real account.

### Background

If you're new to ATProto, here's the minimum context:

- **AT Protocol (ATProto)** is the open protocol behind Bluesky that can be
  used for far more than just social posts.
- Each user has a personal data repository identified by a **DID**
  (decentralized ID).
- In the case of Bluesky or other similar sites that rely on ATProto, when
  someone posts, likes, or follows, their repo emits a **commit**, which is
  a JSON object describing what changed.
- The **firehose** is the real-time stream of those commits across the
  network. Bluesky's [Jetstream](https://github.com/bluesky-social/jetstream)
  service exposes it as plain JSON over WebSocket (easier than the raw
  binary firehose, which uses CBOR/CAR encoding).
- This demo mocks Jetstream's JSON shape so you can run the full pipeline
  offline, then swap one URL to point at the real network.

![Architecture: mock firehose → Fargate ingest consumer → S3 archive and EventBridge fan-out → three Lambdas → DynamoDB / SQS](docs/architecture.png)

_(This diagram shows the `infra-fargate/` path. The `infra-poller/`
alternative swaps the Fargate box for a scheduled Lambda — see
[Two ways to ingest](#two-ways-to-ingest) — everything to the right of
ingestion is identical.)_

## Decision Log

| Decision          | Choice                                                           | Why                                                                                                                                                                                                               |
| ----------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Firehose format   | Mock **Jetstream-style JSON**, not raw `subscribeRepos` CBOR/CAR | Raw firehose requires CBOR/CAR decoding — genuinely complex and a distraction from the architecture story. Jetstream is what most real integrations use anyway.                                                   |
| Ingest compute    | **Fargate or Lambda** — two interchangeable options              | See [Two ways to ingest](#two-ways-to-ingest). Fargate holds a persistent connection open; Lambda polls on a schedule. Neither is "correct" — it depends on how much latency your use case can tolerate.          |
| Fan-out mechanism | **EventBridge**, not direct invocation or SNS                    | EventBridge is AWS's event bus. Content-based filtering lets each consumer subscribe to exactly what it needs without the ingest side knowing any of them exist. Consumers can be added or removed independently. |
| Raw storage       | **S3, written before fan-out**                                   | S3 is object storage. The raw NDJSON log is the source of truth, independent of whatever EventBridge consumers exist today — it enables replay/reprocessing later.                                                |

Whichever ingestion stack you deploy, it only does two things: append every
event to S3, and `PutEvents` a normalized version to EventBridge. Neither
one has any idea the analytics, moderation, or notifications Lambdas exist
— that's the whole point of the pattern.

## Two ways to ingest

Both stacks provision the _identical_ downstream contract — same S3 bucket name, same DynamoDB table names, same SQS queue name, same EventBridge bus and rules, same three fan-out Lambdas — which is exactly what makes them swappable. That also means they can't both be deployed at once: deploy one, and if you want to try the other, either reset your LocalStack container (`lstk reset`) or, if you're on real AWS, `cdk destroy` it first.

|                       | `infra-fargate/`                                 | `infra-poller/`                                                                                                                        |
| --------------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| Ingestion compute     | ECS Fargate task, always running                 | Lambda, invoked on a schedule (default every 2 minutes)                                                                                |
| Delivery latency      | Near-real-time (~1s)                             | Bounded by the poll interval                                                                                                           |
| Idle cost             | Pays for a running container 24/7                | Pays only per invocation                                                                                                               |
| Extra infra           | VPC, ECS cluster, Fargate service                | One DynamoDB table (`IngestCursor`) to remember where it left off                                                                      |
| How it resumes        | Never disconnects                                | Reconnects with `?cursor=<time_us>`, replaying anything missed — the same mechanism the real Jetstream `/subscribe` endpoint documents |
| The "spike" demo beat | Counters visibly climb in real time as you watch | Trigger the spike, then the next poll (scheduled, or triggered manually) catches the whole burst up in one batch                       |

Use `infra-fargate/` when you want events processed as they happen. Use
`infra-poller/` when near-real-time isn't required and you'd rather not run
a container around the clock — which is exactly the tradeoff behind
[DevRel-ish](https://github.com/remotesynth/DevRel-ish-)'s interval-based
approach to the same problem.

Jump to [Quickstart](#quickstart) for `infra-fargate/`, or
[Alternative: interval-polling ingestion](#alternative-interval-polling-ingestion-lambda)
for `infra-poller/`.

## Repo layout

```
firehose-mock/        Mock Jetstream WebSocket server + canned sample events
ingest-consumer/      Fargate task app code: firehose -> S3 archive + EventBridge PutEvents
ingest-poller/        Lambda app code: same job, run on a schedule instead of held open
lambdas/
  analytics/           counts events per type, per hour
  moderation/           flags posts matching a keyword list
  notifications/        alerts on new followers of watchlisted accounts
  common/types.ts       shared event shape
infra-fargate/         CDK stack: S3, DynamoDB, SQS, EventBridge, Lambdas, ECS/Fargate
infra-poller/          CDK stack: same storage/EventBridge/Lambdas, scheduled ingest-poller instead
```

## Prerequisites

- Node.js 22+ and Docker (Docker Desktop on macOS/Windows)
- [`lstk`](https://github.com/localstack/lstk) installed and able to run
  `lstk start` (log in first with `lstk login` if it asks)

## Quickstart

This walks through the `infra-fargate/` (always-on) path. For the
`infra-poller/` (scheduled Lambda) alternative, do steps 1-2 here, then
skip to [Alternative: interval-polling ingestion](#alternative-interval-polling-ingestion-lambda).

**1. Start LocalStack**

```bash
LOCALSTACK_ECR_ENDPOINT_STRATEGY=off LOCALSTACK_LAMBDA_RUNTIME_ENVIRONMENT_TIMEOUT=20 lstk start
```

`ECR_ENDPOINT_STRATEGY=off` avoids a per-account ECR subdomain
(`<account>.dkr.ecr.<region>.localhost.localstack.cloud`) that some
machines' DNS resolvers (notably macOS Docker Desktop) fail to resolve,
which otherwise surfaces as a `docker login` timeout or 404 when CDK
pushes the ingest consumer's image in step 3.

`LAMBDA_RUNTIME_ENVIRONMENT_TIMEOUT=20` raises how long LocalStack waits
for a Lambda's execution container to finish starting up before giving
up. This is separate from the function's own configured timeout, and
defaults to just 3 seconds. On a machine with many accumulated Docker
containers/images (common after repeated `cdk deploy`/`destroy` cycles -
`lstk stop` doesn't clean up the Lambda/ECS containers it spawned, only
its own container, so they pile up over a long session), a cold Lambda
container can genuinely take longer than 3s to start, and every
invocation fails with `Timeout while starting up Lambda environment`
until you either raise this or prune stale containers
(`docker container prune -f`).

**2. Install dependencies** (small, independent npm projects — nothing
shared, nothing to hoist). Skip `ingest-poller`/`infra-poller` here if
you're only running the Fargate path:

```bash
(cd firehose-mock && npm install)
(cd ingest-consumer && npm install)
(cd ingest-poller && npm install)
(cd lambdas && npm install)
(cd infra-fargate && npm install)
(cd infra-poller && npm install)
```

**3. Deploy the stack**

```bash
cd infra-fargate
lstk cdk bootstrap   # one-time per LocalStack instance
lstk cdk deploy --require-approval never
```

CDK builds the ingest consumer's Docker image and pushes it to ECR
automatically. The same command works unchanged against real AWS.

`--require-approval never` skips the "review IAM changes" confirmation
prompt. Without it, `cdk deploy` waits for an interactive y/n answer before touching anything. Drop the flag if you want to review IAM
changes before every deploy against real AWS.

This stands up S3, both DynamoDB tables, the SQS queue, the
EventBridge bus and its three content-filtered rules, all three Lambdas,
and an ECS Fargate service running the ingest consumer.

Give Fargate 30–60 seconds after deploy to pull the container image and
start the ingest consumer. If the verification commands in step 5 return
empty results, wait a moment and try again or watch the logs until you
see `connected to firehose`.

**4. Start the mock firehose** (separate terminal)

```bash
cd firehose-mock
npm start
```

The ingest consumer is already running in Fargate and retrying its
WebSocket connection every 2 seconds — it picks up the stream as soon as
the mock server is listening.

**5. Watch it flow**

```bash
# ingest consumer logs: connecting, flushing NDJSON batches to S3
lstk aws logs tail /atproto-demo/ingest-consumer --follow
```

If this immediately fails with `ResourceNotFoundException ... FilterLogEvents`, you probably tailed logs too quickly after deploying — LocalStack's CloudWatch Logs emulation takes a short moment to make a just-created log group queryable. Wait ~15 seconds and try rerunning the command.

```bash
# the raw archive
lstk aws s3 ls s3://raw-firehose-archive/raw/ --recursive

# live per-type, per-hour counters
lstk aws dynamodb scan --table-name AnalyticsCounts

# posts flagged for moderation (sample data includes posts with keywords like "spam", "scam", "click here")
lstk aws dynamodb scan --table-name FlaggedContent

# queued notifications for watchlisted accounts (sample data already includes follows of them)
QUEUE_URL=$(lstk aws sqs get-queue-url --queue-name notifications-queue --query QueueUrl --output text)
lstk aws sqs receive-message --queue-url "$QUEUE_URL" --max-number-of-messages 10
```

You can also review all the resources and data in the browser using the [LocalStack Console](https://app.localstack.cloud/inst/default/status).

The sample data in [`firehose-mock/data/sample-events.ndjson`](firehose-mock/data/sample-events.ndjson) contains a handful of posts with moderation keywords and follows targeting the notification watchlist, so both tables populate within the
first loop.

**6. Trigger the spike — the dramatic beat**

```bash
curl -X POST http://localhost:8080/spike
```

This collapses the mock firehose's event delay for 15 seconds, simulating a burst of real-world activity (a post going viral, a breaking-news moment). Re-run the `AnalyticsCounts` scan from step 5 before and after. The counters jump noticeably, and every downstream consumer keeps up without any of them knowing a spike happened, or that the other two consumers exist.

**7. Clean up**

Cleaning up LocalStack just requires stopping the container.

```bash
lstk stop
```

Alternatively, you can reset the container to try the next deployment option (be sure to also stop the firehose mock).

```bash
lstk reset
```

See [LocalStack vs. real AWS](#localstack-vs-real-aws) for when to prune
Docker containers between runs.

## Alternative: interval-polling ingestion (Lambda)

_If `infra-fargate` is currently deployed, reset your LocalStack container or destroy it first. Both stacks provision identically-named resources, so only one can exist at a time._

This option includes the same same mock firehose and same downstream fan-out, just a different way of getting events onto the bus. Do Quickstart steps 1-2 above first (`infra-poller` needs its own `npm install` too), then:

**1. Deploy `infra-poller` instead of `infra-fargate`**

```bash
cd infra-poller
lstk cdk bootstrap   # one-time per LocalStack instance
lstk cdk deploy --require-approval never
```

**2. Start the mock firehose** (separate terminal)

```bash
cd firehose-mock
npm start
```

**3. Trigger a poll on demand**

The scheduled rule fires every 2 minutes by default (see
`POLL_INTERVAL` in `infra-poller/lib/atproto-poller-stack.ts` to change
it), which is a while to wait around during a live demo. Invoke the
Lambda directly instead — this is the polling path's equivalent of the
Fargate path's `POST /spike`:

```bash
lstk aws lambda invoke --function-name atproto-ingest-poller /dev/stdout
```

Run the same verification commands as the main Quickstart's step 5 (S3,
`AnalyticsCounts`, `FlaggedContent`, the notifications queue) — they all
work identically regardless of which stack published the events. Instead
of tailing logs continuously, check the poller's logs after each
invocation:

```bash
lstk aws logs tail /aws/lambda/atproto-ingest-poller
```

**4. The spike beat, polling-style**

```bash
curl -X POST http://localhost:8080/spike
```

Unlike the Fargate path, nothing happens immediately. The events are
piling up in the mock firehose's rolling history buffer. Invoke the poller (step 3's command) and you can see it catch up the entire burst in a single batch: one S3 archive object, one wave of `PutEvents` calls, one cursor update. The gap in connectivity doesn't lose data, it just gets processed in one go the next time the poller runs.

**5. Clean up**

Cleaning up LocalStack just requires stopping the container.

```bash
lstk stop
```

Alternatively, you can reset the container to try the prior deployment option (be sure to also stop the firehose mock).

```bash
lstk reset
```

See [LocalStack vs. real AWS](#localstack-vs-real-aws) for when to prune
Docker containers between runs.

## LocalStack vs. real AWS

The mock firehose sends ~35 events on a slow loop. The Quickstart exercises the full pipeline (ingest → S3 → EventBridge → three fan-out Lambdas) at a volume your laptop should comfortably emulate. LocalStack handles that workload well, and repeated `cdk deploy` / invoke cycles are exactly the type of local testing LocalStack is designed for.

However, when you point ingestion at the real Bluesky Jetstream it may be diffcult for LocalStack to keep up with the full fan-out pipeline depending on the resources of your machine. This is a volume and emulation issue, not a flaw in the architecture:

|                           | Mock firehose on LocalStack  | Real Jetstream on LocalStack         | Real Jetstream on AWS             |
| ------------------------- | ---------------------------- | ------------------------------------ | --------------------------------- |
| **Purpose**               | Learn the pattern offline    | Smoke-test ingest connectivity       | Realistic load and fan-out        |
| **Typical volume**        | ~35 events, trickle or spike | Thousands of events per poll/connect | Same as Jetstream delivers        |
| **Analytics invocations** | ~35 (one Lambda per event)   | ~3,000+ per poll (one per event)     | Same count, but AWS Lambda scales |
| **LocalStack fit**        | ✅ Intended path             | ⚠️ Ingest OK; full pipeline risky    | N/A                               |

### Why the real firehose strains LocalStack locally

Eeach downstream Lambda runs in its own Docker container inside LocalStack's emulation which differs from production Lambda, where runtimes are reused and concurrency is managed. The Analytics rule matches _every_ event on the bus, so a single poller catch-up of ~3,000 events can trigger ~3,000 Analytics invocations in quick succession, plus Moderation and Notifications where rules match. That can spawn thousands of Node processes, exhaust Docker memory, and leave LocalStack unresponsive. The ingest side (archive to S3, cursor advance, connection to Jetstream) may work fine; it is the EventBridge fan-out at production volume that does not map cleanly to a laptop.

**What we recommend:**

- **LocalStack + mock firehose** — default for the demo walkthrough. No caveats beyond occasional `docker container prune -f` between long sessions.
- **LocalStack + real Jetstream** — reasonable for a short ingest smoke test (confirm the poller connects, writes S3 objects, advances the cursor). Treat fan-out tables (`AnalyticsCounts`, etc.) as best-effort. Do not invoke the poller in a tight loop. Prune containers between attempts. Not recommended as a sustained load test.
- **Real AWS (sandbox account)** — the right place to run the full pipeline against the real firehose for any serious validation: volume, latency, Lambda concurrency, and downstream behavior at Jetstream rates. The same CDK stacks deploy unchanged only the environment changes.

LocalStack absolutely supports real-world patterns — S3, DynamoDB,
EventBridge rules, Lambda wiring, scheduled invocations, ECS/Fargate. The caveat is matching production event volume to local emulation scaling limits.

**Between long LocalStack sessions** (especially after real-firehose
tests), `lstk reset` does not remove Lambda/ECS containers Docker spawned on your behalf. Prune them before things get sluggish:

```bash
docker container prune -f
```

## Pointing at the real Bluesky firehose

The mock server and the real [Jetstream](https://github.com/bluesky-social/jetstream) service emit the same JSON shape. To ingest the real thing, redeploy with `FIREHOSE_URL` set. Both stacks read it at deploy time and pass it to their ingestion compute:

```bash
# LocalStack (ingest smoke test — see caveats above)
# infra-fargate
cd infra-fargate
lstk cdk bootstrap
FIREHOSE_URL=wss://jetstream2.us-east.bsky.network/subscribe lstk cdk deploy --require-approval never

# infra-poller
cd infra-poller
FIREHOSE_URL=wss://jetstream2.us-east.bsky.network/subscribe lstk cdk deploy --require-approval never

# Real AWS sandbox (recommended for full pipeline at production volume)
cd infra-poller   # or infra-fargate
cdk bootstrap
FIREHOSE_URL=wss://jetstream2.us-east.bsky.network/subscribe cdk deploy
```

No code changes required. The Fargate task picks up the new URL on its
next restart (deploy triggers one automatically); the poller Lambda picks it up on its next invocation.

**After a real-firehose poll on LocalStack**, verify ingest succeeded
before checking downstream tables — S3 archive and poller logs are the
signal that matters locally:

```bash
lstk aws logs tail /aws/lambda/atproto-ingest-poller
lstk aws s3 ls s3://raw-firehose-archive/raw/ --recursive
```

If LocalStack becomes slow or unresponsive, run `docker container prune -f` and `lstk reset`, then redeploy. Avoid back-to-back manual invokes while the 2-minute schedule is active; each catch-up batch fans out to every matching rule.

## What each piece actually does

- **`firehose-mock/`** replays [`sample-events.ndjson`](firehose-mock/data/sample-events.ndjson) (35 hand-authored post/like/follow commit events in Jetstream shape) on a jittered loop, over a plain WebSocket. `POST /spike` temporarily collapses the delay between events.
- **`ingest-consumer/`** (used by `infra-fargate/`) holds the WebSocket connection open, buffers events, flushes NDJSON to S3 on a count/time threshold, and maps each event's ATProto `collection` field to an EventBridge detail-type (`app.bsky.feed.post` → `post.created`, etc.) before publishing it.
- **`ingest-poller/`** (used by `infra-poller/`) does the same archive-then-publish job, but wakes up on a schedule instead of staying connected: reads the last cursor from DynamoDB, connects to the firehose with `?cursor=<value>` for up to 10 seconds to drain whatever's new, archives and publishes it all in one batch, then saves the new cursor and exits. The collection → detail-type mapping is duplicated from `ingest-consumer/` rather than shared. Each ingestion path stays readable on its own, without cross-referencing the other's internals.
- **`lambdas/analytics`** matches every event on the bus and increments a per-event-type, per-hour counter in DynamoDB.
- **`lambdas/moderation`** is only invoked for `post.created` events whose `record.text` matched a keyword list via EventBridge's wildcard content filtering. The Lambda itself just records the hit in DynamoDB. (EventBridge filters are case-sensitive and match substrings literally, not by meaning. A full-featured moderation system would apply semantic analysis or human review on top of keyword filters.)

- **`lambdas/notifications`** is only invoked for `follow.created` events where `record.subject` (the account being followed) is on a watchlist, again filtered by EventBridge before the Lambda runs. It pushes an SQS message as a stand-in for a push notification or webhook.

Each Lambda is intentionally under 30 lines: one trigger, one job, one
output. The filtering logic lives in the EventBridge rules
(identical in both `infra-fargate/lib/atproto-fargate-stack.ts` and
`infra-poller/lib/atproto-poller-stack.ts`), not in
application code.
