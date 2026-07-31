// Infrastructure for the ATProto firehose demo's interval-polling
// alternative — one CDK stack, read top to bottom. Same downstream
// contract (storage, EventBridge bus, fan-out Lambdas) as
// infra-fargate/lib/atproto-fargate-stack.ts using identical resource
// names, so only one of the two stacks can be deployed at a time. The only
// real difference is how events get onto the bus in the first place: a
// scheduled Lambda that polls, instead of an always-on Fargate task.
//
// AWS pieces (quick glossary):
// - S3: object storage for the raw event archive
// - DynamoDB: serverless key-value tables for analytics counts, flagged
//   posts, and the ingest cursor (where the poller left off last time)
// - SQS: message queue standing in for outbound notifications
// - EventBridge: event bus with content-based rules (the fan-out
//   mechanism) - also schedules the poller Lambda itself
// - Lambda: short-lived functions, both the fan-out consumers and the
//   scheduled ingest poller
import * as path from "node:path";
import { Stack, StackProps, RemovalPolicy, Duration } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";

// Demo-only config. Production systems would load these from a database or
// parameter store. Keywords must appear literally in post text — EventBridge
// wildcard filters are case-sensitive.
const MODERATION_KEYWORDS = ["spam", "scam", "free money", "click here"];
// DIDs (decentralized IDs) of accounts we want follow alerts for.
const NOTIFICATION_WATCHLIST_DIDS = [
  "did:plc:watchweb3vip1",
  "did:plc:watchweb3vip2",
];

// How often the poller Lambda wakes up and checks for new events. Shorter
// = lower latency but more invocations; this is the one knob that trades
// off against ingest-consumer's near-real-time delivery. Easy to tune.
const POLL_INTERVAL = Duration.minutes(2);

export class AtprotoPollerStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // --- Storage --------------------------------------------------------

    const rawArchiveBucket = new s3.Bucket(this, "RawArchiveBucket", {
      bucketName: "raw-firehose-archive",
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    const analyticsCounts = new dynamodb.Table(this, "AnalyticsCounts", {
      tableName: "AnalyticsCounts",
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const flaggedContent = new dynamodb.Table(this, "FlaggedContent", {
      tableName: "FlaggedContent",
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // Only the poller uses this - it's how a stateless, scheduled Lambda
    // remembers where it left off between invocations.
    const ingestCursor = new dynamodb.Table(this, "IngestCursor", {
      tableName: "IngestCursor",
      partitionKey: { name: "id", type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const notificationsQueue = new sqs.Queue(this, "NotificationsQueue", {
      queueName: "notifications-queue",
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // --- EventBridge bus: central fan-out point -----------------------------

    const eventBus = new events.EventBus(this, "AtprotoEventBus", {
      eventBusName: "atproto-events",
    });

    // --- Downstream Lambdas (analytics, moderation, notifications) ----------
    // Identical to infra-fargate: same shared lambdas/ directory, same
    // NodejsFunction setup. These don't know or care which ingestion path
    // published the events they're reacting to.
    const lambdasDir = path.join(__dirname, "../../lambdas");
    const lambdaDefaults = {
      runtime: lambda.Runtime.NODEJS_22_X,
      projectRoot: lambdasDir,
      depsLockFilePath: path.join(lambdasDir, "package-lock.json"),
      bundling: {
        // Use local esbuild (see infra-poller/package.json) — avoids Docker
        // bundling on Apple Silicon and the linux/amd64 platform mismatch
        // warning.
        forceDockerBundling: false,
      },
      // CDK's default Lambda timeout is 3s. LocalStack's Lambda cold-start
      // (spinning up a fresh container per invocation) can take longer than
      // that on a loaded machine, which then times out during init before
      // the handler even runs - every invocation fails, not intermittently.
      timeout: Duration.seconds(10),
    };

    // Analytics: receives every event the poller publishes.
    const analyticsFn = new NodejsFunction(this, "AnalyticsFunction", {
      ...lambdaDefaults,
      entry: path.join(lambdasDir, "analytics/index.ts"),
      environment: { TABLE_NAME: analyticsCounts.tableName },
    });
    analyticsCounts.grantWriteData(analyticsFn);

    // Moderation: only invoked when EventBridge's rule matches (see below).
    const moderationFn = new NodejsFunction(this, "ModerationFunction", {
      ...lambdaDefaults,
      entry: path.join(lambdasDir, "moderation/index.ts"),
      environment: { TABLE_NAME: flaggedContent.tableName },
    });
    flaggedContent.grantWriteData(moderationFn);

    // Notifications: only invoked for watchlisted follow events (see below).
    const notificationsFn = new NodejsFunction(this, "NotificationsFunction", {
      ...lambdaDefaults,
      entry: path.join(lambdasDir, "notifications/index.ts"),
      environment: { QUEUE_URL: notificationsQueue.queueUrl },
    });
    notificationsQueue.grantSendMessages(notificationsFn);

    // --- EventBridge rules: the core of the decoupled fan-out pattern --------
    // Identical to infra-fargate — same patterns, same targets.

    new events.Rule(this, "AnalyticsRule", {
      eventBus,
      eventPattern: { source: ["atproto.ingest"] },
      targets: [new targets.LambdaFunction(analyticsFn)],
    });

    // One rule per keyword, not one rule with all keywords OR'd together —
    // see infra-fargate/lib/atproto-fargate-stack.ts for why (EventBridge's
    // wildcard-complexity limit).
    MODERATION_KEYWORDS.forEach((keyword, i) => {
      new events.Rule(this, `ModerationRule${i}`, {
        eventBus,
        eventPattern: {
          source: ["atproto.ingest"],
          detailType: ["post.created"],
          detail: {
            record: { text: [{ wildcard: `*${keyword}*` }] },
          },
        },
        targets: [new targets.LambdaFunction(moderationFn)],
      });
    });

    new events.Rule(this, "NotificationsRule", {
      eventBus,
      eventPattern: {
        source: ["atproto.ingest"],
        detailType: ["follow.created"],
        detail: { record: { subject: NOTIFICATION_WATCHLIST_DIDS } },
      },
      targets: [new targets.LambdaFunction(notificationsFn)],
    });

    // --- Ingest poller (scheduled Lambda) -----------------------------------
    // Wakes up on a schedule, connects to the firehose just long enough to
    // drain what's new since its last saved cursor, then exits. No
    // persistent connection, no VPC, no idle compute between polls - the
    // tradeoff is delivery latency bounded by POLL_INTERVAL instead of
    // near-real-time.

    const pollerFn = new NodejsFunction(this, "IngestPollerFunction", {
      runtime: lambda.Runtime.NODEJS_22_X,
      // Own project root: unlike the fan-out Lambdas above, this one bundles
      // a real dependency (ws) that isn't part of the Lambda runtime, so it
      // needs to be physically installed in ingest-poller/node_modules.
      projectRoot: path.join(__dirname, "../../ingest-poller"),
      depsLockFilePath: path.join(
        __dirname,
        "../../ingest-poller/package-lock.json",
      ),
      bundling: { forceDockerBundling: false },
      entry: path.join(__dirname, "../../ingest-poller/index.ts"),
      // Explicit, predictable name so it's copy-pasteable in the README's
      // manual "trigger a poll now" `aws lambda invoke` command.
      functionName: "atproto-ingest-poller",
      // Poll window (10s default) + margin for a large catch-up batch
      // (e.g. after a traffic spike) to archive and fan out before timing
      // out - a timeout here would archive to S3 but never save the new
      // cursor, silently reprocessing the same range on the next poll.
      timeout: Duration.seconds(60),
      memorySize: 256,
      environment: {
        // host.docker.internal = the machine running Docker (your laptop) -
        // LocalStack runs this Lambda in its own container, same as it does
        // for ECS tasks, so it reaches the host-run mock firehose the same
        // way infra-fargate's ingest-consumer does.
        FIREHOSE_URL: process.env.FIREHOSE_URL ?? "ws://host.docker.internal:8080",
        S3_BUCKET: rawArchiveBucket.bucketName,
        EVENT_BUS_NAME: eventBus.eventBusName,
        CURSOR_TABLE_NAME: ingestCursor.tableName,
      },
    });

    rawArchiveBucket.grantWrite(pollerFn);
    eventBus.grantPutEventsTo(pollerFn);
    ingestCursor.grantReadWriteData(pollerFn);

    new events.Rule(this, "IngestPollerSchedule", {
      schedule: events.Schedule.rate(POLL_INTERVAL),
      targets: [new targets.LambdaFunction(pollerFn)],
    });
  }
}
