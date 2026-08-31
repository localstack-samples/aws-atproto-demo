// Infrastructure for the ATProto firehose demo — one CDK stack, read top to
// bottom in the same order as the architecture diagram in the README.
//
// AWS pieces (quick glossary):
// - S3: object storage for the raw event archive
// - DynamoDB: serverless key-value tables for analytics counts and flagged posts
// - SQS: message queue standing in for outbound notifications
// - EventBridge: event bus with content-based rules (the fan-out mechanism)
// - Lambda: short-lived functions triggered by EventBridge rules
// - ECS Fargate: always-on container for the WebSocket ingest consumer
import * as path from "node:path";
import { Stack, StackProps, RemovalPolicy, Duration } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ecrAssets from "aws-cdk-lib/aws-ecr-assets";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as lambda from "aws-cdk-lib/aws-lambda";
import { NodejsFunction } from "aws-cdk-lib/aws-lambda-nodejs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as logs from "aws-cdk-lib/aws-logs";

// Demo-only config. Production systems would load these from a database or
// parameter store. Keywords must appear literally in comment text —
// EventBridge wildcard filters are case-sensitive.
const MODERATION_KEYWORDS = ["spam", "scam", "free money", "click here"];
// At-uris of publications we want new-subscriber alerts for.
const NOTIFICATION_WATCHLIST_PUBLICATIONS = [
  "at://did:plc:writer4h2mvz7ndp/pub.leaflet.publication/protocoldigest",
  "at://did:plc:writer8jvbnq5trm/pub.leaflet.publication/weeklyroundup",
];

export class AtprotoFargateStack extends Stack {
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

    // The long-running consumer uses this cursor to resume a Jetstream v2
    // subscription after a task restart or WebSocket disconnect.
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
    // NodejsFunction bundles each .ts file with esbuild at deploy time — no
    // separate compile step.
    const lambdasDir = path.join(__dirname, "../../lambdas");
    const lambdaDefaults = {
      runtime: lambda.Runtime.NODEJS_22_X,
      projectRoot: lambdasDir,
      depsLockFilePath: path.join(lambdasDir, "package-lock.json"),
      bundling: {
        // Use local esbuild (see infra/package.json) — avoids Docker bundling
        // on Apple Silicon and the linux/amd64 platform mismatch warning.
        forceDockerBundling: false,
      },
      // CDK's default Lambda timeout is 3s. LocalStack's Lambda cold-start
      // (spinning up a fresh container per invocation) can take longer than
      // that on a loaded machine, which then times out during init before
      // the handler even runs - every invocation fails, not intermittently.
      timeout: Duration.seconds(10),
    };

    // Analytics: receives every event the ingest consumer publishes.
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
    // Each rule subscribes to a subset of events. Downstream Lambdas never
    // know about each other; the ingest consumer never knows they exist.

    // Rule 1: all events → analytics
    new events.Rule(this, "AnalyticsRule", {
      eventBus,
      eventPattern: { source: ["atproto.ingest"] },
      targets: [new targets.LambdaFunction(analyticsFn)],
    });

    // Rule 2: comment.created + keyword in plaintext → moderation
    // Wildcard filters run BEFORE Lambda — unmatched comments never invoke
    // it. (pub.leaflet.document has no plaintext field - its content lives
    // in nested block pages - so moderation only ever fires on comments.)
    // One rule per keyword, not one rule with all keywords OR'd together:
    // EventBridge caps how complex a wildcard pattern can be, and each
    // "*keyword*" needs a leading + trailing wildcard to match anywhere in
    // the text, so 4 keywords in a single field can exceed that limit
    // ("Rule is too complex" / InvalidEventPatternException). All rules
    // still target the same moderation Lambda.
    MODERATION_KEYWORDS.forEach((keyword, i) => {
      new events.Rule(this, `ModerationRule${i}`, {
        eventBus,
        eventPattern: {
          source: ["atproto.ingest"],
          detailType: ["comment.created"],
          detail: {
            record: { plaintext: [{ wildcard: `*${keyword}*` }] },
          },
        },
        targets: [new targets.LambdaFunction(moderationFn)],
      });
    });

    // Rule 3: subscription.created + watched publication → notifications
    // record.publication is the at-uri of the publication being subscribed to.
    new events.Rule(this, "NotificationsRule", {
      eventBus,
      eventPattern: {
        source: ["atproto.ingest"],
        detailType: ["subscription.created"],
        detail: { record: { publication: NOTIFICATION_WATCHLIST_PUBLICATIONS } },
      },
      targets: [new targets.LambdaFunction(notificationsFn)],
    });

    // --- Ingest consumer (ECS Fargate) --------------------------------------
    // Long-running container that holds the firehose WebSocket open. Runs on
    // the host's Docker network via host.docker.internal so it can reach both
    // LocalStack (port 4566) and the mock firehose (port 8080) on your machine.

    const vpc = new ec2.Vpc(this, "IngestVpc", { maxAzs: 1, natGateways: 0 });
    const cluster = new ecs.Cluster(this, "IngestCluster", { vpc });

    const taskDefinition = new ecs.FargateTaskDefinition(this, "IngestTaskDef", {
      cpu: 256,
      memoryLimitMiB: 512,
    });

    // Empty string (or an explicit empty env var) means "use the real AWS
    // endpoints" - see the environment block below.
    const consumerEndpointUrl =
      process.env.AWS_ENDPOINT_URL_FOR_CONSUMER ??
      "http://host.docker.internal:4566";

    // CDK builds the image and pushes it to ECR automatically - on LocalStack
    // this needs ECR_ENDPOINT_STRATEGY=off (see README) or the docker login
    // during asset publishing can fail on macOS Docker Desktop.
    taskDefinition.addContainer("IngestContainer", {
      image: ecs.ContainerImage.fromAsset(
        path.join(__dirname, "../../ingest-consumer"),
        { platform: ecrAssets.Platform.LINUX_AMD64 },
      ),
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: "ingest-consumer",
        // Fixed name (rather than CDK's default auto-generated one) so
        // it's copy-pasteable in the README's `aws logs tail` command.
        logGroup: new logs.LogGroup(this, "IngestLogGroup", {
          logGroupName: "/atproto-demo/ingest-consumer",
          removalPolicy: RemovalPolicy.DESTROY,
        }),
      }),
      environment: {
        // host.docker.internal = the machine running Docker (your laptop).
        // From inside the Fargate container, this reaches LocalStack and the mock firehose.
        FIREHOSE_URL: process.env.FIREHOSE_URL ?? "ws://host.docker.internal:8080",
        S3_BUCKET: rawArchiveBucket.bucketName,
        EVENT_BUS_NAME: eventBus.eventBusName,
        CURSOR_TABLE_NAME: ingestCursor.tableName,
        // Only set when targeting LocalStack. The SDK reads AWS_ENDPOINT_URL
        // from the environment, so leaving it in place on real AWS would send
        // every S3/EventBridge call to host.docker.internal and fail. Deploy
        // with AWS_ENDPOINT_URL_FOR_CONSUMER= (empty) to omit it entirely.
        ...(consumerEndpointUrl ? { AWS_ENDPOINT_URL: consumerEndpointUrl } : {}),
      },
    });

    rawArchiveBucket.grantWrite(taskDefinition.taskRole);
    eventBus.grantPutEventsTo(taskDefinition.taskRole);
    ingestCursor.grantReadWriteData(taskDefinition.taskRole);

    // Public subnet + public IP is what makes `natGateways: 0` viable: the task
    // needs outbound internet to reach Jetstream, pull its image from ECR, and
    // call the S3/EventBridge APIs, and the internet gateway provides that for
    // free where a NAT gateway would cost ~$33/month. CDK would default to
    // these subnets anyway given `assignPublicIp`, but it's stated explicitly
    // because the placement is the whole reason this works. The security group
    // allows no inbound traffic, so the task is not reachable from outside.
    // In a private-subnet design you'd need a NAT gateway regardless of VPC
    // endpoints, since Jetstream is on the public internet.
    new ecs.FargateService(this, "IngestService", {
      cluster,
      taskDefinition,
      desiredCount: 1,
      assignPublicIp: true,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      minHealthyPercent: 100,
      circuitBreaker: { rollback: true },
    });
  }
}
