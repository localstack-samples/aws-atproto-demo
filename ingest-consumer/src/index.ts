// Always-on ingest consumer: connects to the ATProto firehose and fans events
// into AWS.
//
// Why Fargate instead of Lambda?
// The firehose is a WebSocket — a long-lived, always-open connection. Lambda
// functions are short-lived and time out; Fargate runs a container 24/7.
//
// This process does exactly two things for every Jetstream commit it receives:
//   1. Append the raw JSON line to S3 (durable archive you can replay later).
//   2. Publish a normalized copy to Amazon EventBridge so downstream
//      consumers (analytics, moderation, notifications Lambdas) can react
//      without knowing about each other or about this ingest process.
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import {
  EventBridgeClient,
  PutEventsCommand,
} from "@aws-sdk/client-eventbridge";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from "@aws-sdk/lib-dynamodb";
import WebSocket from "ws";

const FIREHOSE_URL = process.env.FIREHOSE_URL ?? "ws://localhost:8080";
const S3_BUCKET = process.env.S3_BUCKET ?? "raw-firehose-archive";
const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME ?? "atproto-events";
const CURSOR_TABLE_NAME = process.env.CURSOR_TABLE_NAME ?? "IngestCursor";
const FLUSH_MAX_EVENTS = Number(process.env.FLUSH_MAX_EVENTS ?? 20);
const FLUSH_INTERVAL_MS = Number(process.env.FLUSH_INTERVAL_MS ?? 10_000);

// LocalStack S3 expects path-style URLs (bucket in the path, not the hostname).
const s3 = new S3Client({ forcePathStyle: true });
const eventBridge = new EventBridgeClient({});
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

// ATProto "collections" name the kind of record in a commit. We map each
// to an EventBridge detail-type so rules can filter on event kind without
// parsing ATProto-specific fields.
//
// These are Leaflet's (https://leaflet.pub) lexicons, not Bluesky's
// app.bsky.* ones - see the README's "Pointing at the real firehose"
// section for why: Leaflet's real traffic volume is low enough for this
// demo's infra to keep up with, unlike the full social firehose.
const DETAIL_TYPE_BY_COLLECTION: Record<string, string> = {
  "pub.leaflet.document": "document.created",
  "pub.leaflet.comment": "comment.created",
  "pub.leaflet.graph.subscription": "subscription.created",
};

// Jetstream v2 sends proposal-0015 JSON envelopes. Only `#commit` messages
// contain records; account, identity, sync, and info messages have different
// shapes and are excluded by the `kinds=commit` subscription filter.
type JetstreamCommit = {
  seq: number;
  did: string;
  time: string;
  operation: string;
  collection: string;
  record?: unknown;
};

function parseCommit(raw: unknown): JetstreamCommit | undefined {
  if (typeof raw !== "object" || raw === null) return;
  const message = raw as { $type?: unknown; payload?: unknown };
  if (message.$type !== "message" || typeof message.payload !== "object" || message.payload === null) {
    return;
  }

  const payload = message.payload as Record<string, unknown>;
  if (
    payload.$type !== "network.bsky.jetstream.subscribeEvents#commit" ||
    typeof payload.seq !== "number" ||
    typeof payload.did !== "string" ||
    typeof payload.time !== "string" ||
    typeof payload.operation !== "string" ||
    typeof payload.collection !== "string"
  ) {
    return;
  }

  return {
    seq: payload.seq,
    did: payload.did,
    time: payload.time,
    operation: payload.operation,
    collection: payload.collection,
    record: payload.record,
  };
}

// The sequence cursor is stored separately from the poller cursor so either
// ingestion stack can be deployed independently. Jetstream resumes
// inclusively, making this at-least-once: the last processed event may replay
// after a restart, but an outage cannot create a gap.
const CURSOR_KEY = "fargate";

async function getCursor(): Promise<number> {
  const result = await ddb.send(
    new GetCommand({ TableName: CURSOR_TABLE_NAME, Key: { id: CURSOR_KEY } }),
  );
  return result.Item?.cursor ?? 0;
}

async function saveCursor(cursor: number) {
  await ddb.send(
    new PutCommand({
      TableName: CURSOR_TABLE_NAME,
      Item: { id: CURSOR_KEY, cursor },
    }),
  );
}

// --- S3 raw archive: buffer NDJSON, flush on count or timer -----------------

let buffer: string[] = [];

async function flushToS3() {
  if (buffer.length === 0) return;
  const batch = buffer;
  buffer = [];

  const now = new Date();
  const prefix = [
    now.getUTCFullYear(),
    String(now.getUTCMonth() + 1).padStart(2, "0"),
    String(now.getUTCDate()).padStart(2, "0"),
    String(now.getUTCHours()).padStart(2, "0"),
  ].join("/");
  const key = `raw/${prefix}/${crypto.randomUUID()}.ndjson`; // e.g. raw/2026/07/28/14/<uuid>.ndjson

  await s3.send(
    new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
      Body: batch.join("\n") + "\n",
      ContentType: "application/x-ndjson",
    }),
  );
  console.log(`flushed ${batch.length} events -> s3://${S3_BUCKET}/${key}`);
}

setInterval(flushToS3, FLUSH_INTERVAL_MS);

// --- EventBridge fan-out: thin normalization, then PutEvents ----------------

async function publishToEventBridge(commit: JetstreamCommit) {
  const detailType = DETAIL_TYPE_BY_COLLECTION[commit.collection];
  if (!detailType) return; // ignore collections this demo doesn't route

  // EventBridge is AWS's event bus — publish once, many subscribers can react.
  const result = await eventBridge.send(
    new PutEventsCommand({
      Entries: [
        {
          EventBusName: EVENT_BUS_NAME,
          Source: "atproto.ingest",
          DetailType: detailType,
          Detail: JSON.stringify({
            did: commit.did,
            cursor: commit.seq,
            time: commit.time,
            collection: commit.collection,
            operation: commit.operation,
            record: commit.record,
          }),
        },
      ],
    }),
  );

  // PutEvents can succeed at the HTTP level while rejecting an individual
  // entry. With one entry per call, any failed entry must prevent the cursor
  // from advancing so Jetstream can replay it after reconnecting.
  if (result.FailedEntryCount) {
    const failedEntry = result.Entries?.find((entry) => entry.ErrorCode);
    throw new Error(
      `EventBridge rejected event: ${failedEntry?.ErrorCode ?? "unknown error"} ${failedEntry?.ErrorMessage ?? ""}`,
    );
  }
}

// --- WebSocket: reconnect from a durable Jetstream cursor -------------------

function scheduleReconnect() {
  setTimeout(start, 2000);
}

function start() {
  void connect().catch((err) => {
    console.error("failed to connect to firehose:", err);
    scheduleReconnect();
  });
}

async function connect() {
  const cursor = await getCursor();
  const wsUrl = new URL(FIREHOSE_URL);
  if (cursor > 0) wsUrl.searchParams.set("cursor", String(cursor));
  const url = wsUrl.toString();
  const ws = new WebSocket(url);
  let stopped = false;

  // WebSocket message callbacks can overlap when an AWS call awaits. Chain
  // them so a later cursor is never saved ahead of an earlier failed event.
  let processing = Promise.resolve();

  ws.on("open", () => console.log(`connected to firehose at ${url}`));
  ws.on("message", (data) => {
    if (stopped) return;

    processing = processing
      .then(async () => {
        if (stopped) return;
        const line = data.toString();
        const commit = parseCommit(JSON.parse(line));
        if (!commit) return;

        buffer.push(line);
        if (buffer.length >= FLUSH_MAX_EVENTS) flushToS3().catch(console.error);
        await publishToEventBridge(commit);
        await saveCursor(commit.seq);
      })
      .catch((err) => {
        stopped = true;
        console.error("failed to process firehose event:", err);
        ws.terminate();
      });
  });

  // Wait for in-flight, ordered processing before reconnecting. The saved
  // cursor remains on the last successful event, so Jetstream replays any
  // event interrupted by the connection failure.
  ws.on("close", () => {
    console.log("firehose connection closed, reconnecting in 2s");
    void processing.finally(scheduleReconnect);
  });
  ws.on("error", (err) => console.error("firehose connection error:", err.message));
}

start();
