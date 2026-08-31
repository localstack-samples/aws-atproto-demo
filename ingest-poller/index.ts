// Interval-based ingestion: the alternative to ingest-consumer's always-on
// WebSocket. Triggered on a schedule (see
// infra-poller/lib/atproto-poller-stack.ts), this Lambda reads the last
// cursor, connects to the firehose just long enough to drain what's new,
// archives it, fans it out, saves the new cursor, and exits — no
// persistent connection, no VPC, no idle compute between polls.
//
// Jetstream v2's subscribeEvents endpoint resumes with `?cursor=<seq>`, a
// monotonic sequence number carried by every event. See the README for the
// tradeoffs versus ingest-consumer.
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
const POLL_WINDOW_MS = Number(process.env.POLL_WINDOW_MS ?? 10_000);

// LocalStack S3 expects path-style URLs (bucket in the path, not the hostname).
const s3 = new S3Client({ forcePathStyle: true });
const eventBridge = new EventBridgeClient({});
const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));

// Same collection -> EventBridge detail-type mapping as ingest-consumer,
// duplicated rather than shared across packages so each ingestion path
// stays independently readable (see README). These are Leaflet's
// (https://leaflet.pub) lexicons - see the README for why.
const DETAIL_TYPE_BY_COLLECTION: Record<string, string> = {
  "pub.leaflet.document": "document.created",
  "pub.leaflet.comment": "comment.created",
  "pub.leaflet.graph.subscription": "subscription.created",
};

// Jetstream v2 sends proposal-0015 JSON envelopes. Only `#commit` messages
// contain records; the subscription uses `kinds=commit`, but this validation
// keeps an unexpected control message from advancing the saved cursor.
type JetstreamCommit = {
  seq: number;
  did: string;
  time: string;
  operation: string;
  collection: string;
  record?: unknown;
  raw: unknown;
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
    raw,
  };
}

const CURSOR_KEY = "poller"; // single fixed row - this demo only runs one poller

// --- Cursor: where we left off last time -----------------------------------

async function getCursor(): Promise<number> {
  const result = await ddb.send(
    new GetCommand({ TableName: CURSOR_TABLE_NAME, Key: { id: CURSOR_KEY } }),
  );
  return result.Item?.cursor ?? 0; // 0 = Jetstream's "before the first event" sentinel
}

async function saveCursor(cursor: number) {
  await ddb.send(
    new PutCommand({
      TableName: CURSOR_TABLE_NAME,
      Item: { id: CURSOR_KEY, cursor },
    }),
  );
}

// --- Firehose: connect briefly, drain, disconnect ---------------------------

// Resolves with whatever arrived within the poll window - including an
// empty array if the firehose never connects in time. A poll with nothing
// new isn't a failure, so this never rejects.
//
// Logged explicitly at each stage (attempting/open/error/close) because a
// real network endpoint can fail in ways a local mock never does - a
// blocked or proxied outbound connection can hang at the TCP/TLS layer
// without ever firing 'error', so this only has the WebSocket's own events
// to go on, not a guarantee they'll fire. The setTimeout below is a second,
// independent guard so the poll still ends even if none of them do.
function drainFirehose(cursor: number): Promise<JetstreamCommit[]> {
  return new Promise((resolve) => {
    const collected: JetstreamCommit[] = [];
    // Built via URL/searchParams rather than string interpolation: once
    // FIREHOSE_URL carries its own query string (e.g. Leaflet's
    // ?collections=... filter - see the README), naively appending
    // "?cursor=" would produce a second "?" and a malformed URL.
    const wsUrl = new URL(FIREHOSE_URL);
    if (cursor > 0) wsUrl.searchParams.set("cursor", String(cursor));
    const url = wsUrl.toString();
    console.log(`connecting to firehose at ${url}`);
    // handshakeTimeout aborts (emitting 'error') if the WS/TLS handshake
    // itself doesn't complete in time - the mock always answers instantly,
    // but a real network endpoint through a blocked or misbehaving proxy
    // can hang at exactly this step without firing any event on its own.
    const ws = new WebSocket(url, { handshakeTimeout: 8_000 });

    let finished = false;
    const finish = (reason: string) => {
      if (finished) return; // 'error' and 'close' can both fire once we terminate()
      finished = true;
      console.log(`poll window ended (${reason}), collected ${collected.length} events`);
      clearTimeout(timer);
      ws.removeAllListeners();
      ws.terminate();
      resolve(collected);
    };

    const timer = setTimeout(() => finish("timeout"), POLL_WINDOW_MS);
    ws.on("open", () => console.log("firehose connection open"));
    ws.on("message", (data) => {
      const commit = parseCommit(JSON.parse(data.toString()));
      if (commit) collected.push(commit);
    });
    ws.on("error", (err) => finish(`error: ${err.message}`));
    ws.on("close", () => finish("closed"));
  });
}

// --- Same archive + fan-out steps as ingest-consumer, run once per poll ----

async function archiveToS3(events: JetstreamCommit[]) {
  const now = new Date();
  const prefix = [
    now.getUTCFullYear(),
    String(now.getUTCMonth() + 1).padStart(2, "0"),
    String(now.getUTCDate()).padStart(2, "0"),
    String(now.getUTCHours()).padStart(2, "0"),
  ].join("/");
  const key = `raw/${prefix}/${crypto.randomUUID()}.ndjson`;

  await s3.send(
    new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: key,
      Body: events.map((event) => JSON.stringify(event.raw)).join("\n") + "\n",
      ContentType: "application/x-ndjson",
    }),
  );
  console.log(`archived ${events.length} events -> s3://${S3_BUCKET}/${key}`);
}

async function publishToEventBridge(events: JetstreamCommit[]) {
  // PutEvents accepts at most 10 entries per call, so chunk the batch. A
  // busy poll (e.g. catching up after a burst) can mean dozens of chunks -
  // sending them one at a time sequentially risks running past the Lambda
  // timeout before the cursor gets saved, which would silently reprocess
  // this whole range on the next poll. Sending chunks concurrently keeps a
  // large catch-up batch well within the timeout.
  const chunks: JetstreamCommit[][] = [];
  for (let i = 0; i < events.length; i += 10) chunks.push(events.slice(i, i + 10));

  await Promise.all(
    chunks.map((chunk) => {
      const entries = chunk
        .filter((commit) => DETAIL_TYPE_BY_COLLECTION[commit.collection])
        .map((commit) => ({
          EventBusName: EVENT_BUS_NAME,
          Source: "atproto.ingest",
          DetailType: DETAIL_TYPE_BY_COLLECTION[commit.collection],
          Detail: JSON.stringify({
            did: commit.did,
            cursor: commit.seq,
            time: commit.time,
            collection: commit.collection,
            operation: commit.operation,
            record: commit.record,
          }),
        }));

      return entries.length > 0
        ? eventBridge.send(new PutEventsCommand({ Entries: entries }))
        : Promise.resolve();
    }),
  );
}

// A second, independent timeout guard around the whole handler - not just
// drainFirehose's own internal one - so a poll against a real network
// endpoint that hangs in some way neither `error` nor `close` catches
// still ends before the Lambda's own timeout kills it with no logs at
// all. Comfortably under the function's configured 60s timeout.
const HANDLER_DEADLINE_MS = Number(process.env.HANDLER_DEADLINE_MS ?? 45_000);

export async function handler() {
  const timedOut = new Promise<"deadline">((resolve) =>
    setTimeout(() => resolve("deadline"), HANDLER_DEADLINE_MS),
  );

  const result = await Promise.race([run(), timedOut]);
  if (result === "deadline") {
    console.log(
      `handler deadline (${HANDLER_DEADLINE_MS}ms) hit before the poll finished on its own`,
    );
  }
}

async function run() {
  const cursor = await getCursor();
  const events = await drainFirehose(cursor);

  if (events.length === 0) {
    console.log(`no new events since cursor ${cursor}`);
    return;
  }

  await archiveToS3(events);
  await publishToEventBridge(events);

  const newCursor = events[events.length - 1].seq;
  await saveCursor(newCursor);
  console.log(`processed ${events.length} events, cursor advanced to ${newCursor}`);
}
