// Stand-in for Bluesky's real-time event stream ("firehose").
//
// ATProto background (minimal):
// - AT Protocol (ATProto) is the open protocol behind Bluesky. Every user has
//   a "repo" identified by a DID (decentralized ID, e.g. did:plc:...).
// - When someone posts, likes, or follows, their repo emits a "commit" —
//   a JSON object describing what changed (collection, operation, record).
// - Jetstream (https://github.com/bluesky-social/jetstream) is a Bluesky
//   service that forwards those commits over a WebSocket as plain JSON.
//
// This mock replays canned commits in Jetstream shape so the rest of the demo
// can run offline. Swap the ingest consumer's FIREHOSE_URL to
// wss://jetstream2.us-east.bsky.network/subscribe for the real stream.
import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { WebSocketServer } from "ws";

const PORT = Number(process.env.PORT ?? 8080);

// One JSON object per line (NDJSON). Each line is a single commit event.
const events = readFileSync(
  new URL("../data/sample-events.ndjson", import.meta.url),
  "utf8",
)
  .trim()
  .split("\n")
  .map((line) => JSON.parse(line));

// Spike mode: temporarily send events much faster, simulating a traffic burst
// (e.g. a post going viral). Trigger with POST /spike during the live demo.
let spikeUntil = 0;
const NORMAL_DELAY_MS: [number, number] = [400, 1200];
const SPIKE_DELAY_MS: [number, number] = [20, 60];
const SPIKE_DURATION_MS = 15_000;

function nextDelayMs(): number {
  const [min, max] = Date.now() < spikeUntil ? SPIKE_DELAY_MS : NORMAL_DELAY_MS;
  return min + Math.random() * (max - min);
}

// Real Jetstream supports resuming a subscription with ?cursor=<time_us>,
// replaying anything missed since that point. Our ingest-consumer never
// disconnects so it never needs this, but ingest-poller (the interval-based
// alternative) connects briefly, disconnects, and reconnects later with the
// cursor of the last event it saw — so we keep a short rolling history to
// replay on reconnect, the same way the real service does. Sized generously
// (spike mode alone can emit ~300-400 events) so a poller that's slower to
// reconnect than its poll interval still doesn't silently miss events once
// they age out of the buffer.
const HISTORY_SIZE = 500;
const history: any[] = [];

const server = createServer((req, res) => {
  if (req.method === "POST" && req.url === "/spike") {
    spikeUntil = Date.now() + SPIKE_DURATION_MS;
    console.log(`spike mode engaged for ${SPIKE_DURATION_MS / 1000}s`);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, spikeForMs: SPIKE_DURATION_MS }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  console.log(`client connected (${wss.clients.size} total)`);
  ws.on("close", () => console.log(`client disconnected (${wss.clients.size} total)`));

  // ?cursor=<time_us>: replay anything broadcast after that point before this
  // client starts receiving new live events, so a poller that was offline
  // between connections catches up instead of silently missing events.
  const cursor = Number(new URL(req.url ?? "", "http://x").searchParams.get("cursor"));
  if (cursor > 0) {
    const missed = history.filter((event) => event.time_us >= cursor);
    for (const event of missed) ws.send(JSON.stringify(event));
    console.log(`replayed ${missed.length} events since cursor ${cursor}`);
  }
});

// Broadcast the same stream to every WebSocket client. Re-stamp time_us on
// each replay so looped sample data always looks "live".
async function playForever() {
  let i = 0;
  while (true) {
    const event = { ...events[i % events.length], time_us: Date.now() * 1000 };

    history.push(event);
    if (history.length > HISTORY_SIZE) history.shift();

    const payload = JSON.stringify(event);
    for (const client of wss.clients) {
      if (client.readyState === client.OPEN) client.send(payload);
    }
    i++;
    await new Promise((resolve) => setTimeout(resolve, nextDelayMs()));
  }
}

server.listen(PORT, () => {
  console.log(`firehose-mock listening on ws://localhost:${PORT}`);
  console.log(`trigger a spike: curl -X POST http://localhost:${PORT}/spike`);
  playForever();
});
