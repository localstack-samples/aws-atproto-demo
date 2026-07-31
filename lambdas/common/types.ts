// Shape of the payload inside every EventBridge event on the atproto-events
// bus. The ingest consumer builds this; each downstream Lambda reads
// `event.detail`.
export interface AtprotoEventDetail {
  did: string; // repo owner — the user whose account emitted this commit
  time_us: number; // microsecond timestamp from the firehose
  collection: string; // record type, e.g. "app.bsky.feed.post" (a Bluesky post)
  operation: string; // "create" | "delete" | "update"
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  record: any; // the actual post/like/follow payload; shape depends on collection
}

// Wrapper EventBridge adds around every published event (what Lambdas receive).
export interface AtprotoEvent {
  source: string; // always "atproto.ingest" in this demo
  "detail-type": string; // e.g. "post.created", "follow.created"
  detail: AtprotoEventDetail;
}
