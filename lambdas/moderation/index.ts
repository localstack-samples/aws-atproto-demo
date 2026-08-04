// Moderation consumer: records comments that already matched keyword filters.
//
// Important: EventBridge did the filtering BEFORE this Lambda ran — the rule
// in infra-fargate/lib/atproto-fargate-stack.ts (and infra-poller's
// equivalent) only invokes us for comment.created events whose
// record.plaintext contains a keyword (spam, scam, etc.). We just persist
// the hit; a real system would queue it for human review.
//
// Only pub.leaflet.comment records have plaintext - pub.leaflet.document
// content lives in nested block pages, not a top-level string, so this
// Lambda never fires on documents. See the README for why.
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand } from "@aws-sdk/lib-dynamodb";
import type { AtprotoEvent } from "../common/types";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE_NAME = process.env.TABLE_NAME ?? "FlaggedContent";

export async function handler(event: AtprotoEvent) {
  const { did, record } = event.detail;

  await ddb.send(
    new PutCommand({
      TableName: TABLE_NAME,
      Item: {
        id: crypto.randomUUID(),
        did,
        plaintext: record.plaintext,
        flaggedAt: new Date().toISOString(),
      },
    }),
  );
}
