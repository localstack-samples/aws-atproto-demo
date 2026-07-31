// Moderation consumer: records posts that already matched keyword filters.
//
// Important: EventBridge did the filtering BEFORE this Lambda ran — the rule
// in infra/lib/atproto-stack.ts only invokes us for post.created events whose
// record.text contains a keyword (spam, scam, etc.). We just persist the hit;
// a real system would queue it for human review.
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
        text: record.text,
        flaggedAt: new Date().toISOString(),
      },
    }),
  );
}
