// Analytics consumer: counts every event type, bucketed by its source event hour.
// The EventBridge rule (infra/lib/atproto-stack.ts) forwards ALL events
// from the ingest consumer — no content filtering here.
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import type { AtprotoEvent } from "../common/types";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const TABLE_NAME = process.env.TABLE_NAME ?? "AnalyticsCounts";

export async function handler(event: AtprotoEvent) {
  const hour = new Date(event.detail.time).toISOString().slice(0, 13); // e.g. "2026-07-28T12"
  const id = `${event["detail-type"]}#${hour}`; // one counter row per type per hour

  await ddb.send(
    new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { id },
      UpdateExpression: "ADD #count :one",
      ExpressionAttributeNames: { "#count": "count" },
      ExpressionAttributeValues: { ":one": 1 },
    }),
  );
}
