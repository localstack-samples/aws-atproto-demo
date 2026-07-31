// Notifications consumer: alerts when someone follows a watchlisted account.
//
// EventBridge already filtered for follow.created events where record.subject
// (the account being followed) is on the watchlist — see the rule in
// infra/lib/atproto-stack.ts. We enqueue a message to SQS as a stand-in for
// sending a push notification, email, or webhook.
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import type { AtprotoEvent } from "../common/types";

const sqs = new SQSClient({});
const QUEUE_URL = process.env.QUEUE_URL!;

export async function handler(event: AtprotoEvent) {
  const { did, record } = event.detail;

  await sqs.send(
    new SendMessageCommand({
      QueueUrl: QUEUE_URL,
      MessageBody: JSON.stringify({
        watchedDid: record.subject, // account that gained a new follower
        followerDid: did, // account that clicked Follow
        followedAt: record.createdAt,
      }),
    }),
  );
}
