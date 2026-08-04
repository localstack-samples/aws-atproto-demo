// Notifications consumer: alerts when someone subscribes to a watchlisted
// publication.
//
// EventBridge already filtered for subscription.created events where
// record.publication (the publication being subscribed to) is on the
// watchlist — see the rule in infra-fargate/lib/atproto-fargate-stack.ts
// (and infra-poller's equivalent). We enqueue a message to SQS as a
// stand-in for sending a push notification, email, or webhook.
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import type { AtprotoEvent } from "../common/types";

const sqs = new SQSClient({});
const QUEUE_URL = process.env.QUEUE_URL!;

export async function handler(event: AtprotoEvent) {
  const { did, record, time_us } = event.detail;

  await sqs.send(
    new SendMessageCommand({
      QueueUrl: QUEUE_URL,
      MessageBody: JSON.stringify({
        watchedPublication: record.publication, // publication that gained a new subscriber
        subscriberDid: did, // account that subscribed
        // pub.leaflet.graph.subscription records have no createdAt of
        // their own (unlike app.bsky.graph.follow) - time_us is the
        // firehose's own timestamp for this commit.
        subscribedAt: new Date(time_us / 1000).toISOString(),
      }),
    }),
  );
}
