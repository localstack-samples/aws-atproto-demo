#!/usr/bin/env bash
# End-to-end check for either ingestion stack. Usage: ./test.sh poller|fargate
#
# Deploy the stack first. This script starts its own mock firehose on port
# 8080, lets events flow through the pipeline, stops the stream, and then
# compares every downstream result with the S3 raw archive:
# - AnalyticsCounts holds one count per archived event, per type and hour
# - FlaggedContent holds exactly the archived comments that contain a
#   moderation keyword, keyed by their Jetstream sequence number
# - notifications-queue holds exactly the archived subscriptions to a
#   watchlisted publication
# - the downstream failure queues are empty
# The poller run also checks the cursor: a second poll resumes from where the
# first one stopped and replays the events published in between.
set -euo pipefail

STACK=${1:-}
if [ "$STACK" != "poller" ] && [ "$STACK" != "fargate" ]; then
  echo "usage: $0 poller|fargate" >&2
  exit 2
fi

ROOT=$(cd "$(dirname "$0")" && pwd)
PORT=8080
MAX_WAIT=${MAX_WAIT:-300}
BUCKET=raw-firehose-archive
# Keep in sync with MODERATION_KEYWORDS and NOTIFICATION_WATCHLIST_PUBLICATIONS
# in infra-*/lib/*-stack.ts
KEYWORDS='["spam", "scam", "free money", "click here"]'
WATCHLIST='["at://did:plc:writer4h2mvz7ndp/pub.leaflet.publication/protocoldigest",
  "at://did:plc:writer8jvbnq5trm/pub.leaflet.publication/weeklyroundup"]'

WORK=$(mktemp -d)
MOCK_PID=""

stop_mock() {
  if [ -n "$MOCK_PID" ] && kill -0 "$MOCK_PID" 2> /dev/null; then
    kill "$MOCK_PID"
    wait "$MOCK_PID" 2> /dev/null || true
  fi
  MOCK_PID=""
}
cleanup() {
  stop_mock
  rm -rf "$WORK"
}
trap cleanup EXIT

# Writes to stderr, so a failure inside $(...) is still shown
fail() {
  {
    echo "Test failed: $*"
    if [ -s "$WORK/mock.log" ]; then
      echo "--- mock firehose log (last 20 lines)"
      tail -20 "$WORK/mock.log"
    fi
  } >&2
  exit 1
}

# wait_for <description> <command...>: retry until the command succeeds
wait_for() {
  local description=$1 deadline=$((SECONDS + MAX_WAIT))
  shift
  until "$@"; do
    [ $SECONDS -ge $deadline ] && fail "timed out after ${MAX_WAIT}s waiting for $description"
    sleep 5
  done
}

table_count() {
  lstk aws dynamodb scan --table-name "$1" --select COUNT --query Count --output text
}

queue_depth() {
  local url
  url=$(lstk aws sqs get-queue-url --queue-name "$1" --query QueueUrl --output text)
  lstk aws sqs get-queue-attributes --queue-url "$url" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --query 'Attributes.[ApproximateNumberOfMessages, ApproximateNumberOfMessagesNotVisible]' \
    --output text | awk '{print $1 + $2}'
}

cursor() {
  lstk aws dynamodb get-item --table-name IngestCursor --key '{"id": {"S": "'"$1"'"}}' \
    --query Item.cursor.N --output text
}

# --- Mock firehose -----------------------------------------------------------

if curl -s -o /dev/null --max-time 2 "http://localhost:$PORT/"; then
  fail "port $PORT is in use. Stop the mock firehose first, this script starts its own"
fi
(cd "$ROOT/firehose-mock" && exec node --import tsx src/server.ts) > "$WORK/mock.log" 2>&1 &
MOCK_PID=$!
wait_for "the mock firehose" grep -q "listening" "$WORK/mock.log"
echo "Mock firehose running (pid $MOCK_PID)"

# --- Ingest ------------------------------------------------------------------

if [ "$STACK" = "poller" ]; then
  # poll <n>: invoke the poller synchronously and print how many events it processed
  poll() {
    local result
    lstk aws lambda invoke --function-name atproto-ingest-poller --log-type Tail \
      --cli-read-timeout 120 "$WORK/poll$1.json" > "$WORK/poll$1.meta"
    if [ "$(jq -r '.FunctionError // empty' "$WORK/poll$1.meta")" != "" ]; then
      cat "$WORK/poll$1.json" >&2
      jq -r .LogResult "$WORK/poll$1.meta" | base64 -d >&2
      fail "poll $1 returned a function error"
    fi
    result=$(jq -r .LogResult "$WORK/poll$1.meta" | base64 -d | grep -o 'processed [0-9]* events, cursor advanced to [0-9]*' || true)
    [ -n "$result" ] || fail "poll $1 did not process any events: $(jq -r .LogResult "$WORK/poll$1.meta" | base64 -d)"
    echo "$result" | awk '{print $2}'
  }

  first=$(poll 1)
  first_cursor=$(cursor poller)
  echo "PASS: first poll processed $first events, cursor $first_cursor"

  # Leave the poller offline while the mock keeps publishing. The next poll
  # reconnects with ?cursor= and must replay what it missed.
  sleep 30
  second=$(poll 2)
  second_cursor=$(cursor poller)
  [ "$second_cursor" -gt "$first_cursor" ] || fail "cursor did not advance: $first_cursor -> $second_cursor"
  [ "$second" -gt "$first" ] || fail "second poll processed $second events, expected the 30s it missed on top of its own window ($first in the first poll)"
  echo "PASS: second poll resumed from cursor $first_cursor and caught up $second events, cursor $second_cursor"
else
  has_archive() {
    [ "$(lstk aws s3api list-objects-v2 --bucket "$BUCKET" --prefix raw/ --query 'length(Contents || `[]`)' --output text)" -gt 0 ]
  }
  wait_for "the Fargate consumer to archive its first batch" has_archive
  echo "PASS: the Fargate consumer is archiving events"
  # Run long enough for the mock to loop through all 35 sample events
  sleep 45
  cursor=$(cursor fargate)
  [ "$cursor" != "None" ] && [ "$cursor" -gt 0 ] || fail "the consumer did not save a cursor"
  echo "PASS: the consumer saved cursor $cursor"
fi

# Stop the stream so the archive stops growing. The Fargate consumer flushes
# its buffer every 10 seconds, so give it time to archive what it published.
stop_mock
if [ "$STACK" = "fargate" ]; then
  sleep 20
fi

# --- Expected results, derived from the raw archive --------------------------

lstk aws s3 cp "s3://$BUCKET/raw/" "$WORK/archive" --recursive --quiet
find "$WORK/archive" -name '*.ndjson' -exec cat {} + | jq -c .payload > "$WORK/events.ndjson"
total=$(wc -l < "$WORK/events.ndjson" | tr -d ' ')
[ "$total" -gt 0 ] || fail "the archive is empty"
echo "PASS: the archive holds $total events"

jq -s 'map({"pub.leaflet.document": "document.created", "pub.leaflet.comment": "comment.created",
           "pub.leaflet.graph.subscription": "subscription.created"}[.collection] + "#" + .time[0:13])
       | group_by(.) | map({key: .[0], value: length}) | from_entries' \
  "$WORK/events.ndjson" > "$WORK/expected-analytics.json"

jq -s --argjson keywords "$KEYWORDS" \
  'map(select(.collection == "pub.leaflet.comment")
       | select(.record.plaintext as $text | any($keywords[]; . as $k | $text | contains($k)))
       | .seq | tostring) | unique' \
  "$WORK/events.ndjson" > "$WORK/expected-flags.json"

jq -s --argjson watchlist "$WATCHLIST" \
  'map(select(.collection == "pub.leaflet.graph.subscription")
       | select(.record.publication as $p | $watchlist | index($p))
       | {subscriberDid: .did, subscribedAt: .time}) | unique' \
  "$WORK/events.ndjson" > "$WORK/expected-notifications.json"

[ "$(jq length "$WORK/expected-flags.json")" -gt 0 ] || fail "the archive has no keyword comments, the run was too short"
[ "$(jq length "$WORK/expected-notifications.json")" -gt 0 ] || fail "the archive has no watchlisted subscriptions, the run was too short"

# --- Analytics ---------------------------------------------------------------

analytics_match() {
  lstk aws dynamodb scan --table-name AnalyticsCounts --output json \
    | jq '.Items | map({key: .id.S, value: (.count.N | tonumber)}) | from_entries' > "$WORK/analytics.json"
  jq -e --slurpfile expected "$WORK/expected-analytics.json" '. == $expected[0]' "$WORK/analytics.json" > /dev/null
}
if ! (MAX_WAIT=120 wait_for "analytics counts" analytics_match); then
  echo "expected: $(cat "$WORK/expected-analytics.json")"
  echo "actual:   $(cat "$WORK/analytics.json")"
  exit 1
fi
echo "PASS: AnalyticsCounts matches the archive: $(jq -c . "$WORK/analytics.json")"

# --- Moderation --------------------------------------------------------------

flags_match() {
  lstk aws dynamodb scan --table-name FlaggedContent --output json \
    | jq '.Items | map(.id.S) | unique' > "$WORK/flags.json"
  jq -e --slurpfile expected "$WORK/expected-flags.json" '. == $expected[0]' "$WORK/flags.json" > /dev/null
}
if ! (MAX_WAIT=120 wait_for "moderation flags" flags_match); then
  echo "expected sequence numbers: $(jq -c . "$WORK/expected-flags.json")"
  echo "flagged sequence numbers:  $(jq -c . "$WORK/flags.json")"
  exit 1
fi
echo "PASS: FlaggedContent holds exactly the $(jq length "$WORK/flags.json") keyword comments in the archive"

# --- Notifications -----------------------------------------------------------

QUEUE_URL=$(lstk aws sqs get-queue-url --queue-name notifications-queue --query QueueUrl --output text)
: > "$WORK/messages.ndjson"
notifications_match() {
  # Hide received messages for 10 minutes so each one is read once
  lstk aws sqs receive-message --queue-url "$QUEUE_URL" --max-number-of-messages 10 \
    --visibility-timeout 600 --wait-time-seconds 2 --output json \
    | jq -c '.Messages // [] | .[].Body | fromjson' >> "$WORK/messages.ndjson"
  jq -s 'map({subscriberDid, subscribedAt}) | unique' "$WORK/messages.ndjson" > "$WORK/notifications.json"
  jq -e --slurpfile expected "$WORK/expected-notifications.json" '. == $expected[0]' "$WORK/notifications.json" > /dev/null
}
if ! (MAX_WAIT=120 wait_for "notifications" notifications_match); then
  echo "expected: $(jq -c . "$WORK/expected-notifications.json")"
  echo "received: $(jq -c . "$WORK/notifications.json")"
  exit 1
fi
if ! jq -e -s --argjson watchlist "$WATCHLIST" 'all(.watchedPublication as $p | $watchlist | index($p))' \
  "$WORK/messages.ndjson" > /dev/null; then
  fail "a notification names a publication outside the watchlist"
fi
echo "PASS: notifications-queue holds exactly the $(jq length "$WORK/notifications.json") watchlisted subscriptions in the archive"

# --- Failure queues ----------------------------------------------------------

for queue in atproto-target-delivery-failures atproto-analytics-processing-failures \
  atproto-moderation-processing-failures atproto-notifications-processing-failures; do
  depth=$(queue_depth "$queue")
  [ "$depth" = "0" ] || fail "$queue holds $depth failed deliveries"
done
echo "PASS: no failed deliveries"

echo "Test passed: the $STACK stack archived, fanned out and processed every event"
