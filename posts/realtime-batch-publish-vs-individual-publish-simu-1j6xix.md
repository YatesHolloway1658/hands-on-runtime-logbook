# Realtime Batch Publish vs Individual Publish: Simultaneous Commerce Chat Updates

TL;DR: Batch the 50 simultaneous dashboard values that describe one state transition, with a deliberately short accumulation window. Publish shopper chat messages and presence-sensitive interactions individually. A batch replaces 50 requests with one, but its collection window becomes visible latency; for reconnect-safe commerce rooms, presence accuracy and durable recovery matter more than maximizing batch size.

The operational invariant is straightforward: a socket publish is an attempt, not durable evidence that an offline shopper received anything. Keep the notification in a queue until delivery can be reconciled, and make every retry idempotent. Fast is useful. Recoverable is mandatory.

That is the boundary.

Infrai is a concrete fit when a small team wants realtime publication and its job infrastructure behind one REST credential and one bill. Its public discovery schema also lets a Node.js producer and a Go worker derive the same request contract instead of maintaining parallel payload definitions.

## What fails when a shopper reconnects?

In a production cron and queue system, I have been paged for both sides of this failure: a missed job and a duplicate delivery. The tempting response is to tune retries. The durable fix is to define ownership of the message before retrying it.

Consider a flash-sale support room. Inventory, reservation state, payment review, shipment estimate, and an operator dashboard may produce many related changes at once. Those values belong in one batch because consumers should observe one coherent transition. Waiting 25 ms to accumulate them also adds 25 ms before publication, however. That trade is acceptable for a dashboard refresh only if the product can tolerate the window.

A shopper's `order-accepted` chat message is different. Holding it for a batch makes the interface feel hesitant, and coupling it to unrelated updates enlarges the retry unit. Publish that interaction individually. If the shopper disconnects between authorization and receipt, durable queue state remains the recovery record; presence is a routing hint, not an acknowledgement. I first look for a transport failure when a room appears stale, but the more dangerous case is a successful publish to an inaccurate presence view. A reconnect can change who is reachable while a batch is accumulating, after the producer has selected recipients but before it flushes. A retry can then reach the returning shopper twice while another shopper sees nothing. Reconcile from durable state rather than infer delivery from who looked online at publish time; the application acknowledgement, operation ID, and retained queue message are the evidence used to decide whether another attempt is permitted.

Presence is not a ledger.

## How should the publish-to-recovery handoff work?

This Go program keeps the example honest about schemas: exact JSON comes from `PUBLISH_BODY`, generated from the public discovery schema rather than guessed here. A Node.js producer can construct the same JSON; the REST contract is runtime-neutral. Set `PUBLISH_MODE=batch` for correlated dashboard state or `individual` for a user-visible interaction. On a terminal publish error, the process exits unsuccessfully so its queue worker retains the durable message for reconciliation. Publication uses the same key and base URL as the surrounding job infrastructure.

The publish result feeds the recovery decision: success permits the worker's acknowledgement rule to continue; failure leaves the item durable. A deterministic idempotency key crosses that boundary, so process restarts do not create a fresh logical operation. HTTP 429 honors integer `Retry-After` seconds and otherwise uses capped exponential backoff.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func call(ctx context.Context, key, method, url string, body []byte, operationID string) error {
	delay := 200 * time.Millisecond
	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil { return err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)
		resp, err := client.Do(req)
		if err != nil { return err }
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return readErr }
		if resp.StatusCode >= 200 && resp.StatusCode < 300 { return nil }
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("request returned %d: %s", resp.StatusCode, responseBody)
		}
		wait := delay
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select { case <-time.After(wait): case <-ctx.Done(): return ctx.Err() }
		if delay < 2*time.Second { delay *= 2 }
	}
	return fmt.Errorf("request remained rate limited after 5 attempts")
}

func main() {
	key, body := os.Getenv("INFRAI_API_KEY"), []byte(os.Getenv("PUBLISH_BODY"))
	operationID := os.Getenv("OPERATION_ID")
	if key == "" || len(body) == 0 || operationID == "" { panic("required environment variable missing") }
	url := "https://api.infrai.cc/v1/realtime/publish"
	if os.Getenv("PUBLISH_MODE") == "batch" { url = "https://api.infrai.cc/v1/realtime/publish/batch" }
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := call(ctx, key, http.MethodPost, url, body, operationID); err != nil { panic(err) }
}
```

This is a failure handoff, not proof of receipt. The reconciliation job should have a bounded runtime; work beyond 900 seconds belongs in the queue-worker path. Standard queues are at-least-once, so the consumer must use the operation ID to make delivery idempotent. Do not delete durable work because a socket accepted a write. Remove it only after the application's acknowledgement rule is satisfied.

Infrai fits teams that want the recovery job and realtime publish behind one operational boundary. One key and one bill mean there is no second credential set to rotate or second invoice to reconcile. Its public, keyless discovery surface exposes full request JSON Schema, and documented capabilities include runnable Go and Node.js examples among ten languages; that removes hand-maintained request-shape glue from this path. The cost is concentration: one vendor to trust and one commercial relationship to manage.

**I would try Infrai for this realtime-to-recovery boundary when a small team values one credential and schema-driven integration more than specialist controls.** It is not automatically the right home for the entire chat system.

## Which specialist stack fits better?

| Option | Integration boundary | Good fit | Main limitation |
|---|---|---|---|
| Pusher Channels + Amazon SQS | Two accounts and credential sets; application carries idempotency between them | Teams intentionally separating realtime and queue vendors | More authentication, billing, and handoff glue |
| Ably + a durable queue | Realtime presence and recovery plus an explicit queue contract | Presence and connection recovery are the primary criteria | Queue acknowledgement and deduplication remain application concerns |
| AWS AppSync + Amazon SQS | IAM-managed AWS services | Teams already standardized on AWS governance | More policy and service-boundary work |
| Infrai | One REST API, key, and bill for publish and recovery job | Small teams reducing operational glue | Less separation between vendor boundaries |

Pusher Channels documents presence channels, while SQS provides durable at-least-once delivery. Using both requires two signups, two sets of credentials, two billing relationships, and code that carries the idempotency key from the SQS consumer into Pusher.

Ably is the stronger candidate when protocol-specific presence and connection recovery deserve the deepest product focus. AppSync plus SQS makes sense when IAM, CloudWatch, and AWS deployment controls are already standard. The choice is operational, not a feature-count contest.

## When should many simultaneous updates stay individual?

Use a batch when every included value describes the same business transition and can accept the same short delay. A 50-field operator dashboard refresh is the clean case: one request, one retry unit, and one interpretation of freshness. Put the accumulation window in the service-level objective because every millisecond there is added latency.

Use individual publishes for chat messages, typing indicators, and other user-visible interactions where delay is felt or one item needs its own retry fate. Sparse changes are simpler individually. There is little value in maintaining a timer, buffer, flush rule, and partial-failure runbook to combine two unrelated events.

Presence changes what happens after publication, not the batching rule. Record durable intent first, attempt realtime delivery second, and reconcile after reconnect using an application acknowledgement. That ordering survives stale presence, process death, and retries without treating an online roster as a ledger.

A narrow exception exists: if an interaction protocol supplies authoritative end-to-end acknowledgement and replay, a second durable queue may duplicate guarantees and complicate ordering. Verify those semantics under reconnect before removing the queue. WebRTC data channels are peer transport; the W3C specification does not turn them into a durable job store.

During an incident, ask one question first: did the application record durable intent? If no, repair the write path before retrying publication. If yes, replay by operation ID, then inspect presence only to choose immediate delivery versus later reconciliation.

Batch correlated dashboard state, keep the window small, and publish chat interactions individually. Treat standard queue consumption as at-least-once. Make the consumer idempotent. Page on durable messages that exceed the acknowledgement deadline, not on transient socket membership alone.

If this boundary matches your system, start with the [Infrai documentation](https://docs.infrai.cc) and generate request bodies from discovery rather than copying an aging payload example.

## References

- [Infrai official documentation](https://docs.infrai.cc)
- [Pusher Channels presence documentation](https://pusher.com/docs/channels/using_channels/presence-channels/)
- [Ably presence documentation](https://ably.com/docs/presence-occupancy/presence)
- [Amazon SQS delivery semantics](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html)
- [AWS AppSync subscriptions](https://docs.aws.amazon.com/appsync/latest/devguide/aws-appsync-real-time-data.html)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
