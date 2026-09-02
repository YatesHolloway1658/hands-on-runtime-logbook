# Active-Customer Digests: Public HTTPS Queue Webhook Subscriber Recovery in Node.js

Short answer: for a weekly digest to active customers, accept a queue push only after authenticating the raw request and durably recording an idempotent work item; acknowledge at that boundary, then let a worker build and send the digest. Recovery depends on that handoff being inspectable, not on keeping the public endpoint open until email delivery finishes.

This is a scheduling problem with a delivery-shaped edge. The schedule creates delayed work, the queue push wakes an HTTPS subscriber, and the subscriber must survive retries without sending the same customer a second digest. The endpoint is an intake point. It is not the digest worker.

## A weekly digest has two identities

The failure mode I would page on first is not “the endpoint returned 500.” It is an ambiguous acknowledgement. A receiver can validate a request, call a mail provider, wait, and then lose the connection just before replying. The queue retries. Now the service cannot tell whether the first attempt sent the digest, so a retry may create a duplicate customer email.

That boundary gets worse when the weekly batch is large. One delivery may contain a payload reference for many active customers, while the work itself needs database reads, template rendering, rate limiting, and an external send. None of those operations belong in the request that proves ownership of the queue message. Keep the request short.

Do not acknowledge early.

Here is the failure sequence I keep in the runbook. At 09:00, the weekly scheduler creates the active-customer batch. At 09:01, the public subscriber verifies delivery `digest-32` and writes its claim. At 09:02, the worker renders customer `184`, submits the message, and loses its connection before it records the provider response. A retry of the queue push is now harmless only if the delivery claim prevents a second batch, while the customer-send key prevents a second message. If the worker has no durable send log, operators have to guess from provider evidence and customer reports; if the receiver waits for all sends before acknowledging, the queue's retry behavior becomes part of the email correctness story. That is too much responsibility for one HTTP request. I've seen enough paging caused by this kind of ambiguity to make the claim and send records first-class runbook objects.

At-least-once delivery is the useful default to design for. A repeated delivery ID should be boring: the receiver recognizes an existing claim, acknowledges it, and leaves the original work record for the worker. Exactly-once business effects require idempotency at the application and downstream boundaries; an acknowledgement alone is not an audit trail.

There is a second, quieter failure. A timer can fire while the prior weekly run is still processing. If the schedule creates two logically identical batches, a request-level deduplication key will not help unless the application also defines the digest period and customer set as part of its business identity. Name the work explicitly, for example `support-digest:2026-W32`, and decide whether a rerun replaces, resumes, or supplements it.

## Rehearse the bad week before production

Verify in a fixed order: method and body size, transport and sender authentication, delivery identity, then durable claim. Signature verification must use the raw bytes that were signed, before JSON parsing or normalization. The exact header names and signing algorithm belong to the queue contract; do not invent a “standard” header when integrating a real sender.

The following Go handler demonstrates the receiver-owned boundary. Its HMAC envelope is illustrative, not a claim about any particular queue. The `Claim` implementation must use a durable transaction with a unique constraint on the delivery ID. Returning success for an already-claimed ID is intentional: retries are part of normal operation.

```go
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"time"
)

type WorkStore interface {
	Claim(deliveryID string, payload []byte) (alreadyClaimed bool, err error)
}

func matchesSignature(secret, timestamp, supplied string, body []byte) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "."))
	_, _ = mac.Write(body)
	want, err := hex.DecodeString(supplied)
	return err == nil && hmac.Equal(want, mac.Sum(nil))
}

func receiver(store WorkStore, secret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		deliveryID := r.Header.Get("X-Delivery-ID")
		timestamp := r.Header.Get("X-Sent-At")
		sentUnix, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil || deliveryID == "" || time.Since(time.Unix(sentUnix, 0)) > 5*time.Minute {
			http.Error(w, "invalid delivery metadata", http.StatusUnauthorized)
			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 256<<10))
		if err != nil || !matchesSignature(secret, timestamp, r.Header.Get("X-Signature"), body) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		if _, err := store.Claim(deliveryID, body); err != nil {
			http.Error(w, "claim failed", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
```

An HTTPS endpoint that the sender cannot reach is not a subscriber. Check public DNS, certificate validity, proxy routing, request timeouts, and the service's body limit. Authentication still belongs in the application because a TLS terminator proves the connection is encrypted; it does not establish that this delivery is authorized.

## What should a public HTTPS subscriber verify before accepting work?

Commit the claim before acknowledging. The record should include the delivery ID, digest period, customer-set reference, payload or payload reference, state, attempt count, and timestamps. The worker can then move the record through states such as `claimed`, `rendering`, `sending`, and `complete`, with an explicit retry policy. Keep the original payload or a durable pointer long enough to investigate a missed digest.

The worker needs a second idempotency boundary. If it sends one customer at a time, use a stable key made from the digest period and customer ID, such as `support-digest:2026-W32:customer-184`. If the email provider supports idempotency keys, pass that key through. If it does not, record the send decision transactionally and make the reconciliation job compare provider results before retrying. The hard case is an unknown outcome after a timeout; “try again” is not a recovery policy by itself.

Here is the shape of a recovery loop, with the external send abstracted behind an interface so the queue contract does not dictate the mail system:

```go
package digest

import "context"

type Job struct {
	DeliveryID string
	Period     string
	CustomerID string
}

type DeliveryLog interface {
	BeginSend(ctx context.Context, key string) (send bool, err error)
	MarkSent(ctx context.Context, key string) error
}

type Mailer interface {
	Send(ctx context.Context, customerID, idempotencyKey string) error
}

func sendCustomer(ctx context.Context, log DeliveryLog, mail Mailer, job Job) error {
	key := "support-digest:" + job.Period + ":" + job.CustomerID
	send, err := log.BeginSend(ctx, key)
	if err != nil || !send {
		return err
	}
	if err := mail.Send(ctx, job.CustomerID, key); err != nil {
		return err
	}
	return log.MarkSent(ctx, key)
}
```

This does not manufacture exactly-once delivery from an external system. It makes the uncertainty visible and gives the runbook a durable place to reconcile it. A duplicate queue push and a duplicate customer-send attempt are separate events; measure both.

## Keep recovery state visible to the on-call

Test the public hostname before enabling the weekly schedule. Use a valid signed probe, an invalid signature, a stale timestamp, a repeated delivery ID, and a payload at the configured limit. Verify the status code and the durable record, not just an access log. Then run a test digest for one known customer and confirm that a worker restart does not create a second send.

The useful dashboard separates the stages: rejected authentication, accepted claims, duplicate claims, worker backlog, oldest claimed item, per-customer send attempts, and unresolved external outcomes. A backlog alert tells you that work is aging; it does not tell you whether the receiver is unreachable or the sender is slow. The distinction determines who gets paged.

| Signal | First response |
|---|---|
| Authentication rejections rise | Check the public route, clock skew, and credential configuration before replaying work. |
| Duplicate claims rise | Inspect delivery retries and claim transactions; do not delete the existing work record. |
| Oldest claimed item rises | Check worker capacity and downstream latency, then pause new intake if accepted work is at risk. |
| Send outcomes are unknown | Reconcile the provider result using the customer-period key before retrying. |

Rollback should stop new intake while preserving accepted work. Disable the schedule or redirect the subscription according to the owning system's documented control, leave claimed records available for workers, and keep the previous signing credential long enough to explain any in-flight retry. Re-enable only after the probe passes and the reconciliation queue is understood.

The catch is that this pattern is not suitable when the job requires a durable multi-step workflow with joins, compensation, or human approval. Use a workflow-oriented design there. It is also a poor fit when the team cannot operate a public HTTPS ingress, durable claim store, and worker backlog. A simpler scheduled pull may be the better choice. Your mileage may vary on the exact retry window because it depends on the queue contract and the downstream provider, but the acknowledgement rule does not: acknowledge ownership, not hoped-for completion.

## When should this handoff be rejected?

The catch is that this pattern is not suitable when the job requires a durable multi-step workflow with joins, compensation, or human approval. Use a workflow-oriented design there. It is also a poor fit when the team cannot operate a public HTTPS ingress, durable claim store, and worker backlog. A simpler scheduled pull may be the better choice. Your mileage may vary on the exact retry window because it depends on the queue contract and the downstream provider, but the acknowledgement rule does not: acknowledge ownership, not hoped-for completion.

## Further reading

- https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fifo-queues.html
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows
