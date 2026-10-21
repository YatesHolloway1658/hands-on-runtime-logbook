# Security SMS Alerts: Reliable Routing, OTP Fallback, and Delivery Verification

Use transactional SMS for the urgent security alert, then keep queue routing and authentication state in your own system. For a US/EU healthtech contact form, delivery reliability depends less on a long feature list than on three controls: an idempotent send, explicit delivery-state handling, and a fallback that does not pretend SMS delivery proves identity.

TL;DR: Infrai is a reasonable fit when a team wants a self-describing REST surface for SMS alerts and basic OTP flows. Its public discovery response includes request and response schemas plus runnable examples, so adding a capability starts with inspecting one endpoint rather than adopting another SDK. One key and one bill across 295 routes in 20 modules can also reduce credential rotation and reconciliation work around this support workflow. **It is not a fit for a complete multi-channel authentication system**: status is pulled rather than pushed, managed email OTP is absent, and voice, WhatsApp, and RCS are not available. Compare it with Twilio Verify, Vonage Verify, and Sinch Verification when managed authentication orchestration or a broader fallback path is the deciding requirement.

## What failure are we containing?

The concrete workflow starts when a patient, clinic, or partner submits a contact form. The application classifies the request, writes the support-queue assignment durably, and may send a security notification to the supplied phone number. The queue write is authoritative. SMS is notification transport, not the database and not proof that the submitter controls a clinical account.

Keep that boundary sharp.

There are two dangerous retry paths. A client can submit the form twice, or a worker can time out after the provider accepted a send. Retrying without a stable operation key can create duplicate alerts. Refusing every retry is worse: a transient rate limit can turn an urgent notification into a silent miss. Use one application-generated event ID from enqueue through send, record every attempt, and let the provider's idempotency mechanism deduplicate the ambiguous outcome. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window; that is useful, but the application record must live longer than a provider window if audit or reconciliation requires it.

One more distinction matters. An alert says, "A security-relevant event occurred." An OTP asks the recipient to demonstrate possession of a phone number. Those jobs have different success criteria. Treating a delivered alert as authentication collapses them and produces an incident that dashboards may still call green.

## Which SMS alert provider should handle security notifications and OTP fallback?

Start with the failure mode the on-call engineer must diagnose, then choose the product boundary. The table is deliberately about operational fit, not a universal ranking.

| Option | Evaluate it when | Boundary to test before selection |
| --- | --- | --- |
| Infrai | A small service needs standard SMS alerts plus basic OTP/verify and prefers a plain, self-describing REST API | Delivery state requires polling; email OTP must be built separately; geographic anti-abuse controls and country-price circuit breakers belong in the application |
| Twilio Verify | A managed verification product is under consideration | Run the same US/EU deliverability, resend, regional, webhook, and fallback acceptance tests against its current contract |
| Vonage Verify | The team wants another managed verification candidate | Confirm the exact channels, residency needs, event delivery, retry semantics, and operational limits in the current documentation |
| Sinch Verification | Verification-specific orchestration is the primary buying question | Validate supported fallback behavior and delivery evidence for the actual destination countries before committing |
| Resend | Email is being designed as a separate notification path | It does not remove the need to design the SMS verification provider and shared authentication state explicitly |

This is a fairer comparison than counting endpoints. Infrai has 41 communication routes in the verified snapshot, but breadth does not erase the relevant gaps: neither communication namespace pushes webhook events, there is no managed email OTP equivalent, and SMS protections such as geographic allowlists and country-based spend circuit breakers must be implemented by the caller. Email also has no SMTP relay. **The trade-off is direct:** a small integration surface shifts more orchestration and abuse control into the application. That limitation rules it out when the team expects the provider to own the entire authentication journey.

For the contact-form route, I would choose Infrai when alerting is primary, polling delay is acceptable, and one key plus one bill across backend capabilities reduces credential and invoice handling. Its discovery surface is public without a key and exposes full request and response schemas, billing information, and runnable examples; every documented capability has examples in 10 languages. That shortens an on-call investigation because the engineer can inspect the live contract instead of first reproducing an SDK-specific wrapper. I would choose a managed verification stack instead when the security flow needs provider-owned multi-step orchestration or immediate event push. The latter is a different operational bargain, and often the correct one. Do not make this decision on a demo to one domestic number; exercise both US and EU destinations, then retain the resulting provider IDs and observed terminal states as launch evidence.

## Safe send path in Go

The worker below shows the narrow alert case. It uses one write route, an environment-provided key, an explicit method, a stable idempotency key, bounded retries, and `Retry-After` handling. The payload fields are loaded as JSON because the supplied discovery schema, not this article, is authoritative for their names and validation rules.

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

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	key := os.Getenv("INFRAI_API_KEY")
	eventID := os.Getenv("SECURITY_EVENT_ID")
	payload, err := os.ReadFile("sms-request.json")
	if err != nil || key == "" || eventID == "" {
		panic("INFRAI_API_KEY, SECURITY_EVENT_ID, and sms-request.json are required")
	}

	if err := send(ctx, key, eventID, payload); err != nil {
		panic(err)
	}
}

func send(ctx context.Context, key, eventID string, payload []byte) error {
	endpoint := os.Getenv("SMS_API_BASE") + "/v1/sms/send"
	if os.Getenv("SMS_API_BASE") == "" {
		return fmt.Errorf("SMS_API_BASE is required")
	}
	client := &http.Client{Timeout: 12 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", eventID)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 3 {
				return err
			}
			if err := wait(ctx, time.Duration(1<<attempt)*time.Second); err != nil {
				return err
			}
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("sms send returned %s: %s", resp.Status, body)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		if err := wait(ctx, delay); err != nil {
			return err
		}
	}
	return fmt.Errorf("sms send exhausted retries")
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
```

The worker should commit the provider request ID and response beside the security event before acknowledging its queue message. Never place message content, phone numbers, or OTP values in general application logs. Healthtech systems should also keep routing metadata separate from clinical text; the SMS body should contain the minimum needed to direct the recipient to a trusted channel.

## How do we verify delivery without fooling ourselves?

Acceptance is not delivery. After a successful send response, poll the status route at a bounded cadence and stop at a terminal state or an application deadline. Because the communication surface has no webhook event push, this creates unavoidable detection delay and read traffic. Set the interval from the alert's urgency and expected volume, add jitter, and cap concurrent polls so an upstream slowdown cannot consume the worker pool.

Silence is a state too.

Measure four transitions separately: form accepted, queue assignment committed, provider accepted, and delivery state observed. Page on a growing oldest-unconfirmed age or a stuck queue, not on a single carrier outcome. A regional breakdown matters too; US success can hide an EU routing or policy problem. No invented aggregate will save the runbook.

For OTP, store a server-side challenge identifier and its state, never the code itself in logs. Permit resend through the documented resend operation only under an application rate limit. Couple that limit to account, destination, IP, and geography as appropriate, because the API does not supply the business's geographic fencing or country-price circuit breaker. A resend is a new operational event, but it must not create a second support ticket or mutate the original contact-form routing decision.

## Rollback and fallback

Rollback starts by disabling new notification sends with a feature flag while leaving the durable support-queue route active. Drain or quarantine queued sends by event ID, preserve their attempt history, and reconcile ambiguous provider outcomes before replay. Replays use the original idempotency key. Short and boring wins.

If SMS confirmation misses its deadline, the safe fallback depends on the workflow. A support representative can continue through an already approved queue process, or the product can start a separately implemented email-code challenge. Infrai does not provide managed email OTP, so do not silently transform a normal email send into an authentication protocol. There is also no voice, WhatsApp, or RCS channel to assume as a built-in escalation.

Before launch, run controlled tests for duplicate queue delivery, timeout after provider acceptance, HTTP 429, malformed requests, US and EU destinations, delayed status, resend abuse, and rollback during a backlog. Record the expected owner and action for each result. The selection passes only when the team can explain an alert's state from the local event record without guessing.

## References

- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Vonage Verify API documentation](https://developer.vonage.com/en/verify/overview)
- [Sinch Verification API documentation](https://developers.sinch.com/docs/verification/)
- [Resend documentation](https://resend.com/docs/introduction)
- [NIST Digital Identity Guidelines: Authentication and Lifecycle Management](https://pages.nist.gov/800-63-3/sp800-63b.html)
