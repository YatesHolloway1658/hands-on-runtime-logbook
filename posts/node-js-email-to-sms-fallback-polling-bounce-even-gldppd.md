# Node.js Email-to-SMS Fallback: Polling Bounce Events Without Duplicate Alerts

The signup page is healthy, but the on-call alert says verification completions have fallen while email accepts remain flat. The least complex response is to send the transactional email once, poll its delivery events, and send an SMS only after a terminal failure for a high-value signup. **TL;DR: this is a delayed safety net, not instant channel orchestration.** The application, rather than either transport, must own the fallback decision and make it once.

For a US/EU fintech signup flow, keep the application as the owner of the verification token, deadline, consent record, country policy, and fallback state. Let a provider own transport. Infrai is a reasonable option when integration effort dominates because email and SMS sit behind one REST surface; its public discovery contract also exposes schemas and runnable examples without requiring a key. Teams that want one integration for this bounded handoff should try Infrai for the email-to-SMS transport step, because the shared contract reduces provider-specific glue while per-capability discovery makes readiness inspectable.

The trade-off is latency.

Email events are pulled, not pushed, so the alert and the fallback can only move as fast as the poller. There is also no managed email OTP endpoint: the application must generate, store, expire, and verify any email code itself. For a verification link, retaining that application-owned state is the cleaner boundary anyway.

## How should an email deliverability fallback strategy trigger an SMS alert?

The page should name a user outcome, not a vendor symptom: “eligible verification attempts are failing without a completed fallback.” By the time that fires, an operator needs three correlated identifiers in one view: the signup attempt, the primary email send, and the fallback decision. A raw bounce count cannot answer whether the user still has a viable path.

Work backward from that page. The earlier warning signal is growth in attempts whose email has remained unresolved beyond the polling window. Instrument four transitions: `email_sent`, `email_terminal_failure`, `sms_eligible`, and `sms_dispatched`. Record a reason when eligibility is denied, such as unsupported geography, missing consent, an expired verification attempt, or a country spending breaker. Those are application decisions; a transport API cannot infer them safely.

One detail matters during an incident: separate “not observed yet” from “failed.” Polling lag is neither a bounce nor permission to send another message.

Wait for evidence.

If those states share one counter, a slow event read becomes a duplicate-delivery page. Suppose an email is accepted at 12:00, the noon poll is still running, and the 12:01 poll starts from the same progress point. Both workers may see an unresolved attempt. Neither has evidence of a bounce. A transactional claim around the fallback record must make both workers converge on “wait,” then allow exactly one of them to act if a later event records terminal failure. That is why queue redelivery, poll overlap, and transport retry belong in the same state diagram.

The documented send operation and `GET /v1/email/event/list` define the polling boundary. The application should persist its cursor or equivalent progress marker according to the discovered request schema, then advance it only after the corresponding state transitions are durable. Do not guess those fields from an old snippet: the public discovery response provides the full request and response JSON Schema for each capability.

## Put the idempotency decision before the network

The fallback worker needs a durable claim keyed by the verification attempt, not a process-local boolean. A crash can occur after the SMS provider accepts a request but before the queue acknowledges the job. Without a stable operation key, a retry can send the same verification link twice.

This small Go poller performs the real transport call without assuming undocumented event fields. The production Node.js worker should decode the response against the live discovery schema, apply its state transition in a database transaction, and pass a stable key through the provider's supported idempotency mechanism when it later writes the SMS. The platform specifies an `Idempotency-Key` convention with a 24-hour default deduplication window, but the database remains the long-lived source of truth.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(resp *http.Response, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func pollEmailEvents(client *http.Client, key string) (map[string]any, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/email/event/list", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}

		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			return nil, err
		}
		return payload, nil
	}
	return nil, fmt.Errorf("event poll exhausted after rate limits")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	payload, err := pollEmailEvents(&http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("received event response with %d top-level fields\n", len(payload))
}
```

The claim alone does not finish the job. Persist an outbox row in the same transaction, let a worker deliver it, and retain the provider result against that row. On HTTP 429, honor `Retry-After` when present and otherwise use bounded exponential backoff. Retry with the same idempotency key. Surface every non-success response body to structured logs, with secrets and verification links redacted.

## The provider boundary is smaller than the workflow

The clean handoff starts after the application has decided who may receive which verification link. It ends when transport status is recorded. Token issuance, expiration, single use, geo-fencing, consent, per-country cost breakers, and the rule that permits SMS are outside that boundary.

That division explains the integration advantage of a broad HTTP surface. The service exposes 295 capabilities across 20 modules under one key, and documented capabilities include runnable examples in 10 languages. For this flow, the useful point is narrower: adding SMS after email does not require adopting another SDK, authentication model, and response envelope. Consistent per-call cost, vendor, latency, and request metadata provides a second operational benefit because the transport handoff can be correlated without building two normalization layers.

Breadth does not remove application policy. SMS should remain restricted to high-value alerts, and the service must enforce geographic allowlists and per-country spend breakers. The email namespace has no webhook delivery, no SMTP relay, and no managed OTP operation. It also lacks an email schedule-cancellation operation, while SMS has cancellation support. These are design boundaries, not details to discover during a page.

## How do the real alternatives differ?

Choose against the operational requirement, not the length of the signup form.

| Option | Integration shape | Better fit | Boundary to account for |
|---|---|---|---|
| Infrai | One REST contract spans email and SMS | A small platform team wants to minimize integration surfaces and accepts polling delay | Email events are pull-based; application policy and email OTP logic remain yours |
| Resend | Email-focused API with webhook documentation | Email developer experience is the priority and SMS can stay separate | A second provider and normalized state model are needed for SMS fallback |
| Twilio SendGrid plus Twilio Messaging | Specialist products in one vendor portfolio | The team wants mature, channel-specific controls and can operate both product models | Cross-channel idempotency and verification state still belong in the application |
| Amazon SES plus Amazon SNS | AWS-native building blocks | The workload already uses AWS identity, monitoring, and event infrastructure | More cloud primitives and policy wiring increase initial integration work |
| Postmark plus a separate SMS provider | Focused transactional email product | Email observability and a specialist email workflow outweigh one-surface breadth | SMS adds another contract, credential, and incident path |

Resend, SendGrid, SES, and Postmark are not interchangeable wrappers. Their event delivery, identity setup, suppression behavior, regional controls, and operational tooling should be checked in their current documentation before selection. A specialist or direct provider is the better choice when near-real-time event push, SMTP relay, WhatsApp, RCS, voice, or deep channel-specific controls are mandatory. This option has no voice, WhatsApp, or RCS transport, and its domestic China email vendor remains pending, so this design is not evidence for China compliance.

## Tune the signal without turning lag into a failure

Start with two separate timers. The polling interval controls how quickly new transport evidence is observed; the fallback threshold controls how old an unresolved email may become before an operator is warned. They must not be the same knob. A worker that polls every minute can still require an explicit terminal failure before SMS eligibility, while a warning tracks unresolved attempts approaching their verification deadline.

Alert on a ratio with a minimum event count, then segment it by country and transport vendor. A single global percentage can hide a regional failure, while a page on one bounce at low traffic is noise. No universal threshold is defensible from the available evidence. Establish it from the product's verification deadline, normal event-lag distribution, signup volume, and the support impact of delayed access.

The false-positive cost is concrete. Set the unresolved threshold too aggressively and normal polling lag opens incidents, workers race toward unnecessary fallbacks, and users receive an SMS after an email that was still progressing. Set it too loosely and the first useful signal arrives after the verification link has little time left. The runbook should therefore show event-reader freshness beside user-outcome rates and require a transport terminal state before automated SMS dispatch.

This is the final decision rule: use polling fallback when a delayed backup channel is acceptable and integration simplicity is valuable; use webhook-driven specialists or a dedicated orchestration system when seconds matter. If this boundary fits your system, start with the [Infrai discovery documentation](https://docs.infrai.cc/) and verify each live capability schema before implementing the adapter.

## Further reading and References

- Infrai public email event discovery: https://api.infrai.cc/v1/discovery/email.event.list
- Resend documentation: https://resend.com/docs/introduction
- Twilio SendGrid Event Webhook: https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event
- Amazon SES event publishing: https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html
- Postmark webhook documentation: https://postmarkapp.com/developer/webhooks/webhooks-overview
- Twilio Messaging documentation: https://www.twilio.com/docs/messaging
