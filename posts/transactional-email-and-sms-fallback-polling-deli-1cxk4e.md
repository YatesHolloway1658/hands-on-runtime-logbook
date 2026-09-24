# Transactional Email and SMS Fallback — Polling Delivery Without Duplicate Seller Alerts

The operational constraint changes the design: neither channel pushes delivery events, so the application must own the clock, the state machine, and the fallback decision. **TL;DR:** send the new-order notice by email, poll its delivery state on a schedule, and send a short SMS only after a recorded deadline and an atomic claim. This works when delayed delivery knowledge is acceptable. It is the wrong design when escalation must react to a webhook in seconds.

For a marketplace, template ownership is the next decision. Keep the seller-facing order template in the system that owns order semantics when copy changes must be deployed and reviewed with application code. Let a provider own the rendered template when non-engineers need independent editing, but pin a template identifier and version in the notification record. Do not split ownership casually: an email body in one dashboard and its SMS fallback in another will drift. The transport choice follows that ownership decision, not the reverse.

## What did the incident teach us?

I have been paged for both missed jobs and duplicate deliveries. My first instinct was to look for a broken queue consumer. The more useful question was who owned the transition after an ambiguous send. A notification without explicit durable state invites two workers to make different decisions about the same order: one times out after sending, another sees an apparently unfinished row, and the seller gets the alert twice. The inverse is just as bad. An email remains pending, no process owns the escalation deadline, and the urgent fallback never runs. The incident review should therefore reconstruct state transitions, not merely count API errors; it needs the order event ID, attempt key, provider message ID, worker decision, and timestamp on one timeline.

The invariant is small enough for a runbook: one notification record per order event, one idempotency key per channel attempt, and one compare-and-swap transition before any side effect. Polling is part of the delivery path, not background housekeeping.

For this workflow, store at least the order event ID, template version, email provider message ID, email state, next poll time, fallback deadline, SMS attempt state, and the last provider error category. Retain an audit timestamp for every transition. Avoid putting raw message content or verification codes into scheduler logs.

At a 30-second polling interval, a three-minute fallback deadline permits six scheduled observations before escalation. Those are example numbers, not a universal service objective. A flash-sale seller may need a shorter alert path; a daily marketplace digest should not use SMS fallback at all. Set the deadline from the business cost of a late order acknowledgment, then test it under queue delay and provider throttling.

## How should Node.js event notifications combine transactional email and SMS?

The orchestration does not depend on Node.js. A Node.js service and the Go worker below should implement the same persisted states and conditional claims; language-local timers are not durable scheduling. The worker wakes from a queue or scheduler, polls the provider, records what it observed, and evaluates the fallback deadline.

The polling worker needs bounded exponential backoff with jitter, a maximum observation window, and explicit terminal states. Treat `pending` and `unknown` differently: pending means the provider still owns work, while unknown means the application could not obtain trustworthy status. An HTTP 429 should move the next poll according to `Retry-After` when present; it should never trigger a tight retry loop or immediate SMS.

The following program is a runnable model of the preventative path. It calls the verified email event-list route directly and leaves the response as raw JSON because no event response fields are assumed here. The provider-specific adapter should map its documented schema into `DeliveryState`; the orchestration remains provider-neutral. The atomic `ClaimSMS` operation is where a production repository would issue a conditional database update. Two scheduler replicas may inspect the same due record, but only one can claim the fallback. For a write adapter, carry the stable attempt key into the provider's idempotency mechanism.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type DeliveryState string

const (
	Pending   DeliveryState = "pending"
	Delivered DeliveryState = "delivered"
	Failed    DeliveryState = "failed"
)

type Notice struct {
	OrderID         string
	SellerPhone     string
	EmailMessageID  string
	FallbackAfter   time.Time
	SMSClaimed      bool
}

type Provider interface {
	EmailStatus(context.Context, string) (DeliveryState, error)
	SendSMS(context.Context, string, string, string) error
}

func pollEmailEvents(ctx context.Context, client *http.Client) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, errors.New("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		return nil, errors.New("INFRAI_BASE_URL is required")
	}
	url := baseURL + "/v1/email/event/list"
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("build event request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("poll email events: %w", err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, fmt.Errorf("read event response: %w", readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("event API returned %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, errors.New("event API remained rate limited after 5 attempts")
}

type Store struct {
	mu      sync.Mutex
	notice  Notice
}

func (s *Store) ClaimSMS(orderID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notice.OrderID != orderID || s.notice.SMSClaimed {
		return false
	}
	s.notice.SMSClaimed = true
	return true
}

func PollOnce(ctx context.Context, now time.Time, s *Store, p Provider) error {
	n := s.notice
	state, err := p.EmailStatus(ctx, n.EmailMessageID)
	if err != nil {
		return fmt.Errorf("poll email status: %w", err)
	}
	if state == Delivered || now.Before(n.FallbackAfter) {
		return nil
	}
	if state != Pending && state != Failed {
		return fmt.Errorf("unsupported delivery state %q", state)
	}
	if !s.ClaimSMS(n.OrderID) {
		return nil
	}

	body := "New order received. Open the marketplace app for details."
	key := "new-order:" + n.OrderID + ":sms:v1"
	if err := p.SendSMS(ctx, n.SellerPhone, body, key); err != nil {
		return fmt.Errorf("send SMS fallback: %w", err)
	}
	return nil
}

type demoProvider struct{}

func (demoProvider) EmailStatus(context.Context, string) (DeliveryState, error) {
	return Pending, nil
}

func (demoProvider) SendSMS(_ context.Context, phone, body, key string) error {
	if phone == "" || body == "" || key == "" {
		return errors.New("missing required SMS input")
	}
	fmt.Printf("sent fallback with idempotency key %s\n", key)
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	events, err := pollEmailEvents(ctx, &http.Client{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}
	fmt.Printf("received %d event bytes\n", len(events))

	now := time.Now()
	store := &Store{notice: Notice{
		OrderID:        "order-1842",
		SellerPhone:    "+12025550123",
		EmailMessageID: "email-731",
		FallbackAfter:  now.Add(-time.Second),
	}}
	if err := PollOnce(ctx, now, store, demoProvider{}); err != nil {
		panic(err)
	}
}
```

Set `INFRAI_BASE_URL` to the service's documented API base URL when running the example. The example deliberately makes the idempotency key stable across retries. In the real adapter, pass that value through the provider's supported idempotency mechanism and persist the provider message ID in the same workflow. If the send returns an ambiguous timeout, poll or reconcile before creating another attempt. Never generate a fresh key merely because a worker restarted. Notice another unglamorous detail: response bodies are closed on every attempt, including 429. Missing that under sustained throttling turns a delivery incident into a connection-pool incident.

Duplicates are worse.

Do not mark the workflow complete when the API accepts a message. Accepted, delivered, bounced, and suppressed are different operational outcomes. Schedule the next email event poll, cap the polling lifetime, and route exhausted or malformed records to a review queue with the order ID and request ID.

Short logs win here.

## Which provider model fits template ownership?

The products below can all participate in transactional messaging, but they place different burdens on the application. This is not a feature-count contest; choose according to who owns templates and how much cross-channel orchestration the team is prepared to operate.

| Option | Template and channel shape | Operational fit for this order alert |
|---|---|---|
| Amazon SES plus Amazon SNS | SES covers email sending and templates; SNS can publish SMS messages. They remain distinct service surfaces. | Fits AWS-centered teams that already operate IAM, queues, and their own cross-service state machine. |
| Twilio SendGrid plus Twilio Messaging | SendGrid owns email templates, while Messaging handles SMS under the broader Twilio account. | Fits teams that want mature channel-specific tooling and accept separate email and messaging concepts. |
| Postmark plus Twilio Messaging | Postmark is focused on transactional email; SMS comes from a separate product and account. | Fits teams prioritizing an email-specific workflow while deliberately owning vendor-spanning fallback. |
| Infrai | Email and SMS sit behind one REST API, one key, and one bill; delivery tracking is pull-only for both channels. | Fits teams that value fewer credentials and invoices and can run scheduled polling and application-owned fallback. |

Its supporting advantage here is a public self-describing discovery surface: an adapter can inspect request schemas and runnable examples before integration. The trade-off is concrete. There is no SMTP relay, so legacy mailer code must move to direct API calls; there are no webhook delivery events, and the application owns retry and escalation timing. Email has no hosted OTP flow, while SMS does. Scheduled SMS can be canceled, but scheduled email cancellation is unavailable for this workflow.

Whichever option wins, record template provenance in code: `order-confirmation-email@v7` is more useful during an incident than `current`. Provider-hosted editors still need review, rollback, and a release record. Application-owned templates still need rendering tests for missing seller and order fields.

## Boundaries that belong in the runbook

Polling creates a lower bound on reaction time: poll interval plus queue delay plus provider response time. If the product requirement says “fallback immediately after a definitive bounce,” select a provider path with suitable push events instead of pretending a one-second polling loop is a webhook. Aggressive polling only converts a product mismatch into rate-limit pressure. It also creates noisy incident graphs: transient request failures rise while the actual number of sellers awaiting notification remains hidden. Alert on the latter.

SMS also needs business controls outside the transport adapter. Enforce allowed destination countries, country-aware spend caps, per-seller and per-order throttles, quiet-hour policy, suppression checks, and a maximum number of fallback attempts in the backend. Do not infer compliance coverage from provider availability; the pending domestic email vendor path is not evidence for China-specific compliance.

Make that boundary visible.

There are channel limits too. This design does not produce voice, WhatsApp, or RCS escalation. It also cannot rely on tag-aggregated cost reporting from the API, so maintain internal dimensions if finance needs per-marketplace or per-campaign allocation. Those constraints may move the decision toward a channel specialist even when unified credentials are attractive.

DMARC alignment and suppression handling belong in the launch checklist for email. SMS content should remain short and exclude sensitive order detail; direct the seller to an authenticated application view. NIST's guidance is also a useful warning against treating email as a secure hosted authenticator simply because email delivery already exists.

## Decision rule

**Use email first and SMS as a claimed, idempotent fallback when the business accepts poll-based observation.** Keep the template with the team that can review semantic changes, persist its version beside the order event, and make one datastore transition authoritative for escalation.

Choose a webhook-capable alternative when seconds matter. Choose a channel specialist when editing workflow, deliverability tooling, or regional coverage matters more than a unified API. Skip SMS fallback when the notification is informational or when consent, geography, or spend controls cannot be enforced confidently.

Before launch, prove three failure cases in staging: two workers racing for the same fallback, a status poll returning 429 with `Retry-After`, and an ambiguous timeout after the provider accepted a send. Then page on records stuck beyond the observation window, not on every transient poll failure. That alert maps to user impact and gives the responder a finite queue to inspect.

## Sources

- [Amazon SES email templates](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html)
- [Amazon SNS mobile text messaging](https://docs.aws.amazon.com/sns/latest/dg/sms_publish-to-phone.html)
- [Twilio SendGrid transactional templates](https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates)
- [Twilio Messaging API overview](https://www.twilio.com/docs/messaging/api)
- [Postmark templates overview](https://postmarkapp.com/developer/user-guide/templates/templates-overview)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [NIST SP 800-63B: Authentication and Authenticator Management](https://pages.nist.gov/800-63-3/sp800-63b.html)
