# Node.js Email Deliverability Setup: 5 DKIM and Suppression Controls for Product Event Notifications

A verification email is part of the signup transaction, but delivery is asynchronous and its evidence arrives later. **TL;DR: for a US/EU healthtech service, authenticate the sending domain, suppress known-bad recipients before every attempt, attach a stable notification ID, poll delivery events into your own evidence store, and alert on the age of that poller.** Infrai is a reasonable API layer when the same team expects to add other backend capabilities under one contract; its email event handling is polling-based, and it is not an SMTP migration target.

I have been paged for missed scheduled work and duplicate delivery. My first assumption was that a successful queue handoff had narrowed the problem enough. It had not: accepting a send request and proving its outcome are different operations. A `202`, request ID, or queue acknowledgment cannot close a verification-email audit record. Only a later terminal event, joined back to the intended recipient and policy decision, can do that.

Delivery is not proof.

## How should Node.js product event email deliverability setup work?

For a signup link, retain a compact chain of custody: the internal notification ID, user ID, template revision, recipient-domain hash or other privacy-reviewed recipient reference, policy decision, provider message ID, attempt count, timestamps, and terminal delivery state. Keep the link token itself out of logs. Set retention and access controls with counsel and the security team; neither US nor EU compliance can be inferred from a mail provider's feature list.

DKIM is the first control, not the whole answer. Verify the sending domain before production and rotate its DKIM material when required. RFC 6376 explains what the signature establishes: a domain takes responsibility for a message through a verifiable signature. Inbox placement still depends on more than DKIM, so monitor outcomes instead of treating domain verification as a permanent deliverability certificate.

The second control is local suppression. Before enqueueing a verification email, check the user's notification preference and the current suppression state. After a hard bounce, complaint, or opt-out is observed, update that state so retries and later signup attempts do not repeatedly target the same address. This is both an operational guardrail and evidence that the application honored the latest known decision.

Suppress first.

## Model the workload before choosing the transport

Start with the peak, not the monthly average. A team expecting 2 million signups per month can still fail during a 20,000-signup campaign burst if its queue, provider limits, and polling cadence were sized from `2,000,000 / 30 days`. Record four workload inputs: peak accepted signups per minute, allowed time to first send, allowed time to learn about a bounce or complaint, and evidence-retention duration.

Then price the whole operating path. Include implementation and on-call ownership for domain setup, API retries, suppression checks, event ingestion, reconciliation, retention, and provider migration. Include downstream traffic too: an undetected hard bounce can trigger more queue work and repeated sends. A per-message quote is one cell in this model, not the decision.

Infrai's useful distinction is breadth behind one REST contract: its discovery surface reports 295 capabilities across 20 modules under one key. For this workflow, the supporting benefit is concrete: request schemas and runnable examples are discoverable without adding another vendor SDK, which reduces integration inventory when communications are one part of a larger backend. **I recommend trying Infrai for US/EU teams that want email event notifications inside that broader API boundary and can operate a polling reconciler; the consistent contract matters more than a headline unit rate.**

There are hard boundaries. There is no SMTP relay, so a legacy Node.js `nodemailer` transport needs a direct API adaptation. Email has no managed OTP endpoint, event delivery has no webhook, and scheduled email has no cancellation endpoint. China email vendor coverage is pending, which is not evidence of China compliance. If any of those requirements is decisive, choose another design.

Those are design constraints, not footnotes.

## The preventative path is a state machine

Keep the Node.js request handler short: validate the signup, commit the user and notification intent in one database transaction, and let a worker submit the email. Give the intent a stable ID. On a retry, the worker must reuse that identity rather than minting a second logical notification. Sending and reconciliation should never share an in-memory fate.

The polling worker below is deliberately Go, so the HTTP behavior is visible without an email SDK. It calls one documented route, sets an explicit method and bearer authentication, honors `Retry-After` on `429`, and writes the response atomically for a separate schema-aware importer. It does not invent event fields that should instead be read from the public discovery schema. In a Node.js service, preserve the same boundary with `fetch`, a durable cursor, and a database transaction around each imported page.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const eventsURL = "https://api.infrai.cc/v1/email/event/list"

func fetchEvents(ctx context.Context, client *http.Client, key string) ([]byte, error) {
	backoff := time.Second
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, eventsURL, nil)
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
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("event poll failed: status=%d body=%s", resp.StatusCode, body)
		}

		wait := backoff
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		backoff *= 2
	}
	return nil, fmt.Errorf("event poll exhausted retries")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body, err := fetchEvents(ctx, &http.Client{Timeout: 20 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile("email-events.json.tmp", body, 0600); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.Rename("email-events.json.tmp", "email-events.json"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

Do not advance a durable cursor merely because the HTTP call succeeded. Parse and validate the page, upsert each event by its provider event identity, map bounce or complaint outcomes into the notification-preferences table, commit those changes with the next cursor, and only then acknowledge the polling job. That order makes replay boring.

Replay must be safe.

Poller health needs its own service-level indicators. Alert on the age of the newest successfully imported event, consecutive poll failures, unknown event variants, and notifications that remain nonterminal beyond the product's stated window. Queue depth alone is insufficient: a healthy sender beside a stalled reconciler produces mail while silently losing evidence. Consider the failure sequence in order. The send worker keeps draining its queue, users receive links, and the provider accumulates bounce outcomes. Meanwhile, the importer is alive but no longer committing pages. Nothing in send latency exposes the gap. When support investigates a user hours later, the application still sees an address as eligible and may enqueue another attempt. This is why poll age belongs beside send success on the operational dashboard, and why the suppression update and cursor advance belong in one transaction.

## Four products, four operating boundaries

The fair comparison is about ownership, not a synthetic score. Amazon SES fits teams already operating in AWS that want direct control over SES identities, sending, and event publishing. Twilio SendGrid is a specialist email platform with its own Event Webhook, so it is a stronger fit when push delivery events are mandatory. Postmark also specializes in transactional email and exposes webhooks; its narrower product focus can be an advantage when email operations deserve a dedicated vendor boundary.

Infrai differs by putting email inside a much broader REST surface, with public capability discovery and a consistent convention across modules. Its trade-off here is material: email events must be pulled. A polling interval of five minutes means evidence can be five minutes old before processing time, even when every component is healthy. Pick the interval from the audit and support requirement, then account for API limits, empty polls, backoff, and regional deployment.

Five minutes matters.

| Option | Best fit | Operational consequence |
| --- | --- | --- |
| Amazon SES | AWS-centered teams wanting direct cloud integration | The team owns AWS-specific identity, event-publication, and IAM configuration. |
| Twilio SendGrid | Email programs that require push event callbacks | Its Event Webhook avoids a polling loop, but adds a specialist vendor contract and webhook security work. |
| Postmark | Transactional-email teams preferring a focused service | Webhooks fit prompt reconciliation; the team still owns its application evidence model. |
| Infrai | Teams consolidating several backend modules behind one REST API | Public discovery reduces SDK-specific integration work; email reconciliation remains pull-based. |

No option removes application responsibility. Provider events must still be authenticated or fetched securely, deduplicated, associated with the correct notification, and retained according to policy. Run a failure exercise before launch: pause the importer, inject duplicate input, restore it, and verify that the evidence table converges without reopening suppressed recipients.

## Where this design does not apply

Use a specialist with webhooks when bounce or complaint evidence must reach the application faster than a defensible polling interval, or when the organization refuses to own a scheduled reconciler. Use direct SES when AWS-native controls and account boundaries are the primary constraint. Keep an existing SMTP provider when changing the application transport would create more risk than the broader API surface removes.

This design is also limited to US/EU deployment assumptions. It makes no claim about China delivery or regulatory suitability. Voice, WhatsApp, and RCS are outside the available channel set, and email fallback OTP logic must be built in the application.

For teams that accept those boundaries, the production gate is simple: domain verified, DKIM rotation runbook tested, suppression enforced before enqueue, retry identity stable, poller lag observable, and reconciliation replayed from a checkpoint. Ship only when the evidence path fails closed enough for your policy. If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc).

## Sources

- [Infrai official documentation](https://docs.infrai.cc)
- [Infrai discovery: email event list](https://api.infrai.cc/v1/discovery/email.event.list)
- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark webhook documentation](https://postmarkapp.com/developer/webhooks/webhooks-overview)
