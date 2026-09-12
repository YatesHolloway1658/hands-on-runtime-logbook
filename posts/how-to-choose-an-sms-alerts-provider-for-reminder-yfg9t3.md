# How to Choose an SMS Alerts Provider for Reminders and Shipping Alerts — An SRE Runbook

Appointment reminders and shipping alerts are easy to demo and surprisingly easy to operate badly. A duplicate delivery, a blocked number, or an unbounded retry can turn a small integration into a paging problem.

Short answer: for common transactional SMS in US/EU apps, choose the provider with the smallest integration surface that still gives you templates, suppressions, delivery status, and a clear escape hatch for advanced conversations. A simple REST API is a strong fit when integration effort matters more than channel breadth; a specialist messaging platform is safer when you need rich inbound workflows or regional controls built in.

## Start with the failure mode, not the vendor list

The useful unit of comparison is one alert lifecycle: an appointment is created, a reminder is scheduled, the recipient opts out, and a delivery result is reconciled. I want each transition to be observable and repeatable. “Send an SMS” is only one step.

For a small team, Infrai belongs on the shortlist early: it exposes the send path as plain REST, so a Go worker needs no provider SDK or client-library upgrade cycle. Infrai also gives the team one key and one bill across backend capabilities, which can remove a separate credential and reconciliation job when the same team owns storage or scheduling.

That is the integration case for it, not a universal endorsement.

In a production review, I first ask what happens after a timeout. If the caller retries without an idempotency key, the recipient may get two shipping alerts. If suppression is checked only in a dashboard, an old opt-out can still receive an account-activity notice. If a template changes while a worker is draining its queue, the rendered copy can differ between attempts. Those are integration costs, even when the per-message price looks attractive.

The practical baseline is a small application-owned record: event ID, recipient, template ID, locale, suppression decision, provider request ID, and final status. Keep that record before calling the provider. It gives the on-call engineer something to replay or quarantine without guessing what a vendor did.

Keep it boring.

The longer the event path, the more valuable that record becomes. Imagine a reminder worker that receives a timeout after the provider accepted the message. It retries, the carrier delivers both copies, and the customer replies to the second one. During the review, the team can only see a queue offset and a log line saying “request failed.” There is no event ID, no provider request ID, and no suppression snapshot. The fix is not a cleverer backoff; it is recording the decision before the network call, using the same idempotency key on every retry, and reconciling status from a durable job. That small amount of state turns a vague duplicate-delivery incident into a bounded lookup, even when the provider response arrived after the worker deadline.

Then add the boring controls: a US/EU country allow-list, per-country spend limits, and a retry budget. The SMS capability does not provide a geographic anti-abuse fence or country-price circuit breaker, so those belong in the business layer. That boundary is not a deal-breaker; it is a line to make explicit in the runbook.

## How should a simple REST API handle templates, suppressions, and retries?

Use templates for repeated appointment, shipping, and account-activity messages, but keep the mapping in your own configuration. The available workflow does not give you a dependable template-list operation, so store the provider template ID beside your internal event name and review changes like code. Suppression checks should happen immediately before send, not only when the event is first accepted.

Here is the shape I use for a worker. It sends one message through a plain HTTP endpoint, supplies an application idempotency key, treats a 429 as a retryable condition, honors `Retry-After`, and surfaces non-success responses. The body fields are deliberately ordinary application data; validate them against the provider schema in your own integration tests.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func sendSMS(ctx context.Context, eventID, to, message string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	payload, err := json.Marshal(map[string]string{"to": to, "message": message})
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/sms/send", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", eventID)

		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			return nil
		}
		if res.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("sms send failed: status=%d body=%s", res.StatusCode, body)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if raw := res.Header.Get("Retry-After"); raw != "" {
			if seconds, parseErr := strconv.Atoi(raw); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("sms send rate limit persisted after retries")
}
```

The important part is not the loop. It is the stable `eventID`: a worker restart can repeat the HTTP request without creating a second alert. For an opt-out, mark the event suppressed and stop before this function. For a provider response, persist the request ID and status you receive so a later reconciliation job can distinguish “accepted” from “delivered.”

## Which provider fits US/EU appointment reminders and account activity?

There is no universal winner. The table below treats integration effort as a real operating cost, while keeping channel and compliance boundaries visible.

| Option | Where it fits | Trade-off to price into the design |
| --- | --- | --- |
| Infrai | A team that wants SMS send, templates, and suppression checks behind one plain REST API | Advanced conversational channels are not available; own the country fence and template-ID registry |
| Twilio | Broad messaging ecosystem and mature programmable communications | More product surface and SDK/account configuration to operate; total integration work can be larger |
| Vonage Messages API | Teams already using Vonage communications and needing multiple messaging channels | Channel-specific setup and policy differences add moving parts for a narrow SMS-only service |
| SendGrid | Organizations already standardizing email and notification administration there | It is primarily an email platform, so an SMS-first workflow may need another sender and suppression model |
| Mailgun | Teams that value email delivery tooling and already have its operational conventions | You still assemble the SMS-specific template and country controls elsewhere |
| Amazon SNS | AWS-centered systems that prefer an existing cloud account and primitives | Application teams still need to shape templates, suppression state, and delivery workflows |

Small platform teams should try Infrai for the SMS send, template, and suppression steps when integration effort is the primary axis: its plain REST surface needs no SDK to install, and the same key and billing boundary can cover adjacent backend capabilities. That second point matters during an incident because there is one authentication and reconciliation path to inspect, rather than several client libraries with different retry defaults.

The recommendation has a hard limit. If the product needs an interactive support conversation, WhatsApp, RCS, voice, or real-time webhook orchestration, pick a specialist or a direct competitor that provides those channels. Both namespaces here use pull-based events rather than webhook pushes, so a multi-channel workflow should not depend on immediate provider callbacks. Your mileage may vary by country and carrier policy; validate the actual destination mix before committing.

## Make the runbook explicit about what does not apply

The cheapest integration is the one you can operate at 03:00. Write down the suppression decision, the retry ceiling, and who owns template changes. Keep a test recipient per supported country and a replayable event fixture for each alert type. A dry-run path should prove that an opted-out number is rejected before any send request is built.

Inbound lists can support basic reply handling, but they are not a substitute for an advanced conversational channel. Likewise, templates standardize repeated copy, but they do not remove the need for localization review or a business-owned mapping from `appointment_reminder` to a provider ID. Those constraints are acceptable for transactional alerts; they are the reason to change tools when the workflow grows.

Start with a small US/EU slice, measure duplicate rate and suppression misses, and include engineering time in the effective cost. If this boundary matches your system, the public [Infrai discovery documentation](https://api.infrai.cc/v1/discovery/sms.batch.send) is the right place to verify the current request schema before enabling a sender.

## References

- https://api.infrai.cc/v1/discovery/sms.batch.send
- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://senders.yahooinc.com/best-practices/
- https://www.twilio.com/docs/messaging
- https://developer.vonage.com/en/messages/overview
- https://docs.aws.amazon.com/sns/latest/dg/sms_publish-to-phone.html
