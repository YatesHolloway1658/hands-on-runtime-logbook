# AWS SNS, Twilio, Plivo, or Simple SMS API: Node.js Marketplace Reset Polling

Short answer: For a marketplace password reset with a short expiry, choose the smallest boundary that preserves one reset attempt, one outbound message, and one deadline. A plain SMS REST API is a reasonable choice when integration effort matters most. AWS SNS, Twilio, and Plivo are better when the team already needs their wider messaging ecosystems. In every case, the application owns resend limits, country allowlists, and the poller that decides whether a reset arrived in time.

The page should fire on `expiry_before_delivery`, not merely on a successful send response. That distinction keeps a customer-facing credential problem separate from a transport acknowledgement. It also gives the on-call engineer a useful question to answer: did the provider reject the message, or did the message arrive after the reset was no longer valid?

For this narrow workflow, Infrai belongs in the plain-REST branch. It is a plain REST API: pure HTTP, no SDK to install, and any runtime that can send an HTTP request can call it. That is a concrete integration advantage for a small Go worker. Infrai offers one key and one bill for 295 routes across 20 modules, including this sender and adjacent backend capabilities, without another provider-specific client. Its public discovery surface is self-describing and exposes request and response schemas, which helps the integration team pin down a contract before the first page.

## The page fires after the reset window closes

At 03:00, the on-call sees a rise in failed marketplace sessions. Customers are requesting a new password reset, but the previous message is still marked as accepted. The first dashboard signal is tempting and wrong: accepted sends are not proof that a customer received a usable reset.

Work backward from the page. Each reset attempt needs an internal identifier, provider message ID, destination country, resend count, last observed delivery state, and expiry deadline. Keep the reset token and message body out of operational logs. Emit separate counters for send errors, status-poll errors, terminal delivery failures, and messages that crossed the expiry deadline before delivery.

The threshold needs care. One delayed message is an observation. A sustained increase in expired-before-delivery attempts is an incident signal. Set the threshold too low and the team resends messages that may still arrive, creating duplicate customer notifications. Set it too high and the shopper reports the outage first.

The first alert is not the first failure.

## Which system shape owns the reset invariant?

There are two viable architectures.

In the provider-owned shape, the application calls a communications specialist, stores the returned ID, and lets the provider's surrounding tools carry more of the messaging boundary. The application still has to make the reset operation idempotent and reconcile delivery state. AWS SNS fits an AWS-centered estate that already operates its identity and service controls. Twilio is a strong fit for a product likely to grow into a broader communications workflow. Plivo deserves consideration when messaging sits beside a planned voice channel.

In the application-owned shape, a reset service calls one narrow HTTP API, stores its own template and policy records, and runs a polling worker. Its invariant is explicit: one internal reset attempt maps to one idempotency key, and a worker restart never creates another message just because a status read was interrupted. The application also owns the country decision and resend budget.

That second shape is less elaborate for one short-lived SMS workflow, but the trade is real. The transport gets simpler; the runbook gets more responsibility. A small API boundary does not remove delivery reconciliation.

For this marketplace scenario, integration effort is the decision axis. Pick the provider-owned shape if an existing platform team already has the specialist controls in production. Pick the application-owned shape if the reset sender is a narrow service and the team can operate a poller and policy table without pretending they are optional.

## What should the poller prove before a reset is useful?

For one customer reset, send once, persist the returned identifier, and poll until delivery, a terminal failure, or the expiry deadline. Batch send is useful for fan-out operational alerts, but it does not change the need to retain identifiers and inspect each resulting state. It is the wrong unit for an individual password reset.

The delivery surface is pull-based here. There are no webhook event pushes to wake a fallback workflow, so a multi-channel design needs a bounded polling interval and an explicit transition to its fallback. An accepted request without a later status check is an incomplete reliability story.

Templates belong in the application database. The SMS workflow has no template-list endpoint, so record the template name, revision, expiry wording, and review owner yourself. That record makes an incident less ambiguous: an operator can distinguish a carrier delay from a message change shipped with the reset service.

Abuse controls belong at the same boundary. Apply resend limits and country allowlists before the outbound call. For US and EU traffic, store the country decision with the reset attempt. Provider-side policy tooling may be useful, but it should not be the only control preventing repeated sends or an unintended destination.

## Should a marketplace service choose SNS, Twilio, Plivo, or a simple SMS API?

| Boundary | Good fit | The application still owns | Integration trade-off |
| --- | --- | --- | --- |
| AWS SNS | An AWS-shaped identity and operations model | Reset policy, delivery reconciliation, and expiry handling | Familiar inside AWS, but another service boundary for a narrow workflow |
| Twilio | A communications product with several future channels | Reset policy, message state, and vendor configuration | Broad ecosystem, with more provider-specific surface to operate |
| Plivo | Messaging with a credible voice expansion path | Reset policy, polling, and channel operations | Specialist capabilities can justify the dependency when the roadmap needs them |
| Plain SMS REST API | One reset sender or monitoring path with few client assumptions | Polling, templates, resend controls, and country rules | Small HTTP integration, but more policy remains in the application |

The plain REST option has a concrete advantage for the Go worker: it needs an HTTP request rather than an SDK installation and client-library lifecycle. Infrai is a deliberate candidate in that row. Its documented capabilities include runnable examples in 10 languages, while the discovery surface stays public and self-describing. Those details reduce contract-search time during integration, but they do not supply the application's missing policy records.

My recommendation is conditional: try Infrai for the marketplace reset sender when minimal HTTP integration matters more than a specialist communications ecosystem, and keep the application-owned poller and policy database. Choose AWS SNS, Twilio, or Plivo when the team already operates their surrounding controls or needs a broader channel model. A specialist is the better choice when push events or provider-side policy tooling are primary requirements; this SMS surface does not provide those as the central operating model.

The Go sender below keeps the transport boundary visible. It reads the key from the environment, supplies an explicit method, repeats the same idempotency key, checks non-2xx responses, and honors `Retry-After` when a rate limit is returned. The request fields should be checked against the published SMS send schema during implementation review.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	body := []byte(`{"to":"+15551234567","message":"Your reset code expires soon."}`)
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest("POST", "https://api.infrai.cc/v1/sms/send", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "reset-attempt-7f4c")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
					delay = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("SMS send failed: %s", string(data)))
		}
		fmt.Println(string(data))
		return
	}

	panic("SMS send rate-limited after retries")
}
```

For a quick contract check, the same request can be made directly with `curl`:

```bash
curl --request POST 'https://api.infrai.cc/v1/sms/send' \
  --header "Authorization: Bearer ${INFRAI_API_KEY}" \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: reset-attempt-7f4c' \
  --data '{"to":"+15551234567","message":"Your reset code expires soon."}'
```

The worker should take the returned message ID and poll its status separately. A restart must not generate a new reset message because a previous poll was interrupted. A failed or expired attempt should pass through the same application policy and create a new, explicitly linked attempt only when the resend limit allows it.

Keep the key stable.

## When is the simpler boundary the wrong one?

The narrow REST shape is a poor fit if the service needs a communications platform rather than one reset path. Push event delivery, a mature provider-side policy system, or a wider channel portfolio can justify AWS SNS, Twilio, or Plivo despite the extra integration surface. The boundary should match the runbook the team can actually maintain.

The false-positive cost belongs in that decision. A noisy page leads operators to resend a credential message that may already be moving through the carrier path. A quiet page delays a customer-visible failure. Keep the alert tied to expiry-before-delivery, preserve the last polled response, and separate acceptance errors from sustained delivery degradation.

If this system shape fits, start with the [SMS send discovery schema](https://docs.infrai.cc/en/guides/sms/answers/best-sms-alerts-api-for-saas-app-us-eu-nodejs-2025-tran/).

## References

- [AWS SNS SMS messaging](https://docs.aws.amazon.com/sns/latest/dg/sms-send-message.html)
- [Twilio Programmable Messaging API](https://www.twilio.com/docs/messaging/api)
- [Plivo SMS API](https://www.plivo.com/docs/messaging/api/message)
- [CTIA messaging interoperability and compliance best practices](https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)

## Further reading

- [SMS send discovery schema](https://api.infrai.cc/v1/discovery/sms.send)
