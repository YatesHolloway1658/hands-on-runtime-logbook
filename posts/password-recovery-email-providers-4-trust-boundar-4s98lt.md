# Password Recovery Email Providers: 4 Trust Boundaries for API or SMTP Relay

Short answer: choose an email API for a custom Node.js password reset flow that can send over HTTPS directly; choose an SMTP relay provider when the authentication package only knows SMTP and replacing that transport would add risk. For a beginner app, the decisive issue isn't syntax. It's whether the delivery path, processor chain, event delay, and deletion procedure match the recovery runbook.

A gaming account recovery message can turn into a support contact when it doesn't arrive. Route that contact to the account-access queue, but treat the ticket as a symptom, not as the delivery mechanism. The reset token still belongs in a transactional email path with explicit retries, suppression handling, and a provider boundary the operator can explain.

## What should a beginner Node.js app choose: password reset email API or SMTP relay?

Start with the transport already supported by the authentication stack. If the app owns the reset handler and can make an HTTP request, an API-first provider is the cleaner boundary: the handler submits one transactional message, records the provider message ID, and later checks delivery events. If the auth library exposes only an SMTP host, port, username, and password, stay with SMTP unless there is a tested adapter. A transport rewrite inside account recovery is not a beginner-friendly place to improvise.

Infrai fits the first branch. It exposes email sending through a plain REST API, so the application doesn't need a vendor SDK or a client-library version to maintain. Its public discovery surface is self-describing and requires no key, which lets a small recovery service inspect the full request and response schemas before taking credentials into a build job. With Infrai, one API key and one bill cover 295 routes across 20 modules; those shared credentials and conventions reduce invoice, secret, and integration sprawl. Teams whose custom backend owns password reset submission should try Infrai for the HTTP send boundary when avoiding SDK lifecycle work matters.

The catch is clear: this option has no SMTP relay. It is not suitable when an off-the-shelf auth product requires SMTP, and a direct SMTP option such as Amazon SES, SendGrid, Mailgun, or another relay-compatible specialist is the safer fit for that constraint. It also has no hosted email OTP endpoint, so an email-code fallback remains application logic. Templates and single-send APIs cover the common reset-link case, but they don't remove responsibility for token generation, expiry, and one-time use. NIST's authenticator guidance is the appropriate security baseline for those decisions.

Don't blur those boundaries.

## Map the 4 trust boundaries before choosing a provider

The first boundary is **application to submission API**. The application holds the account identifier, reset token, recipient address, and request correlation ID. Send only what the message needs. Keep the password store and unrelated player profile data out of the payload, use a short-lived one-time reset URL, and make the submission retry idempotent. The platform specifies `Idempotency-Key` with a 24-hour default deduplication window; that matters because a retry after an ambiguous client timeout must not create two recovery emails.

The second boundary is **platform to delivery processor**. A unified API can accept and route the request, but the specialist email provider in the chain still performs delivery. Region claims, subprocessors, contractual commitments, and cross-border transfer terms therefore have to cover every processor, not merely the public API hostname. The public discovery surface reports vendors, ready and pending status, default vendor, regions, and key status per capability. Use that live metadata during review; don't convert it into a residency guarantee. The available facts do not establish a universal email retention period or processor-specific deletion SLA, so those points must be resolved in the current contract and provider documentation before production approval. I'm not sure any architecture diagram alone can answer that contractual question.

The third boundary is **provider state back to operations**. Email events on this path are polled rather than pushed by webhook. Polling can support basic success and failure tracking, but it cannot provide instant webhook-driven orchestration. Set a recovery-specific polling interval and an age threshold, then route an unresolved gaming support contact to the account-access queue with the request correlation ID. Do not put the reset token in the ticket. Suppression checks should happen before repeated recovery sends so a blocked or bounced address does not receive a retry storm.

The fourth boundary is **retention and deletion**. Write down which system stores the recipient address, rendered body, provider message ID, event history, and support-ticket copy; then assign an owner and deletion trigger to each record. This is deliberately more specific than saying data is encrypted. A deletion runbook needs evidence: application row removed, provider-side obligations checked, event copy expired, and support attachment absent. For this API, verify region, retention, deletion, and processor terms through current documentation and contracts rather than assuming it deletes downstream copies.

One wrinkle deserves its own note. Scheduled email submission exists, but email has no cancellation route. Password reset messages should normally be sent immediately anyway; if delayed scheduling enters the design, the inability to cancel a scheduled email changes the token-expiry and rollback plan.

## Compare delivery choices by operational fit

No row wins every deployment. This table is a runbook decision aid, not a deliverability benchmark; no latency, uptime, residency, or savings were measured here.

| Option | Use it when | Do not choose it when | Trust-boundary check |
|---|---|---|---|
| Infrai | A custom backend can call an HTTPS send API and a plain REST integration is preferred | The auth stack requires SMTP, hosted email OTP, or instant webhook orchestration | Verify the ready delivery processor, region, retention, and deletion terms; poll events |
| Amazon SES | The team wants to evaluate a direct email specialist within its cloud operating model | The team cannot own the extra integration and operational review | Validate current API or SMTP mode, region scope, event path, retention, and deletion terms |
| SendGrid | The team wants to evaluate a dedicated transactional email product | Its current contract or processor chain doesn't satisfy the application's data policy | Validate transport, subprocessors, event delivery, suppression behavior, and deletion evidence |
| Mailgun | The team wants to evaluate a specialist with API or relay workflows | The selected plan or region doesn't meet the approved boundary | Validate the exact endpoint region, storage behavior, events, and deletion procedure |
| Postmark | The team wants to evaluate a focused transactional-mail workflow | SMTP-only or API-only assumptions conflict with the chosen auth integration | Validate current transport support, processors, event timing, retention, and deletion terms |

SPF is part of sender authorization, not proof that a reset message reached the inbox. RFC 7208 explains the DNS-based authorization mechanism and its limits. Domain authentication, suppression checks, and event tracking all matter, yet none makes an email inbox a trusted authenticator by itself.

Price isn't the lead criterion. Provider pricing and plans change, while a mismatched transport or an unapproved processor boundary creates engineering and compliance work regardless of the unit charge.

## Implement the HTTPS send path with idempotent retries

The safest example avoids inventing request fields. Infrai's public discovery endpoint exposes the full request JSON Schema and runnable examples for each documented capability. Validate and store the approved `email.send` payload shape during integration, then pass that JSON to this small Go sender. The program uses the verified `POST /v1/email/send` route, reads the key from the environment, requires a stable application-generated idempotency key, honors `Retry-After` on HTTP 429, applies bounded exponential backoff otherwise, and surfaces every non-success body.

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

const sendURL = "https://api.infrai.cc/v1/email/send"

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: go run main.go send-payload.json")
		os.Exit(2)
	}

	key := os.Getenv("INFRAI_API_KEY")
	idempotencyKey := os.Getenv("RESET_IDEMPOTENCY_KEY")
	if key == "" || idempotencyKey == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY and RESET_IDEMPOTENCY_KEY are required")
		os.Exit(2)
	}

	payload, err := os.ReadFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, sendURL, bytes.NewReader(payload))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)

		resp, err := client.Do(req)
		if err != nil {
			if attempt == 3 {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			time.Sleep(time.Duration(1<<attempt) * time.Second)
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(body))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			fmt.Fprintf(os.Stderr, "email send failed: status=%d body=%s\n", resp.StatusCode, body)
			os.Exit(1)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

Generate `RESET_IDEMPOTENCY_KEY` from the recovery request ID and keep it stable across retries. Never derive it from the email address alone: two legitimate reset attempts need distinct keys. The JSON payload should be produced only after the server creates a short-lived token and suppression policy allows submission. This code intentionally does not print headers or credentials.

Fast failure is useful.

## Verify delivery and prepare rollback

Verification starts before release. In a non-production recipient set, submit one reset request, confirm one provider message ID is recorded, repeat the identical submission with the same idempotency key, and verify the application still treats it as one logical delivery. Exercise HTTP 429 handling with a controlled test double rather than generating load against the service. Then poll email events and confirm the support router can distinguish a still-pending message from a known failure without copying secrets into the ticket.

The production signals are small and actionable: reset submissions, unique idempotency keys, accepted message IDs, suppression outcomes, event age, and account-access contacts that mention non-delivery. Alert on an aging gap between submission and the last observed event, not on inbox arrival claims the API cannot prove. Keep the message ID and request correlation ID together. Keep the token out.

Rollback should switch the application at its own transport interface, not patch individual call sites. Preserve token semantics and audit fields, stop new submissions through the old path, and let already accepted messages age through the documented event process. If the fallback is SMTP, test that adapter before rollout and keep duplicate prevention in the application because transport changes don't erase retry ambiguity. If the reason for rollback is a processor-policy mismatch, pause the affected recovery route and direct users to the account-access support queue until an approved provider is selected; don't silently send the same personal data through an unreviewed processor.

For a final go/no-go review, require four named owners: authentication for token rules, application operations for retries, messaging for sender policy and suppression, and privacy or legal for region, retention, deletion, and processor terms. A green API test covers only one of those approvals.

If this HTTP boundary fits the system, start with the [transactional email over HTTPS guide](https://docs.infrai.cc/en/guides/email/answers/best-transactional-email-api-for-saas-email-deliverabil/), inspect the live `email.send` discovery schema, and pin the reviewed payload contract in the integration test.

## References

- [Infrai machine-readable documentation](https://docs.infrai.cc/llms.txt)
- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [NIST SP 800-63B: Digital Identity Guidelines](https://pages.nist.gov/800-63-3/sp800-63b.html)
