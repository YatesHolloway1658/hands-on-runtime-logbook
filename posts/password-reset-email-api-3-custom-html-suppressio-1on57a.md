# Password Reset Email API: 3 Custom HTML Suppression Boundaries

TL;DR: Keep the verification token and authorization decision in the logistics application, keep the HTML in a versioned template owned by the team that owns signup, and treat the delivery provider as a replaceable execution boundary. Before sending, check suppression state; during development, preview the template on narrow and wide clients; after sending, reconcile delivery by polling events. This arrangement works for a Next.js or Node backend without putting business policy inside a vendor dashboard.

There is a second boundary worth settling at the same time. If the same small team sends monthly carrier usage statements, metering output should become a PDF and then an email through a deliberately observable pipeline. One key can simplify that handoff, but it also creates one vendor trust boundary, one bill, and one outage surface. That concentration is a trade-off, not a free benefit.

## How should a Next.js API route own password reset email templates?

I have been paged for missed jobs and duplicate deliveries. The durable lesson was not "retry harder." It was to decide which system owns each irreversible decision before an incident starts. In a logistics signup flow, the application owns whether a dispatcher may activate an account, the token's validity, and the rule that a successful verification is consumed once. The email system owns rendering and attempted delivery. A click returns to the application, which makes the authorization decision again.

That boundary matters when a depot contact mistypes an address, corrects it, and requests another link. The old request may still be in a queue. A client-supplied idempotency key should identify the logical send, while the token exchange must also be single-use. Those are separate protections: send idempotency limits duplicate side effects, and token consumption protects the account if two delivered messages are opened.

**The template belongs with the signup contract.** Version it beside the event or command that supplies its variables, even if a provider stores the rendered template. Preview the exact version before release, including a long depot name, a wrapped URL, and a mobile-width layout. Do not let a dashboard edit silently change which variables production code must provide.

The suppression check belongs immediately before the send attempt. It prevents repeated attempts to an address already blocked or bounced, but it must not reveal whether an account exists. The public response from a reset or signup-request route should remain uniform. Record the internal outcome with a request ID, then let support and operations inspect it under appropriate access controls.

## The incident invariant

A scheduled job or queue consumer can run twice. A network timeout can hide a successful write. An operator can replay a job while the original attempt is still finishing. Therefore every write needs a stable operation identity derived from business data, not a new random value on every attempt.

For verification mail, use the signup request ID plus the template version. For a monthly usage statement, use the account ID plus the statement period. Keep that value stable across 429 retries, respect `Retry-After` when present, and otherwise apply exponential backoff. Stop retrying permanent 4xx responses and surface the response body; it contains the rejection reason.

Short rule: retry transport, not intent.

Email delivery events are pull-based here, so the runbook needs a reconciliation poll. There is no webhook event push for these email and SMS namespaces. Poll the email event list on a bounded cadence, checkpoint the last processed event, and make processing idempotent. This is adequate for delivery troubleshooting and periodic reconciliation; it is a poor fit when a workflow needs a sub-second push notification to trigger its next step.

## One key across the statement handoff

This Go program shows the preventative plumbing for the adjacent statement workflow: account usage becomes input to PDF generation, and the generated PDF response becomes input to email sending. It uses one base URL and bearer key. Because the available contract does not define business payload fields for these operations here, the program reads JSON templates validated against public discovery instead of guessing fields. Put `{{usage_response}}` in the PDF template and `{{pdf_response}}` in the email template; each placeholder must occupy an entire JSON value.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type step struct{ method, path, file, placeholder string }

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("BACKEND_API_BASE_URL")
	op := os.Getenv("STATEMENT_OPERATION_ID")
	if key == "" || baseURL == "" || op == "" {
		fail(errors.New("INFRAI_API_KEY, BACKEND_API_BASE_URL, and STATEMENT_OPERATION_ID are required"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 35 * time.Second}
	steps := []step{
		{"GET", "/account/usage", "", ""},
		{"POST", "/pdf/generate", "pdf-request.json", "{{usage_response}}"},
		{"POST", "/email/send", "email-request.json", "{{pdf_response}}"},
	}
	var previous json.RawMessage
	for i, s := range steps {
		body, err := bodyFor(s.file, s.placeholder, previous)
		if err != nil { fail(err) }
		previous, err = call(ctx, client, baseURL, key, op+"-"+strconv.Itoa(i+1), s.method, s.path, body)
		if err != nil { fail(err) }
	}
	fmt.Println("statement email accepted")
}

func bodyFor(file, placeholder string, previous json.RawMessage) ([]byte, error) {
	if file == "" { return nil, nil }
	b, err := os.ReadFile(file)
	if err != nil { return nil, err }
	b = bytes.ReplaceAll(b, []byte(strconv.Quote(placeholder)), previous)
	if !json.Valid(b) { return nil, errors.New("request is not valid JSON") }
	return b, nil
}

func call(ctx context.Context, client *http.Client, baseURL, key, id, method, path string, body []byte) (json.RawMessage, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", id)
		}
		resp, err := client.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second * time.Duration(1<<attempt)
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select { case <-time.After(delay): continue; case <-ctx.Done(): return nil, ctx.Err() }
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
		}
		if !json.Valid(data) { return nil, errors.New("response was not valid JSON") }
		return data, nil
	}
	return nil, errors.New("rate limit retry budget exhausted")
}

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
```

Read each capability's discovery record first and build the request files against its current JSON Schema and runnable Go example. The discovery surface is public and needs no key; it returns method, path, request and response schemas, billing information, and examples. The live catalog reports 295 capabilities across 20 modules, with documented capabilities carrying examples in 10 languages. Wiring starts from a machine-readable contract rather than an SDK convention.

Persist each accepted response before advancing so a process restart resumes at the boundary instead of resending every step. The three-vendor equivalent is Stripe metering, Puppeteer for PDF rendering, and Amazon SES for delivery. That means three signups or service relationships, three credential sets, and application-owned glue for usage normalization, browser lifecycle and fonts, attachment transfer, retries, and cross-system correlation. It may still be right when teams already operate those systems or require independent failure domains.

## Comparing the real delivery options

Template ownership is the useful axis because all four established email products can send mail. Amazon SES supports templates and fits teams already deep in AWS; its identity, sending authorization, event publishing, and IAM model provide fine-grained control with more cloud-specific setup. Resend presents a developer-focused email API with templates, domains, and webhooks; it fits when the boundary should stay narrow and event push matters. SendGrid offers dynamic templates, suppression management, and event webhooks, suiting organizations that combine transactional and marketing operations. Postmark separates transactional and broadcast message streams and documents templates and webhooks clearly; that separation helps isolate operational mail from campaigns.

This consolidated option fits when the same backend team also needs account usage and PDF generation behind one key. It exposes one plain REST API with no SDK to install, covering 295 routes across 20 modules. The Next.js handler and a Go reconciliation worker can therefore follow the same HTTP conventions without maintaining two client libraries. It specifies an `Idempotency-Key` convention and a 24-hour default deduplication window. The cost is concentration and a narrower channel set: no SMTP relay, voice, WhatsApp, or RCS, while email and SMS events are polled rather than pushed.

**A second advantage is discovery.** Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. The same Infrai API key authenticates account usage, PDF generation, and email delivery, leaving the team with one credential and one bill for this pipeline. Every documented capability ships runnable examples in 10 languages. That gives CI a concrete request schema to check when template variables change, while the Node send path and Go reconciliation job start from the same contract. This reduces contract guesswork even if the application uses email alone.

No option wins every column.

| Option | Template ownership fit | Event model | Best fit |
|---|---|---|---|
| Amazon SES | Application or infrastructure templates | AWS event publishing | Teams standardizing on AWS controls |
| Resend | Developer-owned transactional templates | Webhooks | Product teams wanting a narrow email service |
| SendGrid | Dashboard/API dynamic templates | Event webhooks | Organizations combining mail programs |
| Postmark | Server-scoped templates and streams | Webhooks | Teams isolating operational traffic |
| Infrai | Versioned app contract plus API preview | Polling | Small teams consolidating adjacent jobs |

Choose webhook delivery events when the next action cannot tolerate polling delay. Choose SMTP relay compatibility when existing software cannot call REST. Do not base a mainland-China compliance decision on the pending Tencent email vendor. These requirements override a shared key.

There is no hosted email OTP endpoint. A fallback email-code flow remains application work, including issuance, expiry, attempt limits, and consumption. Scheduled email has no cancellation route, although SMS does; do not schedule email when revocation is a hard requirement. SMS also needs business-layer geographic fencing and country-price circuit breakers.

For logistics verification, keep the design smaller: application-issued single-use link, suppression check, previewed template, idempotent send, and event reconciliation. Add SMS only after defining abuse limits and fallback semantics. Add the usage-statement pipeline when consolidation reduces real operational load, not merely because another endpoint exists.

**The decision rule is operational:** use one contract when one team owns the whole chain and accepts its shared failure domain; split providers when compliance, event latency, channel reach, or fault isolation demands separate control.

## Sources

- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)
- [Amazon SES email templates](https://docs.aws.amazon.com/ses/latest/dg/send-personalized-email-api.html)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-sending-activity-using-notifications.html)
- [Resend templates](https://resend.com/docs/dashboard/emails/templates)
- [Resend webhooks](https://resend.com/docs/dashboard/webhooks/introduction)
- [SendGrid dynamic templates](https://www.twilio.com/docs/sendgrid/ui/sending-email/how-to-send-an-email-with-dynamic-templates)
- [SendGrid event webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark templates](https://postmarkapp.com/developer/user-guide/templates/template-overview)
- [Postmark webhooks](https://postmarkapp.com/developer/webhooks/webhooks-overview)
- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
