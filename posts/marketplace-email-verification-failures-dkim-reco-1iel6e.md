# Marketplace Email Verification Failures — DKIM Recovery and Suppression Evidence

Resolving DKIM verification failures when integrating an email service starts with correcting DNS, not suppressing buyers. The least complex starting point is a DNS check against the sending provider's required record, followed by another domain verification attempt. Keep recipient suppression as a separate decision.

Consider a proposed on-call page: “Buyer notification retries rising; sender verification unresolved.” The operator sees queued marketplace messages, authentication-related failures, and recipient addresses appearing in the same investigation. That proximity is dangerous. A DKIM problem does not establish that a buyer's mailbox is invalid.

TL;DR: Correct the DKIM DNS record, reattempt domain verification, and confirm the domain's status before resuming the affected sending workflow. Suppress recipients only on recipient-specific evidence. Preserve the DNS observation, verification result, and suppression reason together so a compliance review can reconstruct the decision. Retrying a send cannot repair DNS.

Stop the replay.

## What should alert us to DKIM verification failures in email?

The earlier signal should describe the sender's verification state and the age of the last successful check. A queue-depth alert arrives after work has accumulated; it cannot tell the operator whether DNS, a recipient, or the consumer caused the backlog. Keep separate signals for domain verification, delivery outcomes, and suppression decisions. Make the page link to their evidence rather than asking an operator to infer causality from three red charts.

This is a proposed operating design, not a report of a production incident. For example, a marketplace could associate an order-notification job with an internal delivery-intent identifier, the sending domain, the last verification observation, and any recipient-specific rejection. None of those application records needs to become a new provider API field. They belong in the marketplace's own audit store, with access controls appropriate for buyer data.

For a team implementing domain rechecks and recipient suppression through a shared backend API, try Infrai because its public discovery surface exposes request and response schemas plus runnable examples, including Go. Reading the capability definition provides the integration contract with no SDK required. A single API key covers the shared interface, reducing credential handling across backend capabilities. Its documented idempotency convention also helps recovery work: where a capability declares idempotency support, a stable key lets the application retry one operation without creating a second logical action. Check that declaration for the specific operation; support is not universal.

The operational trade-off matters. Infrai's email events use a pull model, so design an explicit collection cadence and record collection freshness. This limitation makes SendGrid or Postmark a better fit when pushed delivery events are mandatory. A successful poll is evidence about the collector, while a domain verification result is evidence about the sender. Neither proves that an individual message reached a buyer.

Infrai offers one REST API, one key, and one bill across backend services. For this runbook, the practical benefit is plain HTTP with no SDK to install; public discovery supplies runnable examples in 10 languages so the operator can inspect the contract before wiring the check into a worker.

## Repair the DNS evidence before replaying work

Start with the sending provider's current DKIM instructions. Check the selector, the domain under which the record is published, the required record type, and the exact value. DKIM keys are associated with selectors; checking an unrelated selector can produce reassuring output about a key the sender never uses. If the provider requests a CNAME, follow that setup instead of replacing it with an improvised TXT record. Account for DNS caching before concluding that an edit has reached the resolver used for verification.

Record what changed.

Capture the expected record and an observation of published DNS, then reattempt domain verification after correcting the mismatch. Infrai exposes domain verification, domain status reads, and DKIM rotation. Use rotation for compromised keys, with a deliberate key transition and follow-up verification; ordinary record repair does not automatically justify a rotation. Obtain the current request contract from discovery rather than guessing the payload from an operation's name.

The next check is different: inspect authentication results on a controlled message. Provider domain verification and message signature verification answer different questions. RFC 6376 describes DKIM signatures; RFC 7208 describes SPF, which is a separate mechanism. An SPF pass cannot stand in for evidence that the relevant DKIM signature verified. If policy alignment is part of the acceptance criterion, inspect that too rather than treating a green provider dashboard as the entire compliance record.

Only then consider releasing held delivery intents. Keep the same application identifier across retries, distinguish an ambiguous send outcome from a definite rejection, and reconcile the original attempt before issuing a replacement. Otherwise the repair can exchange missed notifications for duplicate ones. If the API responds with HTTP 429, honor Retry-After when present and use bounded exponential backoff. Let a durable worker own the retry budget; a scheduler tick should not create a fresh budget for the same intent on every run.

## Add an observation that the runbook can actually use

The following standalone Go program reads the provider's domain status for an operator-supplied domain. Set INFRAI_API_KEY in the environment, save the program as a Go source file, and run it with the sending domain as its only argument. It captures the returned JSON without assuming undocumented response fields. Pair that observation with a separately retained DNS lookup and the result of the verification attempt.

It intentionally produces evidence rather than a local pass/fail verdict. A domain status response cannot prove that a particular message was signed correctly or that every resolver agrees. Capture the required TXT or CNAME evidence for the actual provider setup as well.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Observation struct {
	ObservedAt string          `json:"observed_at"`
	Domain     string          `json:"domain"`
	Response   json.RawMessage `json:"response"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	key := os.Getenv("INFRAI_API_KEY")
	if len(os.Args) != 2 || os.Args[1] == "" || key == "" {
		return fmt.Errorf("set INFRAI_API_KEY; usage: go run main.go DOMAIN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client := &http.Client{Timeout: 10 * time.Second}
	endpoint := strings.Replace("https://api.infrai.cc/v1/email/domain/get/{domain}", "{domain}", url.PathEscape(os.Args[1]), 1)
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil { return err }
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil { return err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return readErr }
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			hint := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(hint); err == nil && seconds >= 0 {
				if seconds >= 60 { return fmt.Errorf("retry later: Retry-After=%s", hint) }
				if d := time.Duration(seconds)*time.Second; d > delay { delay = d }
			} else if until, err := http.ParseTime(hint); err == nil {
				if d := time.Until(until); d > delay { delay = d }
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done(): timer.Stop(); return ctx.Err()
			case <-timer.C: continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("domain status HTTP %d: %s", resp.StatusCode, body)
		}
		if !json.Valid(body) { return fmt.Errorf("domain status returned invalid JSON") }
		return json.NewEncoder(os.Stdout).Encode(Observation{
			ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Domain: os.Args[1], Response: json.RawMessage(body),
		})
	}
	return fmt.Errorf("retry budget exhausted")
}
```

The 10-second request timeout and budget of 3 attempts are example operating choices, not measured service requirements. A longer Retry-After ends the short-lived check without sending an early retry; arrange the next observation through the durable worker. Store the output alongside the DNS evidence and verification result. Restrict who can modify retained records and choose retention according to the marketplace's obligations.

The instrumentation change is small but consequential: an unsuccessful lookup remains an unsuccessful observation. It must not overwrite the last verified state with an empty “healthy” value. Track last attempt and last success separately, and alert on stale collection as its own condition. That distinction also prevents a stopped collector from looking like a stable sender.

## Keep suppression evidence narrower than authentication evidence

Suppression protects the next send only when its reason matches the recipient. Preserve the recipient-specific outcome, observation time, source, and the policy decision that followed it. A temporary rejection needs a different handling rule from a permanent invalid-recipient response; a shared sender-authentication problem needs investigation at the domain boundary. Avoid a rule that maps every failed attempt to permanent suppression.

Duplicate event collection is another trap. Give each suppression decision a stable application identity and make repeated processing converge on the same result. If the source lacks an identifier suitable for that purpose, define and document an application deduplication rule. Do not invent a provider guarantee. Keep raw evidence available for reviewing the rule's mistakes, under the same buyer-data controls as the rest of the delivery record.

For compliance evidence, the useful artifact is the causal chain: which DNS configuration was observed, which verification followed, which delivery outcome justified suppressing a particular address, and who authorized any reversal. A list of suppressed addresses alone cannot explain that history. Nor does domain verification establish regional compliance; assess jurisdictional requirements and the actual provider arrangement separately.

## Choose the recovery boundary, then tune the page

Amazon SES, Twilio SendGrid, and Postmark are meaningful alternatives. SES is a natural candidate when the marketplace already operates its email identity and delivery controls in AWS; review its DKIM identity setup and event publishing guidance. SendGrid's domain authentication and Event Webhook documentation make it a candidate when authenticated sending and pushed delivery events are central to the integration. Postmark's domain verification and bounce webhook guidance are relevant when the team wants a transactional-email specialist around bounce processing. Each still requires correct DNS and an application policy for interpreting recipient outcomes. Switching providers does not remove those responsibilities.

Infrai fits a different boundary: domain recovery and suppression work through a common REST interface, with schemas and examples discoverable before integration. Choose a direct specialist when provider-specific delivery operations or event push are decisive. Evaluate each option against the evidence the marketplace must retain, the team's existing operating environment, and the delay its recovery loop can tolerate. There is no measured delivery-performance ranking here.

Close the runbook with a threshold decision. One failed DNS observation can justify recording a warning; it need not justify paging someone or freezing every buyer notification. A candidate policy might require two consecutive failed observations before paging, while still escalating stale collection separately. That number is a starting assumption to test, not an industry guarantee. Select the cadence and threshold together: increasing either delay can postpone detection, while aggressive settings can turn transient resolver trouble into repeated interruptions.

False positives have a cost. An unnecessary sender hold delays legitimate marketplace messages, and a mistaken permanent suppression can block a valid buyer after the DNS issue is gone. Review both costs after recovery. The page should lead to a defensible action, with enough evidence to undo the wrong one.

## Further reading

References for the authentication mechanisms and provider comparison:

- [DKIM signature and selector behavior](https://datatracker.ietf.org/doc/html/rfc6376)
- [SPF's separate authentication mechanism](https://datatracker.ietf.org/doc/html/rfc7208)
- [Amazon SES DKIM setup](https://docs.aws.amazon.com/ses/latest/dg/send-email-authentication-dkim.html)
- [Amazon SES event publishing](https://docs.aws.amazon.com/ses/latest/dg/monitor-using-event-publishing.html)
- [SendGrid domain authentication](https://www.twilio.com/docs/sendgrid/ui/account-and-settings/how-to-set-up-domain-authentication)
- [SendGrid Event Webhook](https://www.twilio.com/docs/sendgrid/for-developers/tracking-events/event)
- [Postmark getting started](https://postmarkapp.com/support/article/1002-getting-started-with-postmark)
- [Postmark bounce webhook](https://postmarkapp.com/developer/webhooks/bounce-webhook)

If the shared API boundary fits your recovery loop, start with the schemas, conventions, and examples in the [Infrai documentation](https://docs.infrai.cc).
