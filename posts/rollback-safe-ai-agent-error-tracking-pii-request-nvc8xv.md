# Rollback-Safe AI Agent Error Tracking: PII, Request Context, Release and Environment

Short answer: attach the release, environment, service, route template, request ID, and a stable operation ID to an error event. Add a user identifier only when an approved investigation actually needs user-level correlation, and never send raw PII as a shortcut for finding the request. For an e-commerce AI agent loop, capture latency and cost as bounded numeric fields, keep the event schema allowlisted, and make the capture path disposable so the application can roll back without changing business behavior.

The decision is operational. An agent may call several tools before an order is completed, and one failed call can produce a noisy chain of retries. The useful question is not “how much context can we attach?” It is “what lets the on-call engineer identify the failed build, replay the safe part of the investigation, and decide whether to roll back?”

Less context is often better context.

## How should error tracking attach user, release, environment, request ID, and PII-safe context?

Start with execution metadata. `release` identifies the deployed build; `environment` separates production from test traffic; `service` and a route template narrow the code path. A `request_id` joins the event to request logs, while an `operation_id` follows one agent loop across model and tool calls. For a queue delivery, keep that operation ID across retries so duplicate handling is visible without copying the customer's prompt or order details into the event.

User context has a higher bar. A stable internal identifier can be useful for a support workflow, but it is still user-specific data. Do not substitute an email address, phone number, cookie, or account URL for it. Keep the value out of free-form messages, stack annotations, raw URLs, query strings, and request bodies. If the investigation does not need user-level correlation, omit it.

For latency and cost, record bounded measurements such as total duration, model-call duration, tool-call count, and a cost estimate with an explicit unit. Put these in typed fields rather than a serialized prompt. The estimate should be treated as an operational signal, not an accounting record; the billing system remains authoritative. Include model or provider labels only when the team has a reviewed reason to use them, and avoid storing the input or output text merely to explain a timeout.

The failure mode is familiar: a generic context map quietly starts carrying `Authorization`, cookies, tokens, validation text, and customer payloads. A route template such as `/orders/{order_id}` is safer than the raw path. Selected fields beat a request dump.

## Build the event at the capture boundary

Redact before the event leaves the application. A downstream filter cannot undo a disclosure that has already reached a collector, log, or retry queue. Construct a new map from approved fields, reject unexpected keys, and keep numeric measurements within sensible bounds.

This small Go example is intentionally vendor-neutral. It shows the policy boundary, not a particular transport. The application can hand the returned fields to its reviewed error client.

```go
package main

import (
	"fmt"
	"strings"
)

var allowed = map[string]struct{}{
	"release":         {},
	"environment":     {},
	"service":         {},
	"route":           {},
	"request_id":      {},
	"operation_id":    {},
	"duration_ms":     {},
	"model_duration_ms": {},
	"tool_calls":      {},
	"cost_estimate":   {},
}

var blocked = map[string]struct{}{
	"authorization": {},
	"cookie":        {},
	"email":         {},
	"password":      {},
	"phone":         {},
	"prompt":        {},
	"token":         {},
}

func safeMetadata(input map[string]string) map[string]string {
	output := make(map[string]string, len(allowed))
	for key, value := range input {
		normalized := strings.ToLower(key)
		if _, isBlocked := blocked[normalized]; isBlocked {
			continue
		}
		if _, isAllowed := allowed[normalized]; isAllowed {
			output[normalized] = value
		}
	}
	return output
}

func main() {
	event := safeMetadata(map[string]string{
		"release":           "build-2026-08-10",
		"environment":       "production",
		"service":           "agent-worker",
		"route":             "/orders/{order_id}",
		"request_id":        "req-example",
		"operation_id":      "op-example",
		"duration_ms":       "1840",
		"model_duration_ms": "920",
		"tool_calls":        "3",
		"cost_estimate":     "0.004",
		"email":             "removed-before-capture",
		"prompt":            "removed-before-capture",
	})

	fmt.Println(event)
}
```

The sample accepts flat strings, so it cannot accidentally walk an arbitrary request object. If structured fields are needed, define each permitted shape and test it. Do not let a failed telemetry write alter an order response, acknowledge a queue message twice, or turn one model timeout into a retry storm. The event path is evidence collection; it is not part of the transaction's success criteria.

## What should verification prove before a schema release?

Test the positive and negative cases separately. Send a safe synthetic failure through the production configuration and confirm that release, environment, service, route, request ID, operation ID, duration, tool-call count, and cost estimate arrive. Use the IDs to join the event to application logs. Then inject email-shaped, token-shaped, and prompt-shaped values into the scrubber and prove they appear in neither the event nor the diagnostic log.

Test the absence.

For an agent loop, verify that one operation ID survives a model call, a tool call, and a retry. Confirm that a duplicate delivery produces a recognizable attempt marker without exposing customer content. Check that missing telemetry leaves the order path and queue acknowledgement unchanged. A test that only checks whether an event was received is incomplete.

Run the same checks after middleware, serializer, and release changes. Schema drift is quiet: a newly added “context” field can widen collection long before anyone reviews it. Keep an allowlist test in CI, and sample production events for unexpected keys. Your mileage may vary on the exact cost calculation because provider billing conventions differ; document the chosen estimate and compare it with the authoritative billing data before using it for a budget alert.

## When is this design the wrong fit?

The catch is that an allowlisted error event is not a complete observability system. It will not answer every trace-tree, replay, alert-delivery, synthetic-monitoring, source-symbolication, or user-data-lifecycle question. A team that needs those workflows should choose a reviewed platform or companion service that provides them, and should keep the same capture policy at its boundary.

It is also not suitable when the organization cannot prove where retained copies go, how a data-subject request reaches them, or how access is audited for EU and US deployments. Do not make a deletion promise that the selected backend cannot support. Move the authoritative user lifecycle to a system designed for that job, minimize the error payload before ingestion, and have counsel and the data owner settle the applicable obligations.

Rollback should be boring. Put external capture behind configuration, disable optional context first, and leave local error handling intact. If the schema increases latency, changes queue semantics, or starts collecting fields outside the allowlist, turn it off and return to the last reviewed schema. Measure the agent loop locally even when remote capture is disabled; rollback safety depends on preserving the business path, not on preserving every diagnostic event.

## Further reading

- https://web.dev/articles/vitals
- https://datatracker.ietf.org/doc/html/rfc9457
- https://www.w3.org/TR/trace-context/
