# Text and Image Moderation with One API Key for Candidate Rubrics (and a Queue)

**Short answer:** Use one authenticated moderation boundary for text and image inputs, return structured decisions, and queue anything uncertain before it reaches candidate scoring.

A candidate-scoring service should make one conservative decision first: is this submission eligible to reach the rubric scorer? Put text and image checks behind a policy gate, return a typed decision, and send uncertain cases to a durable queue. That is the simplest architecture that keeps a hostile upload from becoming a model prompt, while keeping an ambiguous case from silently becoming a rejection.

The important boundary is not one API key. It is the contract around the key: authenticated intake, cheap deterministic checks, normalized media, structured moderation output, and an explicit review state. The same boundary can handle cover letters, profile avatars, and portfolio uploads for a fintech hiring workflow without pretending that a model's confidence is a hiring decision.

## The page fires before the candidate score

The page arrives at 09:17. The alert says moderation decisions are older than five minutes. The on-call view shows three symptoms: new submissions are entering `pending_review`, the candidate scorer has no new eligible records, and the image worker is retrying two large uploads. There is no useful answer in a dashboard that reports only model latency.

I start with the state transition. Did intake create a record? Did the policy gate write a decision? Did the queue acknowledge the work? Did the scorer consume only `allowed` records? A missing transition is more actionable than an average response time.

For this workflow, the record needs an immutable submission ID and a monotonic attempt number. A retry must update an attempt, not create a second candidate score. The moderation result should distinguish `allow`, `block`, and `review`; it should also carry a policy version, input hash, and reason code. Keep the raw upload out of ordinary logs.

That gives the on-call a short path from page to cause. It also gives product teams a defensible answer when a candidate asks why a submission was held.

## Can one API key handle text and image moderation safely?

The gate answers eligibility questions, not merit questions. For a cover letter or comment, it can check abuse categories, prohibited personal data, prompt-injection patterns, and whether the content is present. For an avatar, it can check decodability, dimensions, animation policy, and image-safety categories. A portfolio upload needs a separate file-type and malware pipeline before any extracted text is considered.

Do not send every byte directly to a chat model. Decode images with a bounded pixel budget, strip metadata, normalize orientation, and calculate a content hash. I would cap decoded dimensions at 2560x2560 and reject files above 8 MB before queueing; those are policy limits to tune against the actual submission mix, not universal safety constants. The image-processing step is a control plane concern: it prevents a compressed file from expanding into an unbounded workload, and it makes retries easier to reason about. A small Go boundary can make those rules visible:

```go
type Decision string

const (
	Allow  Decision = "allow"
	Block  Decision = "block"
	Review Decision = "review"
)

type ModerationResult struct {
	SubmissionID string   `json:"submission_id"`
	Decision     Decision `json:"decision"`
	Policy       string   `json:"policy_version"`
	InputHash    string   `json:"input_sha256"`
	Reasons      []string `json:"reason_codes"`
}

func eligibleForScoring(r ModerationResult) bool {
	return r.Decision == Allow && r.Policy != "" && r.InputHash != ""
}
```

The model output is data, not a paragraph for another parser to interpret. Validate it against a schema, reject unknown decision values, and treat malformed output as `review` or a retriable failure according to the runbook. Never convert a timeout into `allow` because the queue is getting old.

That is a deliberate latency trade-off.

## How does the queue protect the scoring path?

The queue separates intake latency from analysis latency. Intake stores the submission and returns a tracking state; workers fetch the bytes by an authorized reference, perform deterministic checks, call the moderation service, validate the result, and commit the decision with an idempotency key such as `submission_id:attempt`. The scorer consumes only committed `allow` decisions.

This is where duplicate delivery becomes a normal event rather than a production mystery. The worker must be safe to run twice. Use a unique constraint on the decision key, or an equivalent compare-and-set transition, and make the acknowledgement happen after the decision is durable. If the process dies after the write but before acknowledgement, the next attempt should observe the existing result and finish without issuing a second score.

An event stream can also expose progress to a review console. Server-sent events are a reasonable fit for one-way status updates: the browser receives state changes, while commands still use ordinary authenticated requests. The stream is not the source of truth. Reconnect from the stored submission state, because a dropped connection must not change the moderation outcome.

Measure each edge separately: intake-to-queued, queued-to-started, normalization duration, moderation duration, decision-to-acknowledgement, and age of the oldest pending item. Add counters for schema failures, deterministic rejects, model timeouts, review outcomes, and duplicate deliveries. A single p95 hides the queue that is actually paging you.

## Thresholds are product policy, not monitoring decoration

A threshold that blocks too much candidate content creates false positives. A threshold that sends too much questionable material to scoring creates reviewer load and may expose unsafe content. The right response is a three-way policy with a bounded review budget, not a single number copied from a demo.

Start with a labeled evaluation set that represents the real submission mix: short comments, long cover letters, scanned images, avatars, multilingual text, and corrupt files. Version the labels and policy. For every change, inspect false-negative and false-positive slices separately. A global accuracy number cannot tell you whether image normalization is failing only for one format or whether longer text is triggering a prompt-injection rule. A model-only path is unsuitable for corrupt files, malware scanning, and strict retention requirements, so keep those controls outside the model call even if that adds a worker and a little latency.

The alert should fire on user-visible risk: eligible submissions stalled beyond the service objective, review backlog growing for a sustained window, or a rise in duplicate decision attempts. A low model error rate does not compensate for a stuck acknowledgement path. Conversely, paging on every isolated timeout teaches the team to ignore the page and turns a recoverable retry into an operational incident.

The first assumption is that the scoring model's latency will be the main operational constraint. The harder boundary is state: a retry that does not preserve the submission ID can produce a second score, while an image check that runs inline can hold the request open. I would keep the first version deliberately plain: one intake contract, one policy result schema, one durable queue, and separate workers for text and image normalization. Add parallelism only after queue age and reviewer capacity are visible. The quality-versus-latency choice is then explicit: strict checks may delay a candidate, but bypassing them makes the downstream rubric score untrustworthy.

## Further reading

- MDN, “Using server-sent events”: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- sharp documentation: https://sharp.pixelplumbing.com
- JSON Schema specification: https://json-schema.org/specification
- OWASP File Upload Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
- OpenTelemetry documentation: https://opentelemetry.io/docs/
