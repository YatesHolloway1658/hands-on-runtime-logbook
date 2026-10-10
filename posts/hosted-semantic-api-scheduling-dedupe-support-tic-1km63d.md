# Hosted Semantic API Scheduling: Dedupe Support Tickets Without Running Models

TL;DR: For a B2B SaaS support queue, use a hosted semantic API to create candidates, but schedule freshness checks around the ingestion watermark rather than trusting the nearest match alone. A Node.js caller does not need to host a model to dedupe support tickets semantically; it does need to own ingestion state, idempotency, and the decision policy. Page on a sustained breach of the end-to-end freshness objective, not on one slow request. Keep deterministic ticket keys and a review band so a retry cannot create a second duplicate decision.

The page usually arrives with an unhelpful symptom: duplicate tickets are climbing while the semantic API still reports successful requests. The on-call can see healthy request latency and a queue that is draining. Yet recent tickets are not being matched. The first question is operational, not mathematical: did the scorer become less useful, or did the scheduled ingestion job stop presenting current candidates to it?

That distinction changes the response. A model-quality problem calls for evaluation and threshold work. A freshness problem calls for replaying a bounded interval, verifying watermarks, and preserving the decisions already written. Mixing the two produces noisy pages and risky reprocessing. The trade-off is concrete: waiting for more candidates can improve retrieval opportunities while adding decision latency, but firing immediately can miss a ticket that has not become visible yet. Scheduling defines that boundary, so it belongs in the design rather than in an emergency workaround.

## How should an API dedupe support tickets semantically without hosting a model?

Semantic matching is a pipeline. A new ticket has to be normalized, embedded, placed in a searchable index, compared with plausible prior tickets, and attached to evidence. In this system, that evidence includes product manuals and incident notes held in a folder of PDFs. Retrieval-augmented generation separates model knowledge from retrieved external memory; the same separation is useful here because the match score and the retrieved support evidence have different failure modes.

A successful embedding call proves only that one stage answered. It does not prove that the newest ticket is searchable, that PDF extraction has reached the latest document, or that the duplicate decision was persisted. Green dependencies can coexist with stale results.

That is the trap.

The signal that should fire earlier is age, measured at boundaries. Record the source event time, normalization completion time, embedding completion time, index visibility time, and decision time. Then publish the oldest unprocessed event age and the latest searchable watermark. Queue depth alone is ambiguous: a large queue may be moving quickly, while a queue of one poisoned item may block a partition.

Use two service objectives rather than one blended dashboard:

- ingestion freshness: how far index visibility trails the source event time;
- decision latency: how long a searchable ticket waits for a duplicate decision.

This split is plain on purpose. It tells the runbook where to start.

No guesswork.

## Schedule the check around watermarks, not wall-clock success

A cron exit code is not a freshness guarantee. The checker should compare a durable source watermark with a durable searchable watermark, then make one decision for a named interval. If the scheduler retries the same interval, the key must remain the same.

For example, treat a five-minute window as a configuration choice, not a universal recommendation. A high-volume queue may need a shorter interval; a low-volume enterprise queue may accept a longer one. The useful invariant is that every interval has a stable identity and that completion advances monotonically only after its writes are visible.

The following Go sketch keeps the scheduler contract small. The stores and scorer are interfaces because hosting the model is outside the application boundary; transport details do not belong in the scheduling loop.

```go
package dedupe

import (
	"context"
	"fmt"
	"time"
)

type Window struct {
	Start time.Time
	End   time.Time
}

type Ticket struct {
	ID   string
	Text string
}

type Source interface {
	Tickets(context.Context, Window) ([]Ticket, error)
}

type Scorer interface {
	Candidates(context.Context, Ticket) ([]string, error)
}

type Decisions interface {
	AlreadyCommitted(context.Context, string) (bool, error)
	Commit(context.Context, string, map[string][]string) error
}

func RunWindow(ctx context.Context, w Window, src Source, scorer Scorer, out Decisions) error {
	key := fmt.Sprintf("tickets:%s:%s",
		w.Start.UTC().Format(time.RFC3339),
		w.End.UTC().Format(time.RFC3339))

	done, err := out.AlreadyCommitted(ctx, key)
	if err != nil || done {
		return err
	}

	tickets, err := src.Tickets(ctx, w)
	if err != nil {
		return fmt.Errorf("read window %s: %w", key, err)
	}

	result := make(map[string][]string, len(tickets))
	for _, ticket := range tickets {
		candidates, err := scorer.Candidates(ctx, ticket)
		if err != nil {
			return fmt.Errorf("score ticket %s: %w", ticket.ID, err)
		}
		result[ticket.ID] = candidates
	}

	if err := out.Commit(ctx, key, result); err != nil {
		return fmt.Errorf("commit window %s: %w", key, err)
	}
	return nil
}
```

This example deliberately commits the window as a unit. Production storage may implement that contract with a transaction, a conditional write, or another atomic mechanism. The mechanism can vary; the observable promise cannot. A retry either finds the committed key or finishes the same interval without multiplying decisions.

One trap deserves explicit treatment: do not advance the searchable watermark when the API accepts embeddings. Advance it only when a read through the query path can observe the indexed records. Acceptance and visibility are separate events. If the backend does not expose a visibility acknowledgement, the checker needs a sentinel read or a conservative delay, and the uncertainty should appear in the freshness metric.

The Node.js service that receives the support ticket can remain a thin client. It submits normalized text to the API, records the returned artifact identity, and hands durable state to the scheduler; the Go example below focuses on the worker contract because retries and commits are the risky part. This language boundary should not change the idempotency key. A deploy of either process must be able to resume the same five-minute interval, observe an existing commit, and stop without producing a second linkage. If the client times out after submission, it should reconcile by stable ticket identity before resubmitting. If the worker times out during scoring, it should retry the bounded interval. If visibility lags, it should hold the watermark. Three similar timeouts, three different actions.

## Trace the page backward to the first actionable signal

Start the runbook with the customer-visible alert: the rate of tickets entering manual review or later identified as duplicates has crossed its agreed boundary. Then walk backward through four checks. Keep this sequence fixed so two responders do not replay the same interval from opposite ends.

| Check | Evidence | Action |
|---|---|---|
| Decision writer | Last committed interval and idempotency key | Resume the first uncommitted interval |
| Candidate query | Latest searchable ticket ID and visibility time | Probe the query path before replaying writes |
| Embedding stage | Oldest pending event age and error class | Retry only the bounded failed interval |
| Source intake | Durable source watermark | Repair intake before touching thresholds |

The key comparison is between source and visibility watermarks. If that gap grows while scoring latency stays flat, tuning the similarity threshold cannot help. If freshness is inside its objective but review outcomes drift, then a labeled evaluation set becomes the next tool. Those paths should be different alerts because they have different owners, rollback options, and blast radii.

Keep alert annotations concrete: affected interval, oldest event time, latest visible event time, and the run identifier. Avoid putting ticket text or PDF contents into labels; high-cardinality content makes operational signals harder to aggregate and may carry customer data. An opaque ticket ID is enough for a responder to follow the authorized diagnostic path.

I use the same postmortem question for each boundary: what evidence would have shown this failure one stage earlier? For intake, it is the source watermark. For indexing, it is read visibility. For decisions, it is the committed interval key. This is an idempotency reflex, but it also shortens the page: every answer names the next safe action.

## Keep retrieval quality separate from scheduler health

Fresh data can still produce poor matches. Evaluate semantic retrieval offline with labeled pairs that include true duplicates, related-but-distinct incidents, boilerplate-heavy tickets, and cases whose resolution depends on a PDF revision. Split evaluation data by time so a later manual label does not leak into an earlier decision. The exact score threshold is local to the corpus and scorer; no defensible universal value applies to every queue.

Use at least three decision bands: automatic linkage only where evidence supports it, human review for ambiguous candidates, and no match below the review boundary. The bands make the quality-versus-latency trade-off explicit. Expanding the candidate set can recover difficult matches, but it increases scoring work and may delay the queue. Tightening it reduces work while risking missed duplicates. Measure both effects on the same labeled set before changing production configuration.

PDF evidence adds another clock. Store the document identity and revision associated with a decision so a reviewer can tell whether the match used current documentation. When a PDF is replaced, reprocessing every historical ticket may be unnecessary. Schedule the smallest affected set that the document-to-ticket references can identify, and give that replay its own idempotency key.

Do not let the generated answer decide whether two tickets are duplicates. Retrieval can supply context for an explanation, but the duplicate decision should remain a recorded, testable output with its score, candidate set, evidence revision, and policy version. That separation makes a threshold change auditable and a replay bounded.

## Instrument the repair before lowering the alert threshold

Add metrics at the state transitions first: source watermark age, searchable watermark age, uncommitted interval count, interval completion duration, retry count, and review-band rate. Correlate them with a run ID. A trace is helpful when it crosses the hosted scoring call and the storage commit, but it does not replace durable watermarks; traces can describe an execution that never advanced system state.

Then test the failure modes directly. Delay visibility after a successful embedding response. Return a scorer error halfway through an interval. Deliver the same source event twice. Restart the worker after the decision write but before acknowledgement. The required result is boring: one committed decision per ticket and policy version, a watermark that never skips unfinished work, and a page that identifies the blocked boundary.

Deploy scheduler changes with a shadow calculation before they control paging. Compare the proposed freshness state with the current state over complete business cycles, including quiet hours when sparse traffic makes rate-based alerts unstable. A zero-ticket interval should be recorded as complete, not silently absent, or the monitor cannot distinguish no work from a worker that never ran.

The final threshold has a real false-positive cost. Page too early and transient visibility lag repeatedly wakes someone who has no safe action beyond waiting; responders learn to distrust the signal. Page too late and the support queue accumulates stale decisions that require a larger replay. Set the boundary from the documented support objective and observed end-to-end lag distribution, require a sustained breach, and route non-urgent drift to a ticket. The best alert is the earliest one tied to a reversible action.

## Further reading

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
