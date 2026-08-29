# How to Govern Image Derivatives — Private Object Storage, Presigned Download URLs

Short answer: let the browser upload each large customer-support image directly to private S3-compatible object storage, enqueue an immutable object key, resize it with Node.js and Sharp, and issue a short-lived presigned download URL only after the thumbnail is durably stored. Page on stalled work, not raw traffic. The useful early signal is the age of the oldest unprocessed object, paired with a completion ratio and enough request volume to rule out idle periods.

The page arrives as “thumbnail delivery SLO burning.” On-call can see that attachment metadata exists, yet agents opening a ticket still have no preview. The original image may already be safe in the private bucket; the customer-facing failure is the gap between that upload and a readable derivative. I've been paged by missed jobs and duplicate deliveries, so the first runbook question is deliberately dull: which immutable object key entered the pipeline, and which idempotency key should have produced its thumbnail?

Don't proxy a 400 MB support video or a 40 MB phone photo through the application just so it can reach object storage. That spends application memory and network capacity without improving authorization. The application should authorize the intent, constrain the object key and expiry, then return a presigned upload URL. The browser sends the bytes to storage. For genuinely large objects, use the storage service's multipart-upload mechanism rather than one oversized request; completion must happen only after every part succeeds.

## Govern the migration away from proxied bytes

Migrate one ownership boundary at a time. Treat the target workflow as two trust boundaries and one asynchronous handoff. First, the application authenticates the support user and creates a collision-resistant key such as `tickets/<tenant>/<ticket>/<attachment-id>/original`. It returns a narrowly scoped presigned upload URL, while the bucket remains private. A presigned URL is a bearer capability: anyone holding it can perform the signed operation until it expires, so don't log the query string or place it in analytics events. Second, a worker receives the key, not the file body. It reads the object as a stream, gives that stream to Sharp, applies an explicit orientation policy, resizes within the chosen bounds, and writes a new immutable key such as `.../thumb-v3.webp`. Sharp's resize API allows a fit mode and prevents enlargement with `withoutEnlargement`; pin those choices in code rather than relying on defaults. Keep `v3` in the destination key when transformation settings change. Overwriting `thumbnail.webp` makes retries easy to describe but hard to audit, and stale caches can hide which bytes an agent actually saw. Publish attachment state as `upload_authorized`, `original_observed`, `resize_started`, and `thumbnail_stored`, with timestamps recorded by the component that owns each transition. Only `thumbnail_stored` permits the application to mint a presigned download URL for the derivative. A private bucket plus an expiring URL limits access, but application authorization still has to run before every signing request. Make the worker idempotent as well: the stable input is `(source version, transform version)`, and the stable output follows from it. A redelivered queue message should confirm the expected derivative and report success rather than create another name. If the S3-compatible service exposes object version identifiers, retaining the source version in job metadata avoids resizing a later overwrite under the same key. Compatibility varies at the edges — multipart checksums, conditional writes, and versioning deserve a contract test against the exact service in production.

The upload acknowledgement is not proof that a preview exists.

Signing is not tenancy enforcement.

## How should an integration boundary govern private image resize uploads and presigned download URLs?

The alert is late by definition: users are already missing previews. Governance here means assigning one owner and one piece of evidence to every state transition, not adding an approval meeting. The web application owns authorization and the upload intent; object observation owns proof that bytes arrived; the queue owns delivery evidence; the worker owns the deterministic resize; the metadata service owns publication; and the application owns each presigned download decision. The runbook should connect those records through a trace containing the tenant-safe attachment ID, source object key hash, transform version, attempt number, and resulting object key hash. Keep bucket credentials, presigned query parameters, and customer filenames out of telemetry. A filename can contain personal data; a presigned URL contains authorization material. Consider a concrete support ticket with one 40 MB photo: metadata appears at 09:00:00, storage observation records the original at 09:00:04, a job starts at 09:00:06, and the derivative is stored at 09:00:09. Those timestamps let an operator locate the slow owner without treating the browser's upload acknowledgement as completion. They also make a duplicate at 09:00:07 unsurprising: it carries the same idempotency key, resolves to the same versioned derivative, and doesn't inflate the logical completion count.

Work backward through four questions. Did the browser complete the upload? Did storage observation produce exactly one logical resize job? Did a worker start it before the queue-age objective expired? Did the derivative write finish before the UI asked for a signed read? HTTP status helps classify a symptom, but `403` alone is ambiguous: an expired presigned URL, clock skew, a mismatched signed header, and denied application authorization are different branches of the runbook. Preserve the operation name and safe error class alongside the status.

This is where raw error rate misleads. A quiet support queue can have no completions because it has no requests. A busy queue can show a tolerable aggregate success rate while one tenant's large images wait behind smaller files. Measure accepted originals, completed derivatives, failed terminal attempts, retry attempts, bytes read and written, processing duration, and oldest pending age. Split latency by a bounded size class, not by customer or object key; unbounded labels will turn the monitoring system into its own incident.

One signal should have fired earlier: oldest pending age rising while accepted work continues. Pair it with a minimum event count and completion ratio so a single slow, valid panoramic image doesn't wake anyone. Also expose transfer time separately from Sharp processing time. If transfer dominates, changing resize concurrency won't fix the bottleneck. If processing dominates, raising concurrency without measuring memory can trade a lag alert for worker termination.

Stop guessing.

## Compare lag, completion, and transfer time

The following Go program is a small alert evaluator, not a storage SDK example. Feed it one JSON object per observation window. It pages only when there is meaningful traffic, the completion ratio is below the objective, and the oldest pending image has crossed the lag budget. The defaults are illustrative; set them from your own SLO and measured image-size distribution.

```go
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
)

type Window struct {
	Accepted         int     `json:"accepted"`
	Completed        int     `json:"completed"`
	OldestPendingSec float64 `json:"oldest_pending_seconds"`
	TransferP95Sec   float64 `json:"transfer_p95_seconds"`
	ResizeP95Sec     float64 `json:"resize_p95_seconds"`
}

func main() {
	minEvents := flag.Int("min-events", 20, "minimum accepted images in a window")
	minRatio := flag.Float64("min-ratio", 0.99, "minimum completion ratio")
	maxLag := flag.Float64("max-lag-seconds", 300, "maximum oldest-pending age")
	flag.Parse()

	scanner := bufio.NewScanner(os.Stdin)
	page := false
	for scanner.Scan() {
		var w Window
		if err := json.Unmarshal(scanner.Bytes(), &w); err != nil {
			fmt.Fprintf(os.Stderr, "invalid observation: %v\n", err)
			os.Exit(1)
		}

		ratio := 1.0
		if w.Accepted > 0 {
			ratio = float64(w.Completed) / float64(w.Accepted)
		}
		shouldPage := w.Accepted >= *minEvents &&
			ratio < *minRatio && w.OldestPendingSec > *maxLag

		fmt.Printf("accepted=%d completed=%d ratio=%.4f oldest=%.1fs transfer_p95=%.1fs resize_p95=%.1fs page=%t\n",
			w.Accepted, w.Completed, ratio, w.OldestPendingSec,
			w.TransferP95Sec, w.ResizeP95Sec, shouldPage)
		page = page || shouldPage
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read observations: %v\n", err)
		os.Exit(1)
	}
	if page {
		os.Exit(2)
	}
}
```

Run it against a captured, sanitized observation window before wiring it to paging. Exit code `0` means no page, `1` means malformed input, and `2` means the combined conditions crossed the threshold. A deployment can shadow this decision for a week and compare it with ticket-preview latency without notifying on-call. That shadow period is also where the team should discover whether five minutes is meaningful or merely a round number. I'm not sure what lag budget fits a given support operation until its urgency classes and real attachment distribution are visible.

The instrumentation change belongs in the same release as the pipeline. Count `accepted` only after the system has durable evidence of the original object, and count `completed` only after the derivative write succeeds. Otherwise, the ratio compares different populations. Use monotonic processing durations inside a process, wall-clock timestamps for cross-service queue age, and synchronized clocks for hosts that participate in the trace.

## Implement transform-version migration in a private rehearsal

Test the contract at the boundaries. Use a private test bucket and assert that an unsigned read is denied, a valid presigned upload accepts only the intended object constraints, and a presigned download stops granting access after its expiry. Avoid asserting the literal URL because signatures include time-sensitive values. Assert the method, host, key, required headers, and observable outcome instead.

Then run a small matrix: a normal JPEG, a portrait with orientation metadata, a file smaller than the thumbnail bounds, a very large image, a truncated input, and a duplicate job. Verify that the original is never made public, the derivative has the expected dimensions and format, the duplicate resolves to the same destination key, and terminal failures leave enough safe metadata for an operator to retry. Put strict input and pixel limits ahead of decoding; compressed image size alone doesn't bound decoded memory.

Deployment needs a rollback that respects data shape. Because the transform version is in the output key, the old and new worker can overlap without fighting over one object. Roll out to a fraction of jobs, compare transfer and resize latency by size class, then increase traffic. Lifecycle rules can later expire abandoned multipart uploads, superseded derivatives, or originals according to the support retention policy, but expiration should be reviewed as a records decision rather than treated as cleanup. Object lifecycle actions are asynchronous, so they don't belong on the request path.

There is a real trade-off in direct upload. It removes large bytes from application servers and improves the throughput ceiling, but it also moves progress reporting, cancellation, checksum handling, and cross-origin configuration into a browser-to-storage contract. It is not suitable when policy requires every byte to pass through an inline inspection gateway before storage acceptance. Keep the proxy path for that case, or upload into a quarantined private prefix and allow processing only after the required scanner approves it.

## Govern paging as a production change

A lag threshold must map to customer impact and an operator action. Page when an urgent support attachment is old enough that an agent cannot do time-sensitive work and the runbook can change the outcome. Send slower degradation to a ticket or daytime channel. If the only response at 03:00 is “watch the graph,” the alert isn't ready for paging.

The catch is that adding a minimum-volume guard can hide a single high-priority ticket. Solve that with an urgency-specific signal whose cardinality stays bounded, not by removing the guard from the global alert. Conversely, a threshold tied only to the largest allowed image will be too relaxed for ordinary screenshots. Separate a few policy-backed size or urgency classes and give each an explicit objective.

False positives have a concrete cost: they train on-call to distrust the next page and encourage risky concurrency changes during healthy but bursty uploads. False negatives cost agent time and delay customer resolution. Review both after each alert, record which earlier signal was available, and adjust one condition at a time. The final decision rule should remain boring enough to explain in a postmortem: work arrived, completion fell behind, the oldest item exceeded its budget, and there was something actionable to do.

## References

- AWS, “Uploading objects with presigned URLs”: https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html
- AWS, “Uploading and copying objects using multipart upload”: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- Sharp, “Resizing images”: https://sharp.pixelplumbing.com/api-resize
- Node.js, “Stream”: https://nodejs.org/api/stream.html

## Further reading

- AWS, “Object lifecycle management”: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- OWASP, “File Upload Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/File_Upload_Cheat_Sheet.html
