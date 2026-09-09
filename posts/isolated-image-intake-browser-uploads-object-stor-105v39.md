# Isolated Image Intake: Browser Uploads, Object Storage, and Deadline Thumbnail Queues

A deletion deadline changes the design: a browser upload of an original image cannot be considered complete until its tenant, retention deadline, and processing identity are durably bound together. **Short answer:** let the browser upload the original to object storage, turn the resulting notification into a small validated queue message, and make the backend thumbnail worker idempotent; don't run resizing in the webhook request.

The concrete case here is a gaming marketplace that retains scanned, signed creator releases. Each studio is a tenant. Reviewers need thumbnails, while the signed original and every derivative must disappear on an explicit deadline. Tenant isolation therefore outranks a few seconds of thumbnail latency.

I've been paged by missed jobs and duplicate deliveries. The invariant those incidents reinforce is plain: a notification says that something happened, but the processing record says what the system has accepted responsibility for. Keep those two facts separate.

## How can an original browser image upload survive webhook retries before a thumbnail worker runs?

Use four boundaries. First, the application backend authenticates the user and creates an upload intent containing an opaque upload ID, tenant ID, object key, expected media type, and `retain_until`. Second, the browser transfers the bytes to the authorized object location and reports upload progress. MDN documents upload progress events on `XMLHttpRequest.upload`; using those events changes the progress UI, not the server-side completion rule.

Third, an object storage notification reaches a narrow webhook or event ingress. That ingress verifies the notification, resolves the object back to the existing upload intent, and durably enqueues a normalized job. It returns success only after that enqueue commits. Fourth, a worker claims the job, reads the exact original version, generates thumbnails, writes derivatives inside the same tenant boundary, and records the outputs in a manifest.

The browser never chooses a tenant ID by writing a path segment. It receives an already authorized destination. Likewise, the notification's bucket and key are lookup inputs, not authorization proof. The upload-intent record remains the authority for ownership and deletion time.

A useful normalized message is small:

```go
type ThumbnailJob struct {
	EventID    string    `json:"event_id"`
	UploadID   string    `json:"upload_id"`
	TenantID   string    `json:"tenant_id"`
	Bucket     string    `json:"bucket"`
	ObjectKey  string    `json:"object_key"`
	Version    string    `json:"version"`
	RetainUntil time.Time `json:"retain_until"`
}
```

Don't put image bytes in that message. The object version is the immutable work input, while the upload ID is the business identity used for claims, manifests, and deletion. If the storage system does not expose object versions, use a content digest or another immutable identity established at upload completion; the exact mechanism varies, and I'm not sure which primitive your storage layer exposes. Resolve that before choosing the queue schema.

## Retention and tenant governance belong in the event contract

Treat delivery as repeatable even if a particular notification service usually appears to deliver once. A stable event ID suppresses replay of the same notification. A unique processing key such as `(tenant_id, upload_id, transform_revision)` suppresses two different notifications that refer to the same logical work. Both checks matter: the first protects the ingress path, and the second protects output creation.

This is where an apparently harmless shortcut becomes an incident. Suppose Studio Red uploads `red/originals/release-81.png` with a Friday deletion deadline. A delayed notification arrives after a retry, and two workers receive related jobs. If each worker merely checks whether `thumb.png` exists, one can race the other, and a later transform revision can be mistaken for the old output. Worse, if deletion has already run, a stale job can recreate a thumbnail after the signed original's deadline. The claim must include the tenant and transform revision, and the write must reject an expired `retain_until`. The manifest must list every derivative under the same deletion policy. Existence is not a state machine.

Use the deadline as data, not as a convention in an object name.

| Decision | Safer default | Why it matters |
|---|---|---|
| Tenant authority | Server-side upload intent | A browser path is user input |
| Work identity | Tenant, upload ID, transform revision | Retries cannot cross tenants or revisions |
| Source identity | Exact object version or digest | A replaced key cannot change queued work |
| Expiry | One explicit timestamp propagated to derivatives | Late work cannot extend document retention |
| Completion | Manifest committed before acknowledgment | Deletion and review can find all outputs |

The webhook should do little work: authenticate, validate shape, find the upload intent, deduplicate the event, enqueue, and answer. Under HTTP semantics, `202 Accepted` means a request was accepted for processing but processing is not complete; that makes it suitable only when the durable handoff succeeded. A malformed or unauthorized notification is not retryable work. A temporary enqueue failure should leave the delivery unacknowledged according to the notification transport's retry contract rather than pretending that thumbnail generation started.

## Implement the expiry-aware claim before resizing

The queue consumer below is in Go, but a Node.js queue worker should enforce the same order of operations. The interfaces deliberately hide any vendor SDK. `Claims` represents a transactional store with a uniqueness constraint, and `Objects.PutDerived` must enforce the supplied deadline atomically with the write. Those are contracts worth testing, not comments to trust.

```go
package thumbnails

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrExpired = errors.New("retention deadline reached")

type Claims interface {
	TryStart(ctx context.Context, tenantID, uploadID, revision string) (bool, error)
	Complete(ctx context.Context, tenantID, uploadID, revision, derivedKey string) error
}

type Objects interface {
	GetVersion(ctx context.Context, tenantID, bucket, key, version string) ([]byte, error)
	PutDerived(ctx context.Context, tenantID, key string, body []byte, retainUntil time.Time) error
}

type Resizer interface {
	Thumbnail(original []byte, width int) ([]byte, error)
}

type Worker struct {
	Claims  Claims
	Objects Objects
	Resizer Resizer
	Now     func() time.Time
}

func (w Worker) Handle(ctx context.Context, j ThumbnailJob) error {
	const revision = "thumb-320-v1"

	if !w.Now().Before(j.RetainUntil) {
		return nil // Stale work must not recreate an expired document.
	}

	started, err := w.Claims.TryStart(ctx, j.TenantID, j.UploadID, revision)
	if err != nil {
		return fmt.Errorf("claim job: %w", err)
	}
	if !started {
		return nil // Another delivery already owns or completed this transform.
	}

	original, err := w.Objects.GetVersion(
		ctx, j.TenantID, j.Bucket, j.ObjectKey, j.Version,
	)
	if err != nil {
		return fmt.Errorf("read original: %w", err)
	}

	thumb, err := w.Resizer.Thumbnail(bytes.Clone(original), 320)
	if err != nil {
		return fmt.Errorf("resize original: %w", err)
	}
	if !w.Now().Before(j.RetainUntil) {
		return ErrExpired
	}

	derivedKey := fmt.Sprintf("%s/derived/%s/%s.jpg", j.TenantID, j.UploadID, revision)
	if err := w.Objects.PutDerived(ctx, j.TenantID, derivedKey, thumb, j.RetainUntil); err != nil {
		return fmt.Errorf("write thumbnail: %w", err)
	}
	if err := w.Claims.Complete(ctx, j.TenantID, j.UploadID, revision, derivedKey); err != nil {
		return fmt.Errorf("commit manifest: %w", err)
	}
	return nil
}
```

There is an intentional sharp edge: a worker can write the derivative and then lose its manifest commit. The retry must converge on the same deterministic derivative key, and `Complete` must be idempotent. A reconciliation job should compare successful claims, derived objects, and manifests before the deletion deadline. It should not scan every tenant through one unrestricted credential; partition the reconciliation scope using the same tenant boundary as normal processing.

Ack last.

HTTP idempotency does not make this worker idempotent. RFC 9110 defines an idempotent request method by the intended effect of multiple identical requests, but queue delivery, image transformation, manifest mutation, and storage writes form a larger operation. The application must supply its own stable identity and convergence rules across that operation.

## Rollout: rehearse deletion races before choosing the handoff

The minimum test matrix includes duplicate notifications, two workers claiming concurrently, a replaced object key, a tenant/key mismatch, an expiry reached before processing, an expiry reached during resizing, a derivative write followed by a lost acknowledgment, and a transform revision change. Inject each condition. Then assert both visible output and absence: no cross-tenant read, no duplicate manifest row, and no derivative after expiry.

Deployment needs equally plain signals. Track the age of the oldest accepted job, claim conflicts, retry counts, time remaining until `retain_until`, and manifest reconciliation gaps. Alert on time-based risk rather than raw queue depth alone: ten jobs expiring in five minutes can matter more than ten thousand cosmetic images with no deadline. Logs should carry event ID, upload ID, tenant ID, source version, transform revision, and claim result, but never the signed image bytes or sensitive document contents.

Webhook-only processing is suitable when the transformation is tiny, delivery retries are bounded and understood, the caller can wait, and there is no meaningful retention race. The catch is that signed-document resizing has a durable responsibility boundary and a deletion clock, so doing the resize inside the notification handler couples storage delivery to CPU time and deploy restarts. Use a queue in that case. Conversely, a queue is not free operationally: it adds claim storage, retry policy, dead-letter review, reconciliation, and lag monitoring. Stick with a synchronous backend upload and resize when volume is low, the request can safely complete within its timeout, and the original does not need direct browser-to-storage transfer.

For tenant isolation, shared infrastructure is acceptable only if authorization is enforced on every read, write, claim, and reconciliation query. Separate buckets or credentials can reduce the blast radius for high-risk tenants, but they increase provisioning and policy work. The right boundary follows the threat model and deletion evidence your organization must produce, not a generic preference for more queues or more buckets.

## References

- https://www.rfc-editor.org/rfc/rfc9110
- https://developer.mozilla.org/en-US/docs/Web/API/XMLHttpRequest_API/Using_XMLHttpRequest
