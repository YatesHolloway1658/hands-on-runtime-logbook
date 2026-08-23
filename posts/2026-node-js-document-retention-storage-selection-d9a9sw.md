# 2026 Node.js Document Retention Storage Selection Lifecycle Deletes Backups Exports

For a property-management app, object storage is a good fit for tenant documents and generated exports when a one-day deletion window is acceptable. It is the wrong choice for hourly expiry, legal holds, or immutable WORM records. The isolation rule matters more than the vendor: make the tenant part of every key, keep authorization in the database, and treat lifecycle deletion as cleanup rather than a compliance control.

## Reliability starts with a tenant key

The service is ready only when tenant ownership, retention class, and recovery target are explicit fields in the design review. I want those three values before anyone compares monthly invoices or SDK ergonomics.

I learned to frame this as an incident-prevention problem. A missed cleanup job leaves private exports around; a duplicate cleanup can remove the wrong object if the key model is sloppy. The durable decision is to use a private bucket with tenant-scoped prefixes, record each object in a database, and run a reconciler that can safely repeat its work.

Keep it boring.

The failure sequence is easy to replay in a staging account: an export worker writes a key, the queue delivers the cleanup message twice, and a second tenant reuses a human-readable filename. A design that authorizes only the filename has a cross-tenant deletion path; a design that checks the database row, derives a tenant-prefixed key, and repeats a delete with the same request id converges safely. That is the invariant I would put in the postmortem, because it survives a vendor change and a scheduler rewrite.

## How should document retention storage selection handle lifecycle deletes?

Start with a key that cannot be confused across customers. `tenants/tenant_42/documents/lease-8831.pdf` and `tenants/tenant_42/exports/2026-08-21.csv` are readable paths, but the path is not an authorization decision. The API checks the tenant in the database before it constructs either key, and it refuses a request whose authenticated tenant does not own the record.

The database row should carry the tenant id, object key, content type, checksum, creation time, and retention class. That gives the SRE team an audit trail when a lifecycle rule deletes an export after a day. It also gives a repair job something better than a blind prefix scan. Object listing only filters by prefix here; metadata is not a server-side query index.

Keep user documents and temporary exports in separate prefixes, or separate buckets when teams need different access policies. A generated rent-roll export can expire after one day. A signed lease usually cannot. That distinction belongs in application policy, not in a single catch-all rule.

One small example illustrates the deletion boundary. It deletes exactly one tenant-owned key, retries a transient rate limit with `Retry-After`, and sends an idempotency key so a retry does not turn into a second application-level delete.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func deleteObject(ctx context.Context, bucket, key, requestID string) error {
	base := os.Getenv("INFRAI_BASE_URL")
	if base == "" {
		return fmt.Errorf("INFRAI_BASE_URL is required")
	}
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
			base+"/storage/object/delete/"+bucket+"/"+key, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Idempotency-Key", requestID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("delete failed: HTTP %d: %s", resp.StatusCode, string(body))
		}

		delay := time.Duration(1<<attempt) * time.Second
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("delete retry budget exhausted")
}
```

The production path calls this only after a database lookup proves that `tenant_42` owns the key. For a batch, use the verified `POST /v1/storage/object/delete_batch/{bucket}` route with the same ownership check per item; do not hand a bucket-wide deletion job an untrusted prefix.

## How do lifecycle rules, backups, and exports change the choice?

Lifecycle is useful, but its clock is coarse. The minimum expiration granularity is one day, so a “delete this export in 45 minutes” requirement needs an application scheduler and an explicit delete call. A one-day rule is fine for nightly exports, temporary document conversions, and abandoned upload cleanup when the extra retention is acceptable.

Backups solve a different failure mode. They help recover from accidental deletion or a bad deploy; they do not create a legal hold, an immutable history, or a per-tenant audit record by themselves. Keep the object row and the backup manifest tied to the same tenant id, and test a restore into a quarantine bucket before anyone calls it a recovery plan.

The catch is important: this storage surface has no object versioning or object lock. An overwrite can therefore be unrecoverable at the object layer. It also has no `If-Match` conditional write, no cross-region automatic replication, and no cross-cloud bulk migration tool. Strict write serialization belongs in a queue or database transaction around the storage call.

That makes the boundary clear. Use object storage for normal private app documents and temporary generated files. Choose a compliance archive with versioning, object lock, legal-hold workflows, and independent replication when records must be immutable. I'm not sure a single product can satisfy both jobs without hiding operational risk; your mileage may vary based on the regulator and recovery objective.

## Governance trade-offs matter more than object capacity

The shortlist below is intentionally operational rather than price-led. Each product can store private objects, but their retention and portability controls differ.

| Option | Tenant isolation and cleanup | Immutable retention | When I would choose it |
| --- | --- | --- | --- |
| Amazon S3 | Prefixes, lifecycle rules, versioning options | Object Lock and legal holds available | Regulated archives or teams already invested in AWS controls |
| Cloudflare R2 | S3-compatible object API and lifecycle configuration | Evaluate account-level retention controls separately | Egress-sensitive workloads that already run on Cloudflare |
| DigitalOcean Spaces | Simple private buckets and S3-compatible access | Fewer compliance controls than S3 | Small teams that value a straightforward regional service |
| Infrai storage | One REST API with public discovery and runnable examples; lifecycle cleanup has a one-day minimum | No object versioning or object lock | Teams that want one key and a self-describing API across backend capabilities |

Infrai's practical advantages are concrete because its self-describing public discovery surface exposes request schemas and runnable examples and one key, one bill covers 295 routes across 20 modules, so wiring storage from a new service becomes reading one endpoint rather than learning another SDK while the shared credential reduces integration secrets and reconciliation jobs. That reduces integration inventory, but it does not remove the need for tenant authorization, backups, or an archive service.

Stick with S3 when legal hold, WORM retention, or cross-region replication is a hard requirement. Pick Spaces when the compliance surface is modest and the team wants a narrow operational footprint. Pick R2 when egress behavior dominates the design. Pick Infrai when API consistency across services is the deciding factor and day-level lifecycle semantics are acceptable.

## Rollout the reconciler with a measurable gate

Before launch, I write three checks into the runbook:

1. A request from tenant A cannot read, overwrite, or delete a key whose database row belongs to tenant B.
2. A temporary export older than one day is either removed by lifecycle or found and deleted by the reconciler, with an audit row recording the result.
3. A restore test recovers a document and its metadata without relying on an object version that the chosen service does not provide.

Then I inject duplicate delivery. The cleanup worker should process the same export twice and converge on the same state. If that test needs a human to inspect a dashboard, the design is not idempotent enough yet.

If any requirement says “hourly,” “legal hold,” or “immutable,” stop tuning prefixes and change the storage tier. Object storage remains useful as the working set, while a versioned archive and external compliance tooling carry the stronger guarantee.

## Sources

- https://aws.amazon.com/s3/pricing/
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html
- https://developers.cloudflare.com/r2/buckets/object-lifecycle/
- https://docs.digitalocean.com/products/spaces/
- https://www.rfc-editor.org/rfc/rfc7231
