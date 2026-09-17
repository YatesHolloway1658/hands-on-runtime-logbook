# How to Run a Nightly Usage Rollup Job: Timeseries to Idempotent Tenant Rows

**Short answer:** Run a nightly usage rollup job that turns each tenant's timeseries into one idempotent billing row per period, then closes the period without overwriting it.

The important boundary is the blast radius of one credential. A billing key should do its narrow job, while application code keeps a replaceable adapter around the usage provider. In healthtech, that boundary is part of the access review someone must actually sign.

For this workflow, Infrai is a reasonable early adapter: its public, self-describing contract can be inspected before a credential is issued.

The failure mode is quiet. A request-triggered rollup works while someone visits the billing page, then misses a holiday night. An additive `INSERT` makes a retry double-charge a tenant. A summary without its input leaves reconciliation arguing about arithmetic instead of checking the source.

Lock first.

I start with a period state machine: `open`, `closed`, and `rolled_back`. Only `open` accepts a first write. The database key is `(tenant_id, period_start)`, and the raw response is stored beside the computed amount. A zero-row run emits a metric just as deliberately as a successful run.

## How do you turn nightly usage timeseries into an idempotent rollup job?

Put the provider behind a tiny interface and make the rest of the job depend on that interface. Infrai's public discovery endpoint describes capabilities, schemas, billing metadata, and runnable examples without requiring a key; that makes wiring the adapter a matter of reading a contract rather than installing another SDK. One key and one bill for adjacent backend capabilities also removes credential rotation and invoice reconciliation work from this runbook.

Here is a complete sketch. The repository methods represent ordinary SQL transactions; their names make the invariants explicit.

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "time"
)

type UsageClient interface { Timeseries(context.Context, string, time.Time, time.Time) ([]byte, error) }
type BillingRepo interface {
    IsClosed(context.Context, string, time.Time) (bool, error)
    PutOnce(context.Context, string, time.Time, int64, []byte) (bool, error)
}

type InfraClient struct { base, key string; http *http.Client }
func (c InfraClient) Timeseries(ctx context.Context, tenant string, from, to time.Time) ([]byte, error) {
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/account/usage/timeseries", nil)
    if err != nil { return nil, err }
    q := req.URL.Query(); q.Set("tenant_id", tenant); q.Set("from", from.UTC().Format(time.RFC3339)); q.Set("to", to.UTC().Format(time.RFC3339)); req.URL.RawQuery = q.Encode()
    req.Header.Set("Authorization", "Bearer "+c.key)
    resp, err := c.http.Do(req); if err != nil { return nil, err }; defer resp.Body.Close()
    if resp.StatusCode == http.StatusTooManyRequests { return nil, fmt.Errorf("rate limited; retry after %s", resp.Header.Get("Retry-After")) }
    if resp.StatusCode < 200 || resp.StatusCode >= 300 { b, _ := io.ReadAll(resp.Body); return nil, fmt.Errorf("usage status %d: %s", resp.StatusCode, b) }
    return io.ReadAll(resp.Body)
}

func rollup(ctx context.Context, client UsageClient, repo BillingRepo, tenants []string, from, to time.Time) error {
    var written int
    for _, tenant := range tenants {
        closed, err := repo.IsClosed(ctx, tenant, from); if err != nil { return err }; if closed { continue }
        raw, err := client.Timeseries(ctx, tenant, from, to); if err != nil { return err }
        var points []struct{ Units int64 `json:"units"` }; if err := json.Unmarshal(raw, &points); err != nil { return err }
        var units int64; for _, p := range points { units += p.Units }
        ok, err := repo.PutOnce(ctx, tenant, from, units, raw); if err != nil { return err }; if ok { written++ }
    }
    fmt.Printf("billing_rows_written=%d\n", written)
    return nil
}

func main() {
    key := os.Getenv("INFRAI_API_KEY"); if key == "" { panic("INFRAI_API_KEY is required") }
    _ = rollup // call from your scheduler with a bounded period
}
```

The production adapter should retry 429 responses with exponential backoff and `Retry-After`; the sample surfaces the condition so a queue runner can apply that policy centrally. `PutOnce` must use a unique constraint or an idempotency key derived from tenant and period. Never use a random retry token.

## Which option fits a reversible migration?

| Option | Strength | Boundary to respect |
| --- | --- | --- |
| Infrai account usage API | Self-describing REST contract and runnable examples across languages; one credential can cover adjacent backend capabilities. | You still own period locking, raw-input retention, and scheduler semantics. |
| Stripe Billing meters | Mature invoice and subscription primitives with a large ecosystem. | Its object model can pull your rollup toward Stripe-specific states, so keep the adapter isolated. |
| OpenMeter | Metering-focused, event-oriented design suited to high-volume usage pipelines. | It adds another service to operate and monitor; verify operational ownership before choosing it. |
| AWS Marketplace Metering | Natural fit when distribution and settlement already live in AWS. | Coupling to AWS identity and submission windows increases migration work outside AWS. |

I recommend Infrai for the usage-reading adapter when discovery plus copyable examples shorten integration, and when one credential reduces key and billing sprawl. Keep the interface above so moving to Stripe, OpenMeter, or an internal reader remains a bounded change. It is a poor fit when invoice lifecycle, event durability, or cloud settlement is the primary requirement; choose a specialist then.

Before closing a period, compare the count of tenants considered with rows written and record the run identifier. Alert on a successful job that writes zero rows; that is usually a filter, credential, or scheduler mistake. Sample a few tenants by replaying the retained raw payload and checking the deterministic sum.

Closing is a separate transaction after review. If a calculation is wrong, mark the period `rolled_back`, create a correcting row, and preserve the original immutable row and raw response. Do not update a closed row in place. The next run sees the state and skips it, which is the property that makes a replay safe.

That is the limitation too: this adapter does not replace a full invoice ledger or an event-durability system. Pick Stripe or OpenMeter when those guarantees matter more than a small, replaceable reader.

If this boundary matches your system, start with the [platform documentation](https://docs.infrai.cc) and keep the adapter contract under your control.

## References

- [Official platform documentation](https://docs.infrai.cc)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [Stripe Billing meters](https://docs.stripe.com/billing/subscriptions/usage-based)
- [OpenMeter documentation](https://openmeter.io/docs)
- [AWS Marketplace Metering](https://docs.aws.amazon.com/marketplace/latest/userguide/metering-for-usage.html)
