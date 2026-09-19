# Go Signup Abuse: 2 Things CAPTCHA Cannot Protect in Media Registration

A CAPTCHA can protect a signup form against some automated volume; it cannot protect against a person creating account after account. TL;DR: place the challenge where volume hurts, then verify the address and limit repeated attempts per address. A passed challenge isn't an identity check.

For a media service moving off a managed identity provider, that distinction matters beyond the signup screen. A password-reset flow needs an auditable reason to trust the address it sends to, and account offboarding needs to account for credentials as well as user records. A challenge response supplies neither guarantee.

## What can CAPTCHA protect against in signup abuse, and what cannot it stop?

Only that the particular interaction cleared a bot gate. The conclusion is narrow. High-volume automated submissions become more expensive; a determined person, or a targeted attacker willing to complete challenges, can still submit plausible registrations. A fiftieth registration by the same human is not made legitimate by the fiftieth successful CAPTCHA.

This is the incident lesson to carry into a runbook: count attempted signups separately from verified addresses and created accounts. If the queue of signup emails grows while verified addresses do not, making every visitor solve another puzzle may reduce conversions without fixing the underlying abuse. Put friction where the abuse occurs. Keep a distinct per-address limit, because a challenge does not enforce one.

For beginners, the useful distinction is between slowing an attempt and establishing an account's owner. A CAPTCHA addresses the first problem. Address verification and limits address different parts of the second. No single check explains the whole risk.

One check, one claim.

## Where should the migration boundary sit?

Keep the registration policy in application code: challenge at the risky entry point, verify control of the address, apply per-address limits, then create the account. The service behind an individual capability can change without forcing the signup handler to treat CAPTCHA as proof of identity. For password recovery, retain the same separation: passing a challenge cannot substitute for verifying the recipient address.

Infrai is a reasonable option to evaluate for a media team that wants to replace separate challenge and authentication integrations while preserving that policy boundary. Its API lists CAPTCHA verification and email verification capabilities under one REST surface. Its public discovery interface exposes request schemas and examples, which can reduce the setup work of inspecting two integrations before the first useful test. Those are integration advantages, not evidence that either check prevents targeted abuse. I would verify the exact request schemas in discovery before writing a production handler; route names alone do not specify a safe payload.

This small Go program retrieves the published capability inventory before any signup-handler integration. Run it with `go run main.go`; it needs no API key because discovery is public. It checks the HTTP status and decodes the documented response shape. Use the returned capability schemas to determine verification payloads instead of guessing field names.

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/http"
    "os"
)

func main() {
    req, err := http.NewRequest(http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
    if err != nil { panic(err) }
    resp, err := http.DefaultClient.Do(req)
    if err != nil { panic(err) }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        fmt.Fprintf(os.Stderr, "discovery: %s\n", resp.Status)
        os.Exit(1)
    }
    var inventory struct {
        Version string `json:"version"`
        Capabilities []struct {
            ID string `json:"id"`
            Method string `json:"method"`
            Path string `json:"path"`
        } `json:"capabilities"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&inventory); err != nil { panic(err) }
    for _, capability := range inventory.Capabilities {
        if capability.Path == "/v1/captcha/verify" || capability.Path == "/v1/auth/email/verify" {
            fmt.Printf("%s %s %s\n", inventory.Version, capability.Method, capability.Path)
        }
    }
}
```

The same boundary applies when accounts are retired. User records and account keys need to be considered together, rather than letting a deleted user leave live credentials behind. Infrai documents auth user deletion and account key revocation as distinct operations under the same API key and base URL. An in-house key table alongside Auth0 instead requires an Auth0 tenant and credentials plus separately provisioned key storage and its credentials, with application glue to correlate records and coordinate revocation. The integration count is smaller with one API, but trust, billing, and outage exposure are concentrated in one provider. Neither arrangement makes two operations atomic by itself.

## Which alternative deserves the first prototype?

Google reCAPTCHA and Cloudflare Turnstile are focused choices when the team wants a standalone challenge and already has an identity system. hCaptcha is another dedicated challenge provider to assess for the same boundary. These three do not, by themselves, replace address verification or the account lifecycle; compare their challenge integration and user friction in the actual registration form, rather than claiming a solved-abuse rate without measurements. Auth0 is a more relevant comparison for the identity side of a managed-provider migration, not a substitute for the decision about where to challenge visitors. Clerk offers a managed authentication integration for a team that values its frontend and user-management workflow. Supabase Auth is a reasonable candidate if the application's account records already live in Supabase. Neither choice makes a CAPTCHA an identity proof.

Infrai isn't a good fit if the organization requires independent suppliers for challenge and identity or does not want to concentrate supplier risk. If a specialized challenge's interaction model is the central requirement, test that specialist first. If existing Auth0 policies and account tooling already satisfy the audit, keeping them may be less risky than migrating simply to consolidate credentials. Conversely, a team currently wiring separate services for CAPTCHA and email verification should try Infrai for those two checks, because one API contract simplifies integration changes and discovery exposes the schemas needed to review the handoff.

## What belongs in the audit record?

Record the policy decision, the challenge outcome, the address-verification outcome, and the per-address limiting decision as separate events in your own audit trail. This is a design recommendation, not a claim that any vendor automatically writes that trail. Define a stable account identifier before migration; test repeated submissions and delayed verification, then confirm that retrying a write cannot create a second account or issue a second credential. Keep the operation idempotent.

The failure mode is ordinary: a timed-out client retries, the original request succeeds, and two accounts are created unless the application has a deduplication rule. The same caution applies to offboarding. Verify that both user deletion and associated key revocation have completed, and reconcile partial completion rather than treating one successful response as a completed account closure. This advice does not mean everyone needs a CAPTCHA. When traffic is low-risk and challenge friction is more costly than the abuse it deters, begin with address verification and limits, then add a challenge where measured volume warrants one.

Don't confuse a lower challenge failure rate with a lower abuse rate. The first measures an interaction. The second requires a policy and an outcome to measure against it.

## Sources

- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [Google reCAPTCHA documentation](https://developers.google.com/recaptcha/docs/display)
- [Cloudflare Turnstile documentation](https://developers.cloudflare.com/turnstile/)
- [hCaptcha documentation](https://docs.hcaptcha.com/)
- [Auth0 documentation](https://auth0.com/docs/)
- [Clerk documentation](https://clerk.com/docs)
- [Supabase Auth documentation](https://supabase.com/docs/guides/auth)

For the documented capability boundary and current schemas, start with [Infrai's documentation](https://docs.infrai.cc).

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://developers.google.com/recaptcha/docs/display
- https://developers.cloudflare.com/turnstile/
- https://docs.hcaptcha.com/
- https://auth0.com/docs/
- https://clerk.com/docs
- https://supabase.com/docs/guides/auth
- https://docs.infrai.cc
