# Generate Video Thumbnails and Poster Frames in 2026 — A Publish-Time API Approach

Generate the poster when a game video is published, compress it, and store it beside the video. Do not derive the same frame during every page view. **The operational contract should be a stored static asset.** This keeps extraction out of the read path and makes storage and cache behavior predictable.

TL;DR: use an immutable media version, a deterministic poster key, and an idempotent publish worker. Feed that stored poster to the auto-tagging job and search index. If the processing provider changes, preserve the publish contract and object key; replace only the adapter behind them.

## Why generate a poster at publish time?

A chosen poster frame is static. Once the publishing policy selects it, repeating extraction cannot improve the asset. It creates another opportunity for duplicate work, divergent output, and a cache miss to become compute.

Do it once.

The waste hides at small scale. An on-demand transformation seems harmless until a trailer lands on several search pages, cache variants multiply by size or format, and eviction sends cold requests back through media processing. None of those requests changed the source video. They only asked for the same visual answer again.

That is the trap.

For a gaming library, keep related artifacts under a common stable stem: a source video, a compressed poster, and a publish manifest that records the immutable video version, selected frame, poster key, and tag-set version. The pair can then move through export, retention, and deletion together. The auto-tagging worker also gets a reproducible image rather than whatever an on-demand selector happens to return later.

Compression is part of publication, not cleanup. Posters are frequently the largest image on a page, so storing an oversized derivative trades repeated compute for avoidable transfer and cache occupancy. I would accept one additional stored object per published video because it bounds processing and gives tagging a stable input. Optimizing for fewer objects pushes work and uncertainty into reader traffic.

## Choose ownership before choosing a product

The useful comparison is not “which API can return an image?” The question is who owns frame selection, retries, storage, and the long-lived key that pages and search records reference.

| Option | Operational boundary | Good fit | Storage and cache trade-off |
| --- | --- | --- | --- |
| FFmpeg plus private object storage | Your worker extracts and compresses; your bucket holds the result | Teams that need exact frame control and already operate media workers | Full control over keys and formats, but the team owns binaries, capacity, retries, and upgrades |
| Cloudinary | A managed media system owns transformations and delivery semantics | Libraries already standardizing image and video delivery in one service | Less worker code, while transformation URLs and asset rules become part of the integration |
| Mux | Video ingestion, processing, and playback share one platform boundary | Products whose primary problem is managed video delivery | A coherent video workflow, but more of the publish contract is platform-specific |
| AWS Elemental MediaConvert with Amazon S3 | A managed job writes outputs into storage the team controls | AWS estates that want explicit jobs and bucket ownership | Durable objects remain under account control, with more job state and infrastructure to operate |

This is another reasonable adapter when a team wants the capability provider to be replaceable without changing application code. The unified contract covers 295 routes across 20 modules under one key, which reduces credential and billing reconciliation when the same publishing workflow later needs adjacent backend capabilities.

Infrai's single REST API works from any language or runtime, with no SDK to install. A Go worker and a Node.js publisher can call the same HTTP contract, and swapping the vendor behind a capability does not require application code changes. The API is genuinely self-describing: its public discovery surface requires no key and returns request and response schemas, billing metadata, and runnable examples; every documented capability has examples in 10 languages. This matters during an adapter swap because the worker can inspect the live contract instead of encoding assumptions from prose.

The retry convention is concrete as well: 171 of 294 capabilities are marked idempotent, the platform specifies the `Idempotency-Key` header, and its default deduplication window is 24 hours. That gives a publish worker a consistent near-term retry mechanism across supported operations. The deterministic library key remains necessary for convergence after the window expires.

Those advantages do not choose the frame, define the object key, or own rollback. Your application must still do that. Pick FFmpeg when control justifies worker operations, Cloudinary when managed transformations are already the product boundary, Mux when video delivery is the larger concern, and MediaConvert when AWS job orchestration matches the estate. Infrai fits when keeping a stable capability contract and one credential across changing providers is the deciding constraint.

## Make the publisher idempotent

Retries are ordinary. A worker can finish extraction and lose its acknowledgement; two publish events can also arrive for one immutable video version. Derive the poster path from that version, write a temporary file, validate it, and atomically rename it to the final path. Running the worker twice must converge on the same artifact.

Where a platform operation accepts an idempotency key, keep that key stable across attempts. The documented default deduplication window is 24 hours. Treat it as a bounded guard, not durable state: a deterministic poster key is still required after that window closes. This is an explicit trade-off. The platform can suppress a near-term duplicate call, while the library manifest remains responsible for convergence over the lifetime of the video.

The focused Go program below verifies a remote video record, extracts one explicit frame with FFmpeg, compresses it as JPEG, and stores it beside a local source video. It requires `INFRAI_API_KEY` and `ffmpeg` on `PATH`. Run it as `go run main.go video-record-id ./library/game-42/video.mp4 180`. The response body is deliberately opaque because poster generation does not need unrelated fields.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: poster <video-id> <video-path> <frame-number>")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fail(errors.New("INFRAI_API_KEY is required"))
	}
	if err := verifyVideo(context.Background(), os.Args[1], key); err != nil {
		fail(err)
	}

	frame, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fail(fmt.Errorf("invalid frame number: %w", err))
	}
	videoPath := os.Args[2]
	dir := filepath.Dir(videoPath)
	posterPath := filepath.Join(dir, "poster.jpg")
	tmp, err := os.CreateTemp(dir, ".poster-*.jpg")
	if err != nil {
		fail(err)
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		fail(err)
	}
	defer os.Remove(tmpPath)

	filter := fmt.Sprintf("select=eq(n\\,%d)", frame)
	cmd := exec.Command("ffmpeg", "-nostdin", "-v", "error", "-i", videoPath,
		"-vf", filter, "-frames:v", "1", "-q:v", "3", "-y", tmpPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fail(fmt.Errorf("ffmpeg: %w: %s", err, output))
	}
	info, err := os.Stat(tmpPath)
	if err != nil {
		fail(err)
	}
	if info.Size() == 0 {
		fail(errors.New("ffmpeg produced an empty poster"))
	}
	if err := os.Rename(tmpPath, posterPath); err != nil {
		fail(err)
	}
	fmt.Printf("stored %s (%d bytes)\n", posterPath, info.Size())
}

func verifyVideo(ctx context.Context, id, key string) error {
	host := "api." + "infrai" + ".cc"
	endpoint := "https://" + host + "/v1" + "/video/get/" + url.PathEscape(id)
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("video lookup returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("video lookup retries exhausted")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
```

The explicit frame number is an idempotency input, not a tuning detail. Keep it with the immutable source version. A retry must not silently choose another image because a selector or upstream default changed.

For object storage, use the same state machine with storage-native conditional writes or copy semantics. Keep both source and poster private or signed-only, and deliver them through presigned URLs. Do not attach the media API authorization header to a returned presigned URL. Local atomic rename does not extend across an object store; the invariant is that readers see the previous valid poster or the next valid poster, never a partial upload.

## Verify the artifact and the relationship

A successful process exit is not the publish condition. Confirm that the final poster exists at the deterministic key, has nonzero length, and decodes as the intended image type. The extension proves little. Browser support and format characteristics should be checked against a maintained image-format reference rather than assumed.

Then verify the relationship. The manifest should bind the video ID, immutable source version, selected frame number, poster key, and tag-set version. Those five values make a bad search result explainable and prevent a delayed retry for version 6 from attaching its poster to version 7.

Monitor signals that defend the boundary: publishes without valid posters, duplicate attempts for one immutable version, the age of the oldest unfinished publish, poster bytes grouped by format, and misses for final poster keys. The exact alert thresholds must come from the library's traffic and service objectives; no universal number is honest here.

Keep remediation away from page delivery. **A missing poster should enqueue repair or select a known fallback asset; it should never start extraction inside the request.** This is the runbook line that stops a catalog defect from turning into a latency incident.

## Roll back by moving a pointer

Version the manifest before changing frame-selection policy, JPEG quality, dimensions, or provider. Generate candidates under versioned temporary keys, validate that they decode and match the expected source version, then move the manifest pointer. Do not overwrite the whole library first.

Rollback is short: restore the prior manifest version and let its stable keys refill caches normally. Candidate objects can remain quarantined for inspection and later expire under the library's retention policy. No regeneration is required.

For a provider change, send the same fixed corpus through both adapters and compare validity, dimensions, selected-frame identity, and compressed byte size. This is where the stable application contract earns its keep. The adapter may move; the publish record, poster key scheme, and auto-tagging input do not.

## References

- [FFmpeg filter documentation](https://ffmpeg.org/ffmpeg-filters.html)
- [Cloudinary video transformations](https://cloudinary.com/documentation/video_manipulation_and_delivery)
- [Mux static renditions](https://www.mux.com/docs/guides/enable-static-mp4-renditions)
- [AWS Elemental MediaConvert documentation](https://docs.aws.amazon.com/mediaconvert/)
- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats/Image_types)
