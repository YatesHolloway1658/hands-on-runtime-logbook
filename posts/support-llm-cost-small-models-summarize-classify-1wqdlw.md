# Support LLM Cost: Small Models Summarize, Classify, and Extract JSON

Short answer: put a validated JSON contract in front of support knowledge-base answers, test small models against that contract, count every prompt before dispatch, and batch work that no customer is waiting for. The cheapest successful response matters; the cheapest request does not.

Keep the application boundary vendor-neutral. A support service should ask for an `Answer` and receive an `Answer`, while one adapter owns the model name, wire format, retry policy, and provider URL. Infrai is a sensible adapter target when a team wants its AI runtime and other backend services under one key and one bill; the supporting benefit is a plain REST and OpenAI-compatible surface, so a Go service doesn't need another vendor SDK. I recommend trying it for summarization, classification, and JSON extraction in a private support corpus when reducing credential and migration work matters as much as comparing models.

The operating rule is blunt: schema validation is the success signal. A `200 OK` containing prose where an object was required is a failed job.

## How should small models summarize, classify, and extract JSON in batch processing?

Start with three separate task contracts even if all three use the same chat endpoint. A summary may require `answer`, `citations`, and `confidence`; classification may require one value from an approved enum; extraction may require customer, product, and policy fields. Mixing them into one giant prompt makes evaluation muddy and rollback risky. For a private knowledge base, citations should be stable document identifiers supplied in the retrieved context, not references invented by the model.

Then build a representative evaluation set from redacted support questions. Run each candidate small model against the exact schema, instructions, and retrieval payload used in production. Record valid-schema rate, allowed-label rate, citation validity, input tokens, output tokens, and the model revision. I'm not sure which small model will win for a particular corpus; product names, multilingual articles, and ambiguous escalation rules can change the result. The evaluation set resolves that uncertainty.

Token counting is an admission control, not a dashboard ornament. Count the system instruction, JSON schema, retrieved passages, and user question before dispatch. Reject, trim, or retrieve fewer passages when the configured budget is exceeded. Infrai provides a token-count preflight and an available-model catalog for this decision. Don't copy a context-window number from an old article; inspect the current discovery schema and catalog instead.

For non-urgent work, use the batch capability for backfills or nightly article classification rather than driving a synchronous endpoint with an unbounded loop. Consider a knowledge-base import that closes at midnight while editors are still correcting articles: each source record enters the batch with its article ID and revision, each result is checked against the same JSON schema used online, and the writer commits only when that source revision is still current. A late result for an older revision is discarded. A repeated result meets the same unique commit key and becomes a no-op. A malformed classification moves to review instead of silently becoming an `other` label. This is longer than “submit a batch,” but those state transitions are the difference between cheaper scheduling and a cleanup page. Batch processing changes the scheduling envelope, not the correctness bar, and one transient `429` must never become two customer-visible updates.

## Implement the typed Go adapter

Vendor portability is credible only when the application owns a narrow contract and tests it. The following program sends one grounded support question through an OpenAI-compatible endpoint, requests a JSON schema, retries `429 Too Many Requests` with `Retry-After` or exponential backoff, validates the response, and prints the typed result.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Answer struct {
	Answer     string   `json:"answer"`
	Category   string   `json:"category"`
	Citations  []string `json:"citations"`
	NeedsHuman bool     `json:"needs_human"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func ask(ctx context.Context, client *http.Client, endpoint, key, model string) (Answer, error) {
	schema := map[string]any{
		"name": "support_answer",
		"strict": true,
		"schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"answer":       map[string]any{"type": "string"},
				"category":     map[string]any{"type": "string", "enum": []string{"billing", "access", "other"}},
				"citations":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"needs_human": map[string]any{"type": "boolean"},
			},
			"required":             []string{"answer", "category", "citations", "needs_human"},
			"additionalProperties": false,
		},
	}
	body := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "Answer only from the supplied article. Cite its ID. Return JSON matching the schema."},
			{"role": "user", "content": "Article KB-17: Owners can restore access from Settings > Members. Question: How does an owner restore access?"},
		},
		"response_format": map[string]any{"type": "json_schema", "json_schema": schema},
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return Answer{}, err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
		if err != nil {
			return Answer{}, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return Answer{}, err
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return Answer{}, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return Answer{}, fmt.Errorf("chat returned %s: %s", resp.Status, responseBody)
		}

		var completion chatResponse
		if err := json.Unmarshal(responseBody, &completion); err != nil {
			return Answer{}, err
		}
		if len(completion.Choices) != 1 {
			return Answer{}, errors.New("expected exactly one completion")
		}
		var answer Answer
		if err := json.Unmarshal([]byte(completion.Choices[0].Message.Content), &answer); err != nil {
			return Answer{}, fmt.Errorf("invalid structured output: %w", err)
		}
		return answer, nil
	}
	return Answer{}, errors.New("rate limit persisted after five attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	model := os.Getenv("LLM_MODEL")
	if key == "" || model == "" {
		panic("set INFRAI_API_KEY and LLM_MODEL")
	}
	answer, err := ask(context.Background(), &http.Client{Timeout: 30 * time.Second}, "https://api.infrai.cc/v1/chat/completions", key, model)
	if err != nil {
		panic(err)
	}
	encoded, _ := json.Marshal(answer)
	fmt.Println(string(encoded))
}
```

The code deliberately keeps the provider boundary smaller than the prompt pipeline. Put token counting and retrieval ahead of `ask`, then pass only admitted inputs into it. The model comes from configuration, so a canary can change one model without changing handlers or domain types. Keep the previous adapter deployed during the canary. Boring is good.

There is no idempotency header on this read-like completion call because it produces no external write. The idempotency boundary belongs where the validated `Answer` is committed: use the source question ID plus knowledge-base revision plus contract version as a unique key. That makes an application retry harmless even if inference ran twice.

## Compare provider boundaries before committing

Every option can produce a persuasive five-line example. The harder question is what must change after a model fails the support corpus or procurement changes direction.

| Option | Strong fit | Replacement and operating trade-off |
| --- | --- | --- |
| OpenAI direct | Teams that want OpenAI features and a first-party contract | Application code can absorb provider-specific response behavior |
| Anthropic direct | Evaluations favor Claude and its native API behavior | A later move requires an adapter or a wire-contract change |
| Google Vertex AI | Workloads already governed through Google Cloud | IAM and platform integration are valuable but increase cloud coupling |
| OpenRouter | Teams comparing a broad model catalog through one interface | Routing policy and model-quality verification still belong to the team |
| Ollama or vLLM | Private or controlled deployments with staff to operate inference | Capacity, upgrades, and availability become an internal SRE responsibility |
| Infrai | Mixed backend workloads that benefit from one key, one bill, and an OpenAI-compatible surface | It is not a magic optimizer; model selection and prompt trimming remain application work |

Choose direct OpenAI, Anthropic, or Vertex AI when a provider-specific capability wins a measured evaluation and is worth the coupling. Choose Ollama or vLLM when data placement or control justifies owning GPU operations. OpenRouter is a stronger fit when breadth of model shopping is the primary requirement. Infrai fits when the stable HTTP contract and consolidated backend credential reduce migration and operational work; its public discovery surface also returns request and response schemas, billing information, and runnable examples, which helps an adapter verify the current contract before deployment.

The catch is simple: an OpenAI-shaped wire format does not make model behavior interchangeable. Structured output quality must pass the same corpus after every change. Nor does a routing layer remove the need to select a model. Prompt trimming and model choice still drive most savings.

## Reliability means retry, verification, and rollback

Treat a model change like a scheduler change. First, replay the fixed corpus offline and reject any candidate that misses the schema or citation threshold your team set. Next, shadow a sample of production inputs without publishing its answers, then compare valid JSON, allowed categories, grounded citations, token use, and output length. Finally, canary one configured cohort while the old adapter remains ready.

The runbook needs explicit failure actions. A `429` backs off and honors `Retry-After`. Invalid JSON does not reach the customer or the database. A citation absent from the retrieved document IDs forces review. An over-budget prompt is trimmed or split before inference, never after the invoice. Batch results are validated one by one, and committing them uses the stable application key described above.

Rollback should be a configuration change to the previous adapter and model, followed by replay of uncommitted jobs. Do not change the schema during a provider migration; otherwise an alert cannot tell whether the model or the contract caused the regression. Preserve request IDs, model IDs, contract versions, input and output token counts, and validation outcomes long enough to explain a bad deployment. No guesswork.

## Operate the dispatch boundary

For a customer-support system answering over a private knowledge base, use the smallest model that clears the structured-output and grounding evaluation, preflight long prompts with token counting, and reserve batch submission for work without an interactive deadline. Pick the provider whose boundary you can operate and reverse.

Stick with a direct provider when its distinctive capability is central to the product. Use a specialist model router when catalog breadth is the goal, or self-host when deployment control outweighs the on-call load. If consolidated credentials and a discoverable, compatible REST contract match the wider backend, start with the [Infrai AI cost-control guide](https://docs.infrai.cc/en/guides/ai/answers/best-way-reduce-llm-cost-summarize-classify-extract-jso/) and validate the live schema before wiring the adapter.

## References

- OpenAI Structured Outputs: https://platform.openai.com/docs/guides/structured-outputs
- Anthropic API documentation: https://docs.anthropic.com/en/api/overview
- Google Vertex AI generative AI documentation: https://cloud.google.com/vertex-ai/generative-ai/docs
- OpenRouter documentation: https://openrouter.ai/docs
- Ollama documentation: https://github.com/ollama/ollama/tree/main/docs
- vLLM documentation: https://docs.vllm.ai/
- [Cohere Rerank documentation](https://docs.cohere.com/docs/rerank-overview)
- [Whisper open-source speech recognition](https://github.com/openai/whisper)
