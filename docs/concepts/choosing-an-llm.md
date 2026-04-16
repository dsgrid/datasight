# Choosing an LLM

datasight works with any of several LLM backends. The right choice depends on
your data sensitivity, budget, and whether you want to run a model locally.
This page helps you pick one without reading every provider's pricing page.

## Quick decision guide

| Your situation | Start with |
|---|---|
| Trying datasight for the first time, non-sensitive data | **Anthropic Claude Haiku** or **OpenAI GPT-4o-mini** |
| Want zero cost, don't mind rate limits | **GitHub Models** (free tier) |
| Already have an OpenAI key | **OpenAI** (`gpt-4o-mini` or `gpt-4.1-mini`) |
| Data is sensitive and must not leave your network | **Local Ollama** (laptop or HPC GPU node) |
| Data is sensitive but you have a secure hosted endpoint | **Anthropic on Bedrock** or **Azure OpenAI** (custom `base_url`) |
| Writing SQL for a well-documented schema | **Haiku** / **GPT-4o-mini** is usually enough |
| Complex multi-step analytical questions, poor results from the cheap tier | Step up to **Sonnet** or **GPT-4o** |

When in doubt, start with Haiku. datasight's main job — turning a question
into SQL against a documented schema — is not a frontier-model task, and
Haiku handles it well for most projects.

## Factor 1: data sensitivity

This is the first question to answer, because it rules some options out.

- **Non-sensitive or already-public data.** Any hosted provider is fine.
  Only the SQL and sampled result rows leave your machine; datasight does
  not upload raw files.
- **Sensitive data where a hosted API is acceptable under a BAA or
  enterprise agreement.** Use a secure endpoint such as Anthropic on AWS
  Bedrock, Azure OpenAI, or a corporate gateway. Configure datasight with
  the provider's `base_url` pointing at your endpoint.
- **Sensitive data that must not traverse the public internet at all.**
  Run a local model via Ollama — on your laptop, or on an HPC GPU node
  (see below).

Note: even with a hosted API, the *data values* that reach the LLM are
limited to column names, schema descriptions, example queries, and small
result samples used for summarization. Full tables are never uploaded.
That said, column names and sample rows can themselves be sensitive, so
treat them accordingly.

## Factor 2: cost

For the hosted options, rough order of magnitude (check current pricing —
these move):

- **GitHub Models** — free for a generous monthly quota, rate-limited.
  Great for evaluation and light use. Provides access to GPT, Llama, and
  other open models through a single GitHub token. **Note:** the free
  tier caps requests at 8,000 tokens, which is easy to exceed on databases
  with many tables or wide tables. If you hit context-length errors, see
  [Limit schema sent to the LLM](../project-developer/schema-config.md).
- **Cheap hosted tier** (Anthropic Haiku, OpenAI GPT-4o-mini / GPT-4.1-mini)
  — typical datasight sessions cost pennies to single-digit cents.
- **Mid hosted tier** (Anthropic Sonnet, OpenAI GPT-4o) — roughly 5× the
  cheap tier. Noticeably better at ambiguous questions and multi-step
  reasoning.
- **Top hosted tier** (Anthropic Opus, OpenAI's largest model) — roughly
  5× the mid tier. **Rarely needed for datasight's workload.** If the
  mid tier is struggling on your schema, better schema descriptions and
  example queries usually help more than jumping a tier.

A practical starting rule: use the cheap tier until you can point to
specific questions it gets wrong, then try the mid tier on just those.

## Factor 3: local models with Ollama

Local models cost nothing per query, keep data on your hardware, and work
offline — at the price of needing GPU memory and slower inference than
hosted APIs.

### Sizing rule of thumb

VRAM needed ≈ **model parameter count × bytes per parameter**, plus some
overhead for context.

- **4-bit quantized** (Ollama default): ~0.5 GB per billion parameters
- **8-bit**: ~1 GB per billion parameters
- **16-bit (fp16)**: ~2 GB per billion parameters

So a Llama 3.1 8B model fits in ~5 GB VRAM at 4-bit, a 70B model needs
~40 GB, and a 405B model needs ~200+ GB.

### On a laptop

| Laptop hardware | What fits comfortably |
|---|---|
| Apple Silicon with 16 GB unified memory | 7–8B models at 4-bit |
| Apple Silicon with 32 GB | 13B at 4-bit, or 8B at 8-bit |
| Apple Silicon with 64 GB+ | 34–70B at 4-bit |
| NVIDIA laptop GPU, 8 GB VRAM | 7–8B at 4-bit |
| NVIDIA laptop GPU, 16 GB VRAM | 13B at 4-bit |

For datasight's SQL-generation workload, an 8B model (e.g. Llama 3.1 8B
or Qwen 2.5 Coder 7B) is a reasonable floor. Smaller models often
struggle with realistic schemas.

### On an HPC GPU node

If your HPC has GPU nodes, they typically unlock much larger models.

**NLR Kestrel as a concrete example.** Kestrel has 156 GPU nodes, each
with 4 NVIDIA H100 SXM GPUs (80 GB VRAM each, 320 GB per node) and
384–1536 GB system RAM. On a single Kestrel GPU node you can run:

- Llama 3.1 70B at fp16 (~140 GB) with headroom to spare
- Llama 3.1 405B at 4-bit quantization (~200 GB) across the 4 GPUs
- Multiple mid-sized models concurrently

Kestrel's `debug` partition lets you request up to half a GPU node for
4 hours without a large allocation — a practical way to try local
models before committing resources.

See [Run on an HPC compute node](../end-user/how-to/run-on-hpc.md) for the
deployment pattern (datasight runs on the compute node, tunnels back to
your laptop browser).

### When hosted beats local

A hosted cheap-tier call (Haiku or GPT-4o-mini) often produces better
SQL than a locally-run 8B model, at a fraction of a cent. Don't reach
for local models just to avoid hosted costs — reach for them when data
sensitivity or offline use requires it.

## Factor 4: where the LLM call originates

datasight makes its LLM calls from wherever the datasight process is
running. That matters when you're combining a remote data backend with
any kind of policy or network constraint:

- **datasight on your laptop + local data** — LLM call from laptop.
- **datasight on an HPC compute node** — LLM call from the compute node.
  Good fit if you want to use the compute node's GPU for a local model,
  or if hosted API keys are configured there.
- **datasight on your laptop + remote Flight SQL backend on HPC** — LLM
  call from laptop, SQL executed on HPC. Good fit if your laptop has the
  GPU you want to use, or if compute-node egress to hosted APIs is
  blocked.

See the two HPC how-tos for the tradeoffs:
[Run on an HPC compute node](../end-user/how-to/run-on-hpc.md) and
[Connect to a remote Flight SQL backend](../end-user/how-to/connect-flight-sql.md).

## Configuring your choice

Once you've picked a provider, see the
[Install and configure an LLM](../end-user/how-to/install.md) how-to for
the exact environment variables. The short version:

```bash
# Anthropic (default)
ANTHROPIC_API_KEY=sk-ant-...

# OpenAI
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...

# GitHub Models
LLM_PROVIDER=github
GITHUB_TOKEN=ghp-...

# Ollama (local)
LLM_PROVIDER=ollama
OLLAMA_MODEL=llama3.1:8b
```

A secure hosted endpoint (Bedrock, Azure OpenAI, corporate proxy) is
configured by setting `ANTHROPIC_BASE_URL` or `OPENAI_BASE_URL` alongside
the credentials.
