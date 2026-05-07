# What the AI sees

datasight sends information to an LLM to translate your question into SQL. This page
explains exactly what is and isn't included in those calls.

## What datasight sends

| Item | Why it's sent |
|---|---|
| Table and column names | The AI needs the schema to write correct SQL |
| Column data types | Helps the AI pick appropriate aggregations and filters |
| Row counts per table | Helps the AI reason about the size and shape of the data |
| Your `schema_description.md` | The plain-English context you wrote to explain your data |
| Your `queries.yaml` examples | Shows the AI the correct SQL patterns for your schema |
| Small result samples | Used to generate a plain-English answer after the query runs (typically a few rows) |
| Your natural-language question | The text you typed in the chat input |

## What datasight does NOT send

- **Full table contents.** Raw data values are never uploaded. Only the small
  result-row samples used to summarize an answer reach the LLM.
- **Your `.env` file or API keys.** Configuration is read locally and never transmitted.
- **Raw files.** If you load a CSV or Parquet file, the file stays on your machine.
  The AI only sees the inferred column names and types.
- **Filesystem paths** beyond the table and column names introspected from the database.
- **Other files or directories** outside the open project.

## Column names and samples can still be sensitive

Even though full data is never sent, column names like `patient_id` or `salary_usd` and
sampled result rows may themselves be sensitive. Treat the schema description and any
example queries you write as data-sensitivity artifacts — the AI will see them.

## Hosted APIs vs. local models

When datasight calls a hosted API (Anthropic, OpenAI, GitHub Models), the schema
information and result samples leave your machine over the internet.

If your data sensitivity requirements prohibit that, two options keep everything local:

- **Local Ollama** — runs a model on your hardware; nothing leaves the machine.
- **Secure hosted endpoint** — Anthropic on AWS Bedrock, Azure OpenAI, or a corporate
  gateway with a data-processing agreement.

See [Choosing an LLM](choosing-an-llm.md) for guidance on which option fits your
situation.
