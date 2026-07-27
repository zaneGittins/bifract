# AI Chat

LLM-powered chat assistant scoped to each fractal. It queries your logs using BQL, discovers fields, and presents structured findings in a conversational interface.

![AI Chat conversation](../images/ai.png)

## Setup

Chat requires a [LiteLLM](https://docs.litellm.ai/) proxy container and an API key for at least one supported provider (OpenAI, Anthropic, etc). AI keys are not configured during initial setup; add them manually to your `.env` file after installation.

### 1. Add your API key to .env

Open the `.env` file in your install directory and set `LITELLM_API_KEY` to your provider key:

```bash
LITELLM_API_KEY=sk-ant-...
```

### 2. Configure a model (optional)

The default `litellm-config.yaml` uses Anthropic. To use a different provider, edit the file:

```yaml
model_list:
  - model_name: bifract-chat
    litellm_params:
      model: openai/gpt-4o-mini
      api_key: os.environ/LITELLM_API_KEY
```

Change the `model` field to match your provider. The model name must stay `bifract-chat`.

### 3. Restart the stack

```bash
docker compose up -d
```

LiteLLM runs on the internal Docker network only and is not exposed to the host.

## Features

- **Per-fractal conversations** scoped to the selected fractal's log data
- **Streaming** responses token-by-token via SSE
- **Time range control** from a selector in the chat header
- **Multiple conversations** with create, rename, and delete support
- **Search integration** by clicking the magnifying glass on any query tool call
- **Custom instructions** that shape how the assistant behaves in this deployment

### Tools

The assistant has these tools available:

| Tool | Purpose |
|------|---------|
| `run_query` | Execute a BQL query against the fractal |
| `validate_bql` | Check a query parses before running it |
| `get_fields` | Discover which fields exist in the logs |
| `search_alerts` | Look up configured detection alerts for context |
| `present_results` | Render findings as a structured result set |
| `render_chart` | Render a visualization from query output |
| `read_instruction_page` | Read a page from an attached instruction library |
| `think` | Reason through a multi-step investigation before acting |

### Instruction Libraries

A **Library** is a set of markdown pages (organized in folders, with wiki-style `[[links]]` between them) that you attach to a conversation to give the assistant durable, environment-specific knowledge: your naming conventions, escalation procedures, known-good baselines, or triage runbooks. Manage them from the fractal's **Library** tab, and attach one or more to a conversation so the assistant can read them via `read_instruction_page`.

Libraries can also be synced from a Git repository, so runbooks stay version-controlled alongside the rest of your detection content.

!!! tip
    Importing an [alert feed](../alerting/alert-feeds.md) gives the assistant context on your detection rules, enabling it to write more relevant BQL queries for your environment.

## Supported Providers

Any provider supported by LiteLLM works. Change the `model` field in `litellm-config.yaml` to match your provider (e.g. `openai/gpt-4o-mini`, `anthropic/claude-haiku-4-5-20251001`). Set `LITELLM_API_KEY` to the corresponding API key. Some providers may need `drop_params: true` in `litellm_params`.
