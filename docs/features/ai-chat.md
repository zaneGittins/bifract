# AI Chat

An assistant inside each fractal that answers questions by running BQL against your logs. Use it for quick, in-app investigation; for an assistant on your own machine, see the [MCP server](mcp-server.md).

## Set up

Bifract calls the model provider directly. It supports Anthropic, OpenAI and Google Gemini, and needs an API key for one of them.

1. In your install directory, set the provider (`anthropic`, `openai` or `gemini`) in `.env`:

    ```bash
    BIFRACT_LLM_PROVIDER=anthropic
    BIFRACT_LLM_API_KEY=<your provider key>
    BIFRACT_LLM_MODEL=claude-haiku-4-5-20251001
    ```

    `BIFRACT_LLM_MODEL` is optional for Anthropic (the default is `claude-haiku-4-5-20251001`) and required for OpenAI and Gemini, for example `gpt-4.1-mini` or `gemini-2.5-flash`.

2. Restart: `docker compose up -d`.

To use an OpenAI-compatible server such as Ollama or vLLM, set `BIFRACT_LLM_PROVIDER=openai` and point `BIFRACT_LLM_BASE_URL` at it, for example `http://ollama:11434/v1`. The key is still required; any value works if the server ignores it.

On Kubernetes, set `LLM_PROVIDER`, `LLM_API_KEY`, `LLM_MODEL` and `LLM_BASE_URL` in `bifract-secrets`.

!!! note
    Earlier versions ran a LiteLLM container. `--upgrade` and `--upgrade-k8s` carry its key, model and endpoint over and remove it; the old config is kept in `.backups`. Run the upgrade before `--reconfigure`, which refuses while LiteLLM is still installed.

## Ask a question

1. Open the fractal and click **Chat** in the sidebar.
2. Click **New conversation**.
3. Set the time range in the composer (**Last 1h** to **All time**, default **Last 24h**).
4. Type in **Ask about your logs...** and press Enter.

The reply streams in, with each tool call shown inline. Click **Open in search** on a query call to run it on the **Query** page. Conversations are titled automatically and listed on the left; use **Clear** to empty one, **Delete** to remove it, or **Clear all** to remove every conversation.

To start from a single event, open it on the **Query** page and click **Analyze with AI**.

## Approve actions

Tools that change something are proposed, not run. A card shows the exact arguments with **Approve** and **Decline**; nothing happens until you click **Approve**.

| Tool | Approving it |
|------|--------------|
| `add_comment`, `add_tag`, `remove_tag` | Writes a comment or changes its tags |
| `create_notebook`, `add_notebook_section` | Writes to a notebook |
| `create_alert`, `update_alert` | Creates or changes a live detection |
| `search_archive` | Starts an archive scan (minutes, reads object storage) |

The server holds the arguments, so what the card shows is what runs. The action runs with your own role, so approving never grants more than you have. Unanswered proposals expire after 15 minutes.

## What the assistant can read

Tools run as you, with your role and scope checks. The time range from the composer replaces any range the assistant asks for.

| Area | Tools |
|------|-------|
| Query | `query_logs`, `validate_bql`, `get_fields`, `get_field_stats`, `get_recent_logs`, `get_bql_reference` |
| Detections | `list_alerts`, `get_alert`, `get_alert_executions`, `get_alert_tests`, `run_alert_tests`, `get_alert_policies`, `get_attack_coverage` |
| Investigation | `find_processes`, `get_provenance_graph` |
| Enrichment | `list_dictionaries`, `get_dictionary`, `search_dictionary`, `list_models`, `get_model`, `get_model_data` |
| Collaboration | `list_comments`, `get_log_comments`, `list_comment_tags`, `list_notebooks`, `get_notebook`, `list_saved_queries`, `list_dashboards`, `get_dashboard` |
| Instructions | `list_instruction_libraries`, `get_instruction_library`, `read_instruction_page` |
| Archive | `get_archive_search`, `cancel_archive_search` |

`think`, `render_chart` and `present_results` only draw in the conversation.

!!! note "Why some MCP tools are missing"
    Tool results contain log data, which anyone who can send logs can influence. Chat therefore leaves out tools a hidden instruction could abuse for lasting effect: `delete_alert` (a removed detection fails silently), `add_dictionary_rows` (watchlists feed detections), and the instruction-page writes (those pages become part of the assistant's prompt). Use the UI or the MCP server for those.

## Give the assistant context

Each fractal has one instruction library: markdown pages with folders and `[[wiki links]]` for your conventions, runbooks and known-good baselines. Every conversation in the fractal uses it.

1. Click **Instructions** in the sidebar.
2. Click **New page** (or **New folder**) and write the page.
3. Tick **Always include (pinned)** for pages the assistant should always see. Other pages are listed for it to open with `read_instruction_page` when relevant.

To keep pages in Git, open **Library settings**, set **Source** to **Git Repository**, and fill in **Repository URL** and **Branch**. Put credentials in **Auth Token**, not the URL; moving the URL to another host clears the token. Repositories inside your network need **Settings** > **Security** > **Webhooks and Git repositories on the internal network**.

!!! tip
    Importing an [alert feed](../alerting/alert-feeds.md) gives the assistant your detection rules as context for writing BQL.
