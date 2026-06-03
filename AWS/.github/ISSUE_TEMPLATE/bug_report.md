---
name: Bug Report
about: Something is broken or producing wrong output
title: "fix(<scope>): <short description>"
labels: bug
assignees: ""
---

## What happened

<!-- One sentence: what broke and where. -->

## Expected behaviour

<!-- What should have happened. -->

## Steps to reproduce

```bash
# Minimal command or code snippet that triggers the bug
PYTHONPATH=. python main.py --workflow product_screening --params '{"keyword": "..."}'
```

## Logs / stack trace

<details>
<summary>Expand</summary>

```
paste relevant log lines here
```

</details>

## Environment

| Field | Value |
|---|---|
| Python version | <!-- e.g. 3.11.3 --> |
| Branch / commit | <!-- git rev-parse --short HEAD --> |
| Entry point | <!-- CLI / Feishu bot / Claude Desktop --> |
| LLM provider | <!-- gemini / claude / deepseek / local --> |
| Redis in use | <!-- yes / no --> |

## Affected scope

<!-- Tick the subsystem(s) involved -->
- [ ] `core` — data_cache, scraper, storage, models, errors
- [ ] `gateway` — auth, rate limiting, router
- [ ] `intelligence` — LLM providers, router, processors
- [ ] `workflows` — engine, steps, definitions
- [ ] `agents` — MCP agent, session
- [ ] `erp` — Lingxing or other ERP provider
- [ ] `ads` — Amazon Ads API client
- [ ] `feishu` — bot listener, commands, callbacks
- [ ] `mcp-servers` — Amazon scrapers, market, social, output
- [ ] `lp` — LP optimisation / calibration
- [ ] `ad-diag` — ad diagnosis workflow
- [ ] Other: <!-- describe -->
