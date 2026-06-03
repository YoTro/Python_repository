---
name: Feature Request
about: Propose a new capability, workflow step, MCP tool, or integration
title: "feat(<scope>): <short description>"
labels: enhancement
assignees: ""
---

## Problem / motivation

<!-- What gap does this fill? Link to a Feishu thread or prior issue if one exists. -->

## Proposed solution

<!-- How should it work? Be specific about inputs, outputs, and which layer it lives in. -->

## Affected layer

<!-- Where does this change land in the dual-track architecture? -->
- [ ] Entry point — new channel adapter (`src/entry/`)
- [ ] Gateway — rate limit rule, auth extension (`src/gateway/`)
- [ ] Workflow — new step or definition (`src/workflows/`)
- [ ] Agent — system prompt, constraint (`src/agents/`)
- [ ] MCP tool — new scraper or calculator (`src/mcp/servers/`)
- [ ] Intelligence — new LLM provider or processor (`src/intelligence/`)
- [ ] ERP provider — new ERP integration (`src/mcp/servers/erp/`)
- [ ] Core — data_cache, storage backend, models (`src/core/`)
- [ ] Other: <!-- describe -->

## Alternatives considered

<!-- What else was evaluated and why it was ruled out. -->

## Acceptance criteria

- [ ] <!-- specific, verifiable condition 1 -->
- [ ] <!-- specific, verifiable condition 2 -->
- [ ] Unit tests cover happy path and failure/empty path
- [ ] Relevant docs updated per code-to-doc mapping (see `docs/PR_GUIDELINES.md` §6)
