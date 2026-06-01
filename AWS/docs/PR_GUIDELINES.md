# PR Guidelines

This document covers commit format, branch naming, PR structure, the pre-merge checklist, and the code-to-doc mapping rule for the AWS V2 platform.

---

## 1. Commit Message Format

All commits follow **Conventional Commits** (`type(scope): description`), consistent with the project's existing history.

```
type(scope): short imperative description

Optional body: why this change was made, not what it does.
```

### Types

| Type | When to use |
|---|---|
| `feat` | New capability added (new workflow step, new MCP tool, new scraper) |
| `fix` | Corrects a bug or wrong output without adding new behaviour |
| `refactor` | Code restructured — no behaviour change, no bug fixed |
| `chore` | Config, deps, infra, or tooling changes with no production impact |
| `docs` | Documentation only |
| `test` | Test files only |

### Scopes

Scope maps to the affected subsystem. Use the canonical names below; multi-scope is allowed for tightly coupled changes.

| Scope | Maps to |
|---|---|
| `ad-diag` | `src/workflows/definitions/ad_diagnosis.py` and its processors |
| `lp` | LP optimisation logic (`optimizer_ad_budget.py`, `lp_calibration.py`) |
| `monopoly` | `src/workflows/definitions/category_monopoly_analysis.py` |
| `causal` | `src/intelligence/processors/causal_analysis.py` |
| `telemetry` | `src/core/telemetry/` |
| `erp` | `src/mcp/servers/erp/` |
| `feishu` | `src/entry/feishu/` and Feishu callbacks |
| `ads` | Amazon Ads API client (`src/mcp/servers/amazon/ads_*`) |
| `auth` | `src/gateway/auth.py` |
| `core` | `src/core/` (data_cache, scraper, storage, models) |
| `prompts` | `src/intelligence/prompts/` and YAML config |
| `intelligence` | `src/intelligence/router/` and providers |

**Examples:**
```
feat(lp): add per-match-type k_max via Beta precision calibration
fix(ad-diag): correct SP order attribution and organic_daily computation
refactor(ad-diag): migrate deterministic logic from YAML prompt into Python
chore(core): bump curl_cffi to 0.7.1
docs(pr): add PR guidelines
```

### Description Rules

- Imperative mood, present tense: "add", "fix", "remove" — not "added" or "fixes"
- No period at the end
- Under 72 characters
- If the change is non-obvious, add a blank line and a body explaining **why**, not what

---

## 2. Branch Naming

```
type/scope-short-description
```

| Branch | For |
|---|---|
| `feat/lp-match-type-calibration` | New feature |
| `fix/ad-diag-sp-attribution` | Bug fix |
| `refactor/ad-diag-deterministic-python` | Refactor |
| `chore/bump-curl-cffi` | Dependency / infra |
| `docs/pr-guidelines` | Docs only |

Use hyphens, not underscores. Keep the description short enough to be readable in a branch list.

---

## 3. PR Size & Scope

**One concern per PR.** A PR should do exactly one of: add a feature, fix a bug, refactor a module, or update docs. Mixed-type PRs are hard to review and hard to revert.

| Situation | Guidance |
|---|---|
| Related bug fixes found while building a feature | Open a separate `fix/` PR first, then base the feature branch on it |
| Multiple report accuracy fixes in the same workflow | Acceptable to batch if they all touch the same file and are causally related (matches existing history: "14 report accuracy fixes") |
| Refactor + feature | Split — refactor merges first; feature branch rebases on top |
| Docs + code | Docs for the changed code belong in the same PR (see §5 checklist) |

**Size heuristic:** If a PR diff touches more than three unrelated files or more than ~400 lines of logic, consider splitting.

---

## 4. PR Title & Description

The PR title mirrors the commit message: `type(scope): description`.

The description must include:

```markdown
## What
One sentence: what changed and in which module.

## Why
The motivation — a bug report, accuracy regression, missing capability, or
external API change. Link to a Feishu thread or issue if one exists.

## Test plan
- [ ] `PYTHONPATH=. pytest tests/test_<relevant>.py -v`
- [ ] Live test if Redis required: `PYTHONPATH=. python3 tests/test_<name>_live.py`
- [ ] Manual: describe any Feishu bot command or CLI invocation used to verify

## Docs updated
- [ ] Not required (no code-to-doc mapping triggered — see PR Guidelines §5)
- [ ] Updated: list which doc files were changed
```

---

## 5. Pre-Merge Checklist

Run all applicable items before marking a PR ready for review.

### Always

- [ ] `PYTHONPATH=. pytest tests/test_imports.py` passes (no circular imports)
- [ ] `PYTHONPATH=. pytest tests/test_core_models.py tests/test_core_utils.py` passes
- [ ] No secrets, API keys, or `.env` values in the diff
- [ ] No `print()` debug statements left in production code (use `logger.debug()`)

### If logic changed

- [ ] Relevant unit tests updated or added (`tests/test_{domain}_{feature}.py`)
- [ ] `PYTHONPATH=. pytest tests/test_rate_limiting_system.py` passes if gateway touched
- [ ] `PYTHONPATH=. pytest tests/test_workflow_engine.py tests/test_checkpoint_resume.py` passes if workflow engine touched

### If a pricing JSON was updated

- [ ] `PYTHONPATH=. pytest tests/test_gemini_advanced_pricing.py` passes
- [ ] `PYTHONPATH=. pytest tests/test_intelligence_pricing.py` passes
- [ ] `last_verified` date updated in the modified JSON

### If a live-data test exists for the changed area

- [ ] `PYTHONPATH=. python3 tests/test_<name>_live.py` run against a dev Redis instance

---

## 6. Code-to-Doc Mapping

When a PR changes code in these paths, the corresponding doc **must be updated in the same PR**. A PR that skips this is incomplete.

| Code path changed | Doc to update |
|---|---|
| `src/workflows/definitions/` | `docs/ARCHITECTURE.md`, `docs/DEV_GUIDE.md` |
| `src/mcp/servers/` | `docs/MCP_PROTOCOL.md` |
| `src/intelligence/providers/config/*_pricing.json` | `docs/LLM_GUIDELINES.md` |
| `src/intelligence/providers/price_manager.py` | `docs/LLM_GUIDELINES.md` |
| `config/settings.json` (rate limits / scraper config) | `docs/DEV_GUIDE.md` (rate limiting table) |
| `config/workflow_defaults.yaml` | `docs/INPUT_SCHEMA.md` |
| `tests/` (new test files) | `docs/TESTING.md` (Section 3 test categories) |
| `src/entry/feishu/commands.py` | `docs/DEV_GUIDE.md` (Feishu commands table) |
| `src/core/storage/` (new backend) | `docs/DEV_GUIDE.md` §8 |
| `src/mcp/servers/erp/` (new provider) | `docs/DEV_GUIDE.md` §2 Layer 4C |
| `src/core/errors/codes.py` (new provider or code) | `docs/DEV_GUIDE.md` §5, `docs/TROUBLESHOOTING.md` §11 |

---

## 7. Review Criteria

Reviewers check the following. Authors should self-review against this list before requesting review.

**Correctness**
- Logic matches the stated intent in the PR description
- Edge cases handled: empty lists, `None` fields, network timeouts, Redis unavailable
- No silent data loss (failed writes must log `ERROR`, not pass silently)

**Domain Isolation**
- No cross-domain imports (`src/mcp/servers/amazon/` must not import from `src/mcp/servers/erp/`)
- L1 code only writes to `DataCache`; L2 code only reads from it

**Async discipline**
- All I/O is `async`; no blocking calls (`time.sleep`, `requests.get`) in async paths
- `try/finally` used for any resource that must be released (concurrency slots, file handles)

**Logging**
- New code uses `logger = logging.getLogger(__name__)` — no `print()`, no `basicConfig` in domain modules
- Failures log at `ERROR`; degraded-but-continuing paths log at `WARNING`
- No credentials or raw response bodies logged

**Test coverage**
- New MCP tools have `inputSchema` validated in a test
- New workflow steps have at least one unit test for the happy path and one for the failure/empty path
