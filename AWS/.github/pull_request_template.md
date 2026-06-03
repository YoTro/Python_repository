## What

<!-- One sentence: what changed and in which module. -->

## Why

<!-- The motivation — bug report, accuracy regression, missing capability, or external API change.
     Link to a Feishu thread or issue if one exists. -->

## Test plan

- [ ] `PYTHONPATH=. pytest tests/test_imports.py` passes
- [ ] `PYTHONPATH=. pytest tests/ -m "not live and not redis"` passes
- [ ] Live test run (if applicable): `PYTHONPATH=. python3 tests/test_<name>_live.py`
- [ ] Manual: <!-- describe any Feishu command or CLI invocation used to verify -->

## Docs updated

- [ ] Not required — no code-to-doc mapping triggered (see `docs/PR_GUIDELINES.md` §6)
- [ ] Updated: <!-- list which doc files were changed -->

## Pre-merge checklist

- [ ] Commit title follows Conventional Commits: `type(scope): description`
  - Types: `feat` `fix` `refactor` `chore` `docs` `test`
  - Scopes: `ad-diag` `lp` `monopoly` `causal` `telemetry` `erp` `feishu` `ads` `auth` `core` `prompts` `intelligence` `screening`
- [ ] No secrets, API keys, or `.env` values in the diff
- [ ] No `print()` debug statements — use `logger.debug()` instead
- [ ] New errors use `ErrorCode` from `src/core/errors/codes.py`; no raw HTTP status comparisons in callers
- [ ] New MCP tools have `inputSchema` validated in a test
- [ ] New workflow steps have a unit test for the happy path and one for the failure/empty path
