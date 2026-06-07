.PHONY: ci test pre-commit lint typecheck security

PYTEST = PYTHONPATH=. venv311/bin/pytest

# ── Full CI suite (matches .github/workflows/ci.yml) ─────────────────────────
ci: pre-commit test rate-limit lint typecheck security

# ── Individual jobs ───────────────────────────────────────────────────────────
test:
	$(PYTEST) tests/ -m "not live and not redis" -v --tb=short \
	  --cov=src --cov-report=term-missing

rate-limit:
	PYTHONPATH=. venv311/bin/python -m unittest tests/test_rate_limiting_system.py -v

pre-commit:
	venv311/bin/pre-commit run --all-files

lint:
	venv311/bin/ruff check src/ tests/
	venv311/bin/ruff format --check src/ tests/

typecheck:
	venv311/bin/mypy src/core/ --follow-imports=silent

security:
	venv311/bin/bandit -r src/ -ll -ii -c pyproject.toml
