.PHONY: ci test pre-commit lint typecheck security

PYTHON = venv311/bin/python
PYTEST = PYTHONPATH=. $(PYTHON) -m pytest

# ── Full CI suite (matches .github/workflows/ci.yml) ─────────────────────────
ci: pre-commit test rate-limit lint typecheck security

# ── Individual jobs ───────────────────────────────────────────────────────────
test:
	$(PYTEST) tests/ -m "not live and not redis" -v --tb=short \
	  --cov=src --cov-report=term-missing

rate-limit:
	PYTHONPATH=. $(PYTHON) -m unittest tests/test_rate_limiting_system.py -v

pre-commit:
	$(PYTHON) -m pre_commit run --all-files

lint:
	venv311/bin/ruff check src/ tests/
	venv311/bin/ruff format --check src/ tests/

typecheck:
	$(PYTHON) -m mypy src/core/ --follow-imports=silent

security:
	$(PYTHON) -m bandit -r src/ -ll -ii -c pyproject.toml
