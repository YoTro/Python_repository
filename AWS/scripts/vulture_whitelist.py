# Whitelist for vulture (dead code detection, see `[tool.vulture]` in pyproject.toml).
#
# Referencing a name here marks it "used" without vulture needing a real call site.
# Add entries when vulture flags a false positive it can't resolve on its own —
# e.g. Pydantic fields only read by name, ABC methods only called through a
# dynamic registry, or attributes accessed via getattr()/string dispatch.
#
# Usage: `whitelist_module.attribute` or `_.attribute` for a bare reference.
#
# Example:
# from src.core.models.checkpoint import Checkpoint
# Checkpoint.resumed_from
