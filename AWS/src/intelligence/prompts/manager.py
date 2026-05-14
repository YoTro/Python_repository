from __future__ import annotations

import logging
import os
import string
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class PromptValidationError(ValueError):
    """Required variables missing or a {placeholder} left unresolved."""

class PromptBudgetError(ValueError):
    """Rendered prompt exceeds the spec's token_budget."""


# ---------------------------------------------------------------------------
# PromptSpec — the definition object (loaded from config/specs/*.yaml)
# ---------------------------------------------------------------------------

class PromptSpec(BaseModel):
    id: str
    version: str = "0.0.1"
    scope: Literal["per_item", "batch", "system"] = "batch"
    token_budget: int = 32_000          # ~128K chars; tighten per-spec as needed
    role_id: str = "product_manager"
    framework_ids: List[str] = Field(default_factory=list)
    include_system_in_prompt: bool = False
    required_vars: List[str] = Field(default_factory=list)
    optional_vars: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Optional[str] = None # pydantic class name — reserved for future use
    template: Optional[str] = None      # None = caller must supply via template_override


# ---------------------------------------------------------------------------
# RenderedPrompt — the output of render_spec()
# ---------------------------------------------------------------------------

@dataclass
class RenderedPrompt:
    system: str
    user: str
    token_estimate: int
    spec_id: str
    version: str
    scope: str
    _manager: Optional["PromptManager"] = None
    _base_variables: Optional[Dict[str, Any]] = None
    _template_override: Optional[str] = None
    _include_system_in_prompt: bool = False

    def format(self, **runtime_variables: Any) -> str:
        """
        Behave like a string template for ProcessStep.

        ProcessStep calls prompt_template.format(...). Re-rendering from the
        original spec here avoids a fragile two-pass .format() where escaped
        JSON braces in the YAML template would be collapsed too early.
        """
        if self._manager is None:
            user = self.user.format(**runtime_variables)
            return f"{self.system}\n\n{user}" if self._include_system_in_prompt else user
        variables = {**(self._base_variables or {}), **runtime_variables}
        rendered = self._manager.render_spec(
            self.spec_id,
            variables,
            template_override=self._template_override,
            _attach_formatter=False,
        )
        if self._include_system_in_prompt:
            return f"{rendered.system}\n\n{rendered.user}"
        return rendered.user


# ---------------------------------------------------------------------------
# PromptManager
# ---------------------------------------------------------------------------

class PromptManager:
    """
    Centralized manager for prompt components (SSOT).

    Two rendering modes:
      Legacy     — get_role / get_frameworks / get_template /
                   assemble_report_instructions / render
                   (all existing callers continue to work unchanged)
      Spec-based — render_spec(spec_id, variables)
                   validates required vars, enforces token budget,
                   records version, exposes scope

    Extension point
      Override _fetch_remote_template() in a subclass to pull templates
      from a remote store (e.g. Langfuse) at render time.
      The priority order is: template_override > remote > local spec.template.
    """

    def __init__(self) -> None:
        self.root_dir   = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        self.config_dir = os.path.join(os.path.dirname(__file__), "config")
        self.specs_dir  = os.path.join(self.config_dir, "specs")
        self.project_specs_dir = os.path.join(self.root_dir, "config", "specs")

        self.workflow_defaults  = self._load_workflow_defaults()
        self.config_variables   = self._flatten_config_thresholds(self.workflow_defaults)

        self.roles      = self._load_yaml("roles.yaml")
        self.frameworks = self._load_yaml("frameworks.yaml")
        self.templates  = self._load_yaml("templates.yaml")

        self._specs: Dict[str, PromptSpec] = self._load_specs()

    # ── Loaders ─────────────────────────────────────────────────────────────

    def _load_workflow_defaults(self) -> Dict[str, Any]:
        path = os.path.join(self.root_dir, "config", "workflow_defaults.yaml")
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error("PromptManager failed to load workflow_defaults: %s", e)
        return {}

    def _flatten_config_thresholds(self, config: Dict[str, Any]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for wf_cfg in config.values():
            if isinstance(wf_cfg, dict):
                for k, v in wf_cfg.get("thresholds", {}).items():
                    out[k] = str(v)
        return out

    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        path = os.path.join(self.config_dir, filename)
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error("Failed to load prompt config %s: %s", filename, e)
        return {}

    def _load_specs(self) -> Dict[str, PromptSpec]:
        specs: Dict[str, PromptSpec] = {}
        # Load legacy package-local specs first, then project-level specs so
        # config/specs can override while product_screening remains compatible.
        for specs_dir in (self.specs_dir, self.project_specs_dir):
            if not os.path.isdir(specs_dir):
                continue
            for fname in sorted(os.listdir(specs_dir)):
                if not fname.endswith((".yaml", ".yml")):
                    continue
                path = os.path.join(specs_dir, fname)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f) or {}
                    spec = PromptSpec.model_validate(data)
                    specs[spec.id] = spec
                    logger.debug("Loaded prompt spec: %s v%s", spec.id, spec.version)
                except Exception as e:
                    logger.error("Failed to load prompt spec %s: %s", path, e)
        return specs

    # ── Internal helpers ─────────────────────────────────────────────────────

    def _inject_vars(self, content: str) -> str:
        """Substitute $config_var placeholders from workflow_defaults."""
        if not content:
            return ""
        try:
            return string.Template(content).safe_substitute(self.config_variables)
        except Exception as e:
            logger.warning("Config-var injection failed: %s", e)
            return content

    @staticmethod
    def _estimate_tokens(text: str) -> int:
        """Character-based approximation (~4 chars per token for English)."""
        return len(text) // 4

    # ── Extension point ──────────────────────────────────────────────────────

    def _fetch_remote_template(self, spec_id: str, version: str) -> Optional[str]:
        """
        Override in a subclass to pull templates from a remote store.

        Example (Langfuse):
            def _fetch_remote_template(self, spec_id, version):
                prompt = self._langfuse.get_prompt(spec_id)
                return prompt.get_langchain_prompt().template
        """
        return None

    # ── Legacy API (backward compatible) ─────────────────────────────────────

    def get_role(self, role_id: str) -> str:
        content = self.roles.get(role_id, {}).get("role_content", "")
        return self._inject_vars(content)

    def get_frameworks(self, framework_ids: List[str]) -> str:
        parts = []
        for fid in framework_ids:
            fw = self.frameworks.get(fid)
            if fw:
                title   = fw.get("title", "")
                content = self._inject_vars(fw.get("content", ""))
                parts.append(f"### {title}\n{content}")
        return "\n\n".join(parts)

    def get_template(self, template_id: str) -> str:
        content = self.templates.get(template_id, {}).get("content", "")
        return self._inject_vars(content)

    def assemble_report_instructions(
        self,
        role_id: str,
        framework_ids: List[str],
        template_id: str = "standard_report",
    ) -> str:
        return (
            f"# ROLE\n{self.get_role(role_id)}\n\n"
            f"# ANALYSIS FRAMEWORKS\n{self.get_frameworks(framework_ids)}\n\n"
            f"# OUTPUT FORMAT\n{self.get_template(template_id)}"
        )

    def render(
        self,
        name: str,
        variables: Dict[str, Any],
        role_id: str = "product_manager",
    ) -> tuple[str, str]:
        """Legacy render — returns (system_message, user_prompt)."""
        system = self.get_role(role_id)
        fw = self.frameworks.get(name)
        if not fw:
            logger.warning("Prompt component '%s' not found. Returning raw variables.", name)
            return system, str(variables)
        content = self._inject_vars(fw.get("content", ""))
        try:
            user = content.format(**variables)
        except Exception as e:
            logger.error("Failed to format prompt '%s': %s", name, e)
            user = content
        return system, user

    # ── Spec-based API ────────────────────────────────────────────────────────

    def render_spec(
        self,
        spec_id: str,
        variables: Dict[str, Any],
        *,
        template_override: Optional[str] = None,
        _attach_formatter: bool = True,
    ) -> RenderedPrompt:
        """
        Render a registered PromptSpec with full lifecycle management.

        Resolution order for template text:
          1. template_override  (caller-supplied, highest priority)
          2. _fetch_remote_template()  (Langfuse or other remote store)
          3. spec.template  (local YAML)

        Raises:
          KeyError              – spec_id not registered
          PromptValidationError – required vars missing or placeholder unresolved
          PromptBudgetError     – rendered prompt exceeds token_budget
        """
        if spec_id not in self._specs:
            raise KeyError(
                f"PromptSpec '{spec_id}' not registered. "
                f"Available: {sorted(self._specs)}"
            )
        spec = self._specs[spec_id]

        # 1. Variable validation
        merged  = {**spec.optional_vars, **variables}
        missing = set(spec.required_vars) - merged.keys()
        if missing:
            raise PromptValidationError(
                f"[{spec_id} v{spec.version}] missing required vars: {sorted(missing)}"
            )

        # 2. Template resolution
        template = (
            template_override
            or self._fetch_remote_template(spec_id, spec.version)
            or spec.template
        )
        if not template:
            raise PromptValidationError(
                f"[{spec_id} v{spec.version}] no template defined; "
                "pass template_override or set template in the spec YAML"
            )

        # 3. $config substitution, then {runtime} substitution
        text = self._inject_vars(template)
        try:
            user = text.format_map(merged)
        except KeyError as e:
            raise PromptValidationError(
                f"[{spec_id} v{spec.version}] unresolved placeholder: {e}"
            ) from e

        # 4. Token budget
        tokens = self._estimate_tokens(user)
        if tokens > spec.token_budget:
            raise PromptBudgetError(
                f"[{spec_id} v{spec.version}] ~{tokens} est. tokens exceeds "
                f"budget {spec.token_budget}"
            )

        # 5. System message
        system = self.assemble_report_instructions(spec.role_id, spec.framework_ids)

        return RenderedPrompt(
            system=system,
            user=user,
            token_estimate=tokens,
            spec_id=spec_id,
            version=spec.version,
            scope=spec.scope,
            _manager=self if _attach_formatter else None,
            _base_variables=dict(variables) if _attach_formatter else None,
            _template_override=template_override if _attach_formatter else None,
            _include_system_in_prompt=spec.include_system_in_prompt,
        )

    # ── Runtime spec management ───────────────────────────────────────────────

    def register_spec(self, spec: PromptSpec) -> None:
        """Register a spec at runtime (useful for tests or dynamic prompts)."""
        self._specs[spec.id] = spec

    def get_spec(self, spec_id: str) -> Optional[PromptSpec]:
        return self._specs.get(spec_id)

    def list_specs(self) -> List[str]:
        return sorted(self._specs)


# Singleton
prompt_manager = PromptManager()
