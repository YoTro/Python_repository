import os
import yaml
import logging
import string
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class PromptManager:
    """
    Centralized manager for atomic prompt components (SSOT).
    Synchronizes AI reasoning standards with system-wide workflow configurations.
    """
    
    def __init__(self):
        self.root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
        self.config_dir = os.path.join(os.path.dirname(__file__), "config")
        
        # 1. Load System-wide Configs (The source of threshold values)
        self.workflow_defaults = self._load_workflow_defaults()
        self.config_variables = self._flatten_config_thresholds(self.workflow_defaults)
        
        # 2. Load Prompt Components
        self.roles = self._load_yaml("roles.yaml")
        self.frameworks = self._load_yaml("frameworks.yaml")
        self.templates = self._load_yaml("templates.yaml")

    def _load_workflow_defaults(self) -> Dict[str, Any]:
        path = os.path.join(self.root_dir, "config", "workflow_defaults.yaml")
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            return {}
        except Exception as e:
            logger.error(f"PromptManager failed to load workflow_defaults: {e}")
            return {}

    def _flatten_config_thresholds(self, config: Dict[str, Any]) -> Dict[str, str]:
        """
        Extracts all 'thresholds' from various workflows and flattens them 
        for string template substitution.
        """
        vars = {}
        for wf_name, wf_cfg in config.items():
            if isinstance(wf_cfg, dict) and "thresholds" in wf_cfg:
                for key, val in wf_cfg["thresholds"].items():
                    vars[key] = str(val)
        return vars

    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        path = os.path.join(self.config_dir, filename)
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return yaml.safe_load(f) or {}
            return {}
        except Exception as e:
            logger.error(f"Failed to load prompt config {filename}: {e}")
            return {}

    def _inject_vars(self, content: str) -> str:
        """Injects configuration variables into prompt strings."""
        if not content: return ""
        try:
            return string.Template(content).safe_substitute(self.config_variables)
        except Exception as e:
            logger.warning(f"Variable injection failed for prompt component: {e}")
            return content

    def get_role(self, role_id: str) -> str:
        content = self.roles.get(role_id, {}).get("role_content", "")
        return self._inject_vars(content)

    def get_frameworks(self, framework_ids: List[str]) -> str:
        output = []
        for fid in framework_ids:
            fw = self.frameworks.get(fid)
            if fw:
                title = fw.get('title')
                content = self._inject_vars(fw.get('content', ''))
                output.append(f"### {title}\n{content}")
        return "\n\n".join(output)

    def get_template(self, template_id: str) -> str:
        content = self.templates.get(template_id, {}).get("content", "")
        return self._inject_vars(content)

    def assemble_report_instructions(self, role_id: str, framework_ids: List[str], template_id: str = "standard_report") -> str:
        role = self.get_role(role_id)
        fws = self.get_frameworks(framework_ids)
        template = self.get_template(template_id)
        
        return (
            f"# ROLE\n{role}\n\n"
            f"# ANALYSIS FRAMEWORKS\n{fws}\n\n"
            f"# OUTPUT FORMAT\n{template}"
        )

    def render(self, name: str, variables: Dict[str, Any], role_id: str = "product_manager") -> tuple[str, str]:
        """
        Renders a specific framework/prompt by name with provided variables.
        Returns (system_message, user_prompt).
        """
        system_msg = self.get_role(role_id)
        
        fw = self.frameworks.get(name)
        if not fw:
            logger.warning(f"Prompt component '{name}' not found in frameworks. Returning raw variables.")
            return system_msg, str(variables)
            
        content = fw.get('content', '')
        # 1. Inject system config variables first ($high_monopoly_score etc.)
        content = self._inject_vars(content)
        
        # 2. Inject user-provided variables ({review_data} etc.)
        try:
            user_prompt = content.format(**variables)
        except Exception as e:
            logger.error(f"Failed to format prompt '{name}' with variables: {e}")
            user_prompt = content # Fallback to unformatted
            
        return system_msg, user_prompt

# Singleton instance
prompt_manager = PromptManager()
