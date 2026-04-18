"""
profile.py - Load and render the candidate profile for LLM injection.

The profile is read from candidate_profile.yaml (same directory).
`render_profile()` returns a compact text block suitable for embedding
directly into the _SYSTEM_CANDIDATE prompt.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_PROFILE_PATH = Path(__file__).parent / "candidate_profile.yaml"


def load_profile() -> dict:
    """
    Load candidate_profile.yaml.
    Returns an empty dict if the file is missing or unparseable
    (the bot degrades gracefully without a profile).
    """
    if not _PROFILE_PATH.exists():
        logger.warning("candidate_profile.yaml not found at %s", _PROFILE_PATH)
        return {}
    try:
        import yaml  # PyYAML — already a common dep; falls back gracefully
        with open(_PROFILE_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        # Fallback: minimal hand-rolled YAML reader for simple key: value lines
        logger.warning("PyYAML not installed; using fallback profile loader")
        return _fallback_load()
    except Exception as e:
        logger.warning("Failed to parse candidate_profile.yaml: %s", e)
        return {}


def render_profile(profile: dict | None = None) -> str:
    """
    Return a Chinese text block describing the candidate's background,
    preferences, and constraints — ready to inject into the system prompt.
    """
    if profile is None:
        profile = load_profile()
    if not profile:
        return ""

    lines: list[str] = ["【求职者个人信息（仅供参考，回答HR问题时使用）】"]

    identity = profile.get("identity", {})
    bg = identity.get("background", "")
    if bg:
        lines.append(f"背景：{bg.strip()}")

    skills = identity.get("skills", [])
    if skills:
        lines.append("技能：" + "、".join(skills))

    prefs = profile.get("preferences", {})
    salary = prefs.get("salary", {})
    if salary:
        min_s = salary.get("min_monthly")
        tgt_s = salary.get("target_monthly")
        note  = salary.get("note", "")
        parts = []
        if tgt_s:
            parts.append(f"期望{tgt_s}元/月")
        if min_s:
            parts.append(f"底线{min_s}元/月")
        if note:
            parts.append(note)
        lines.append("薪资：" + "，".join(parts))

    locs = prefs.get("locations", [])
    if locs:
        lines.append("工作地点：" + "、".join(locs))

    mode = prefs.get("work_mode", "")
    if mode:
        mode_map = {"remote": "全远程", "hybrid": "混合办公", "onsite": "全驻场"}
        lines.append("办公方式：" + mode_map.get(mode, mode))

    constraints = profile.get("constraints", {})
    notice = constraints.get("notice_period", "")
    if notice:
        lines.append(f"可入职：{notice}")

    avail = constraints.get("availability", "")
    if avail:
        lines.append(f"面试档期：{avail}")

    rejects = constraints.get("reject_if", [])
    if rejects:
        lines.append("以下条件不可接受：" + "；".join(rejects))

    return "\n".join(lines)


# ── Fallback loader (no PyYAML) ───────────────────────────────────────

def _fallback_load() -> dict:
    """
    Very minimal YAML parser: handles only top-level keys and
    indented list items (- value). Sufficient for candidate_profile.yaml.
    """
    result: dict[str, Any] = {}
    try:
        with open(_PROFILE_PATH, encoding="utf-8") as f:
            lines = f.readlines()
        current_key = None
        for line in lines:
            stripped = line.rstrip()
            if not stripped or stripped.lstrip().startswith("#"):
                continue
            if not line.startswith(" ") and ":" in stripped:
                key, _, val = stripped.partition(":")
                current_key = key.strip()
                result[current_key] = val.strip() or {}
            elif stripped.lstrip().startswith("- ") and current_key:
                item = stripped.lstrip()[2:].strip()
                if isinstance(result.get(current_key), list):
                    result[current_key].append(item)
                else:
                    result[current_key] = [item]
    except Exception:
        pass
    return result
