from __future__ import annotations

import fcntl
import json
import logging
import os
import tempfile
import time

logger = logging.getLogger(__name__)

# Per-step history stored here; keyed by step_name, value = list of durations (s)
_HISTORY_PATH = os.path.join(os.path.dirname(__file__), "step_history.json")
_HISTORY_LOCK_PATH = _HISTORY_PATH + ".lock"
_HISTORY_MAX_SAMPLES = 20  # rolling window per step


def _load_history() -> dict[str, list[float]]:
    try:
        with open(_HISTORY_PATH, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}  # first run — expected
    except json.JSONDecodeError as e:
        logger.warning(f"[telemetry] step_history.json is corrupt ({e}); resetting history")
        return {}
    except OSError as e:
        logger.warning(f"[telemetry] Could not read step_history.json: {e}")
        return {}


def _append_step(step_key: str, duration: float) -> None:
    """Append one duration sample to the on-disk history, multi-process safe.

    Uses fcntl.flock (POSIX advisory lock) so concurrent processes serialise
    the read-modify-write.  The final write is an atomic os.replace() so a
    reader never sees a half-written file.
    """
    try:
        # 'a' mode creates the lock file if absent without truncating it.
        with open(_HISTORY_LOCK_PATH, "a") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            history = _load_history()  # re-read under lock for latest state
            samples = history.setdefault(step_key, [])
            samples.append(duration)
            if len(samples) > _HISTORY_MAX_SAMPLES:
                del samples[:-_HISTORY_MAX_SAMPLES]
            _dir = os.path.dirname(_HISTORY_PATH) or "."
            tmp_fd, tmp_path = tempfile.mkstemp(dir=_dir, suffix=".tmp")
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as tf:
                    json.dump(history, tf)
                os.replace(tmp_path, _HISTORY_PATH)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
    except OSError as e:
        logger.warning(f"[telemetry] Could not persist step history: {e}")
    except Exception as e:
        logger.warning(f"[telemetry] Unexpected error persisting step history: {e}")


class TimeEstimator:
    """
    Static ETA estimations based on heuristics (kept for backward compat).
    """

    WORKFLOW_AVERAGES = {
        "amazon_bsr": 30.0,
    }
    AGENT_BASELINE_PER_ITERATION = 6.0

    @classmethod
    def estimate_workflow(cls, workflow_name: str, params: dict | None = None) -> str:
        base_time = cls.WORKFLOW_AVERAGES.get(workflow_name, 30.0)
        return f"~{int(base_time)}秒"

    @classmethod
    def estimate_agent(cls, max_iterations: int = 5) -> str:
        min_time = int(cls.AGENT_BASELINE_PER_ITERATION * 1)
        max_time = int(cls.AGENT_BASELINE_PER_ITERATION * (max_iterations * 0.6))
        return f"{min_time}~{max_time}秒"


class TelemetryTracker:
    """
    Tracks dynamic progress and calculates ETA using two complementary methods:

    1. Elapsed-ratio (always available, no history needed):
          eta = elapsed_total × remaining_steps / completed_steps
       Same algorithm used by curl, wget, and GitHub Actions progress bars.
       Robust because it doesn't assume equal step durations — it uses actual
       wall time.

    2. Per-step historical baseline (learned from past runs):
          eta = Σ historical_avg(step_i) for remaining steps
       Stored in step_history.json.  When N ≥ 3 samples exist for a step,
       the estimate is blended: 40 % elapsed-ratio + 60 % historical.

    Confidence tiers shown in the UI:
       🔴  < 2 completed steps     — wild guess, elapsed-ratio only
       🟡  2–4 completed steps     — improving estimate
       🟢  history N ≥ 3 per step  — data-backed estimate
    """

    def __init__(self, total_steps: int, workflow_name: str = ""):
        self.total_steps = total_steps
        self.workflow_name = workflow_name
        self.current_step = 0
        self.start_time = time.monotonic()
        self.step_times: list[float] = []
        self.step_names: list[str] = []
        self.last_step_time = self.start_time
        self._history: dict[str, list[float]] = _load_history()
        # Name of the step that is currently executing (started but not yet finished).
        # on_progress is called before a step runs, so the measured duration on the
        # *next* call belongs to this pending name, not the incoming one.
        self._pending_step_name: str | None = None

    def record_step(self, step_name: str = "") -> None:
        now = time.monotonic()

        # on_progress is emitted before a step starts. The first call therefore
        # marks the beginning of step 1; it must not count object construction
        # time as a completed step duration.
        if self._pending_step_name is None:
            self._pending_step_name = step_name
            self.last_step_time = now
            return

        duration = now - self.last_step_time
        self.last_step_time = now
        self.current_step += 1

        # duration is the wall time of the step that just *finished* (_pending_step_name),
        # not the one that is just starting (step_name).
        completed_name = self._pending_step_name
        self._pending_step_name = step_name

        self.step_times.append(duration)
        self.step_names.append(completed_name or step_name)

        if completed_name:
            self._persist_duration(completed_name, duration)

    def finalize(self) -> None:
        """Record the duration of the last step (no subsequent record_step to trigger it)."""
        if self._pending_step_name is None:
            return
        duration = time.monotonic() - self.last_step_time
        self.current_step += 1
        self.step_times.append(duration)
        self.step_names.append(self._pending_step_name)
        if self._pending_step_name:
            self._persist_duration(self._pending_step_name, duration)
        self._pending_step_name = None

    def _persist_duration(self, step_name: str, duration: float) -> None:
        key = f"{self.workflow_name}:{step_name}" if self.workflow_name else step_name
        samples = self._history.setdefault(key, [])
        samples.append(duration)
        if len(samples) > _HISTORY_MAX_SAMPLES:
            samples.pop(0)
        _append_step(key, duration)

    # ── ETA methods ──────────────────────────────────────────────────────

    def _elapsed_ratio_eta(self) -> float | None:
        """eta = elapsed × remaining / completed  (download-bar method)."""
        if self.current_step == 0:
            return None
        elapsed = time.monotonic() - self.start_time
        remaining = self.total_steps - self.current_step
        return elapsed * remaining / self.current_step

    def _historical_eta(self, remaining_step_names: list[str]) -> float | None:
        """Sum of historical averages for the remaining steps."""
        if not remaining_step_names:
            return None
        total = 0.0
        covered = 0
        for name in remaining_step_names:
            key = f"{self.workflow_name}:{name}" if self.workflow_name else name
            samples = self._history.get(key, [])
            if len(samples) >= 3:
                total += sum(samples) / len(samples)
                covered += 1
        if covered == 0:
            return None
        # Scale up to cover steps with no history (proportional)
        total *= len(remaining_step_names) / covered
        return total

    def _min_history_samples(self, remaining_step_names: list[str]) -> int:
        """Minimum number of samples across remaining steps (confidence proxy)."""
        mins = []
        for name in remaining_step_names:
            key = f"{self.workflow_name}:{name}" if self.workflow_name else name
            mins.append(len(self._history.get(key, [])))
        return min(mins) if mins else 0

    def get_dynamic_eta(
        self,
        remaining_step_names: list[str] | None = None,
    ) -> str | None:
        """
        Return a human-readable ETA string, or None when estimate is unavailable.

        Parameters
        ----------
        remaining_step_names:
            Optional list of future step names (enables historical blending).
        """
        if self.current_step == 0 or self.current_step >= self.total_steps:
            return None

        er_eta = self._elapsed_ratio_eta()
        hist_eta = self._historical_eta(remaining_step_names or [])
        min_samples = self._min_history_samples(remaining_step_names or [])

        # Blend: weight historical more heavily when data is solid
        if hist_eta is not None and min_samples >= 3:
            eta = 0.4 * (er_eta or hist_eta) + 0.6 * hist_eta
            confidence = "🟢"
        elif er_eta is not None and self.current_step >= 2:
            eta = er_eta
            confidence = "🟡"
        elif er_eta is not None:
            eta = er_eta
            confidence = "🔴"
        else:
            return None

        secs = max(1, int(eta))
        if secs >= 60:
            mins, s = divmod(secs, 60)
            eta_str = f"{mins}分{s:02d}秒" if s else f"{mins}分钟"
        else:
            eta_str = f"{secs}秒"

        return f"{confidence} 预计剩余 {eta_str}"
