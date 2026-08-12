from __future__ import annotations

from src.workflows.definitions.lp_validation import _build_pas_warnings


class TestBuildPasWarnings:
    def test_low_base_rate_edge_case(self):
        result = {
            "pas_status": "computed",
            "pre_mean_kw_orders_day": 0.0001,
        }
        warnings = _build_pas_warnings(result)
        assert any("low_base_rate" in w for w in warnings), f"Got: {warnings}"

    def test_pas_indeterminate_warning(self):
        result = {
            "pas_status": "indeterminate",
            "n_keywords_excluded": 5,
            "n_keywords": 10,
        }
        warnings = _build_pas_warnings(result)
        assert any("pas_indeterminate" in w for w in warnings), f"Got: {warnings}"
