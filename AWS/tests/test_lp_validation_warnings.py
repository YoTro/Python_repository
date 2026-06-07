import unittest

from src.workflows.definitions.lp_validation import _build_pas_warnings


class TestLPValidationBugs(unittest.TestCase):
    def test_low_base_rate_edge_case(self):
        """
        Test that low_base_rate warning logic correctly identifies
        unstable PAS when pre_mean is near zero.
        """
        # Scenario: PAS is computed but pre-period mean is effectively zero
        result = {
            "pas_status": "computed",
            "pre_mean_kw_orders_day": 0.0001,  # Near zero
        }
        warnings = _build_pas_warnings(result)

        # Verify the warning is triggered
        self.assertTrue(
            any("low_base_rate" in w for w in warnings),
            f"Expected low_base_rate warning, got: {warnings}",
        )

    def test_pas_indeterminate_warning(self):
        """
        Verify that indeterminate PAS status triggers the correct warning.
        """
        result = {"pas_status": "indeterminate", "n_keywords_excluded": 5, "n_keywords": 10}
        warnings = _build_pas_warnings(result)

        # Verify the warning is triggered
        self.assertTrue(
            any("pas_indeterminate" in w for w in warnings),
            f"Expected pas_indeterminate warning, got: {warnings}",
        )


if __name__ == "__main__":
    unittest.main()
