"""
Unit test for _prepare_for_llm field-stripping and keyword_performance trimming.

Verifies:
  1. performance_records is absent from item after the call
  2. auto_mining is absent from item after the call
  3. _summary_json is present and parseable
  4. auto_mining data captured in _summary_json BEFORE pop
  5. auto_mining_negatives capped at 30; auto_mining_harvest capped at 20
  6. keyword_performance trimmed: Pareto 95%-spend coverage
  7. keyword_performance floor = max(keywords_in_lp, keyword_actions, 20)
  8. keyword_performance ceiling = 300
  9. keyword_performance_original_count preserved when trimmed
 10. all items in a batch processed correctly
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.workflows.definitions.ad_diagnosis import _prepare_for_llm, _trim_keyword_performance


# ── Minimal mock context ──────────────────────────────────────────────────────

class _MockCtx:
    def __init__(self):
        self.config = _Cfg({"days": 30})

class _Cfg:
    def __init__(self, d): self._d = d
    def get(self, k, default=None): return self._d.get(k, default)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_negative(i):
    return {"keyword_text": f"term_{i}", "action": "add_negative_keyword",
            "priority": "P1", "spend_total": float(i)}

def _make_harvest(i):
    return {"keyword_text": f"good_term_{i}", "action": "harvest_to_manual",
            "priority": "P0", "orders": i}

def _make_item(n_neg=35, n_harvest=25):
    """Build a minimal item dict with performance_records and auto_mining."""
    return {
        "asin": "TEST0001",
        "performance_records": [
            {"campaign_id": "c1", "spend": 100.0, "sales": 300.0,
             "orders": 10, "clicks": 200},
        ],
        "auto_mining": {
            "summary": {"negative_count": n_neg, "harvest_count": n_harvest,
                        "skipped": False},
            "beta_prior": {"alpha": 1.0, "beta": 50.0},
            "negatives": [_make_negative(i) for i in range(n_neg)],
            "harvest":   [_make_harvest(i)  for i in range(n_harvest)],
        },
        # Minimal fields _build_item_summary expects
        "campaigns": [], "total_spend": 100.0, "total_sales": 300.0,
        "total_orders": 10, "account_acos": 33.3,
        "budget_exhaustion_pct": 0.80, "budget_likely_exhausted": False,
        "lp_summary": None, "keyword_performance": [],
        "change_attributions": [], "natural_rank_series": {}, "market_trends": {},
    }


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_performance_records_stripped():
    item = _make_item()
    assert "performance_records" in item, "pre-condition: field must exist before call"
    _prepare_for_llm([item], _MockCtx())
    assert "performance_records" not in item, \
        "performance_records must be absent from item after _prepare_for_llm"
    print("PASS  performance_records stripped")


def test_auto_mining_stripped():
    item = _make_item()
    assert "auto_mining" in item, "pre-condition: field must exist before call"
    _prepare_for_llm([item], _MockCtx())
    assert "auto_mining" not in item, \
        "auto_mining must be absent from item after _prepare_for_llm"
    print("PASS  auto_mining stripped")


def test_summary_json_present_and_parseable():
    item = _make_item()
    _prepare_for_llm([item], _MockCtx())
    assert "_summary_json" in item, "_summary_json must be injected"
    summary = json.loads(item["_summary_json"])
    assert isinstance(summary, dict), "_summary_json must parse to a dict"
    print("PASS  _summary_json present and parseable")


def test_auto_mining_data_captured_before_pop():
    """_build_item_summary must run before the pop, so summary has the data."""
    item = _make_item(n_neg=5, n_harvest=3)
    _prepare_for_llm([item], _MockCtx())
    summary = json.loads(item["_summary_json"])
    assert summary.get("auto_mining_summary") is not None, \
        "auto_mining_summary must be captured in _summary_json"
    assert len(summary.get("auto_mining_negatives", [])) == 5
    assert len(summary.get("auto_mining_harvest", [])) == 3
    print("PASS  auto_mining data captured in _summary_json before pop")


def test_negatives_capped_at_30():
    item = _make_item(n_neg=35)   # more than the cap
    _prepare_for_llm([item], _MockCtx())
    summary = json.loads(item["_summary_json"])
    neg = summary.get("auto_mining_negatives", [])
    assert len(neg) == 30, f"Expected 30 negatives, got {len(neg)}"
    print("PASS  auto_mining_negatives capped at 30")


def test_harvest_capped_at_20():
    item = _make_item(n_harvest=25)   # more than the cap
    _prepare_for_llm([item], _MockCtx())
    summary = json.loads(item["_summary_json"])
    harv = summary.get("auto_mining_harvest", [])
    assert len(harv) == 20, f"Expected 20 harvest items, got {len(harv)}"
    print("PASS  auto_mining_harvest capped at 20")


def test_multiple_items_all_stripped():
    items = [_make_item() for _ in range(3)]
    _prepare_for_llm(items, _MockCtx())
    for i, item in enumerate(items):
        assert "performance_records" not in item, f"item[{i}] still has performance_records"
        assert "auto_mining" not in item, f"item[{i}] still has auto_mining"
        assert "_summary_json" in item, f"item[{i}] missing _summary_json"
    print("PASS  all items stripped correctly")


# ── keyword_performance trim tests ────────────────────────────────────────────

def _kw_perf(n, spend_distribution="equal"):
    """Generate n keyword_performance rows."""
    if spend_distribution == "concentrated":
        # First keyword takes 96% of spend → Pareto kicks in at 1 keyword
        spends = [960.0] + [1.0] * (n - 1)
    elif spend_distribution == "equal":
        spends = [100.0] * n
    else:
        spends = list(spend_distribution)
    return [{"keyword_text": f"kw_{i}", "match_type": "EXACT",
              "total_spend": spends[i], "total_orders": 1, "acos": 30.0}
            for i in range(n)]


def test_pareto_concentrated_spend():
    """One keyword dominates >95% of spend → Pareto resolves to 1, floor=20 wins.

    For pareto_n=1: first keyword must cover ≥95% of total spend.
    With 200 keywords, others spend=1 each (total others=199):
      x / (x + 199) ≥ 0.95  →  x ≥ 3781
    Use x=4000 → 4000/4199 = 95.3% ≥ 95%.
    """
    spends = [4000.0] + [1.0] * 199   # first keyword = 95.3% of total spend
    item = {"keyword_performance": _kw_perf(200, spends),
            "lp_summary": {"keywords_in_lp": 0}, "keyword_actions": []}
    _trim_keyword_performance(item)
    # pareto_n=1, floor=20 → N=max(20,1)=20
    assert len(item["keyword_performance"]) == 20, \
        f"Expected 20 (floor), got {len(item['keyword_performance'])}"
    assert item.get("keyword_performance_original_count") == 200
    print("PASS  concentrated spend (95.3% in 1 keyword) → floor=20 wins over pareto=1")


def test_pareto_equal_spend_high_lp():
    """Equal spend across 200 keywords → Pareto=190 (95%), LP floor=150."""
    item = {"keyword_performance": _kw_perf(200, "equal"),
            "lp_summary": {"keywords_in_lp": 150}, "keyword_actions": []}
    _trim_keyword_performance(item)
    # pareto=190, floor=150 → N=190
    result_n = len(item["keyword_performance"])
    assert result_n == 190, f"Expected 190 (pareto), got {result_n}"
    print(f"PASS  equal spend → pareto=190 wins over floor=150")


def test_ceiling_enforced():
    """400 keywords with equal spend → ceiling=300 kicks in."""
    item = {"keyword_performance": _kw_perf(400, "equal"),
            "lp_summary": {"keywords_in_lp": 10}, "keyword_actions": []}
    _trim_keyword_performance(item)
    assert len(item["keyword_performance"]) == 300, \
        f"Expected 300 (ceiling), got {len(item['keyword_performance'])}"
    assert item.get("keyword_performance_original_count") == 400
    print("PASS  ceiling=300 enforced on 400-keyword set")


def test_floor_from_keyword_actions():
    """keyword_actions count drives floor when larger than lp keywords."""
    kw_actions = [{"keyword_text": f"act_{i}"} for i in range(35)]
    item = {"keyword_performance": _kw_perf(50, "concentrated"),
            "lp_summary": {"keywords_in_lp": 10},
            "keyword_actions": kw_actions}
    _trim_keyword_performance(item)
    # pareto=1 (concentrated), floor=max(20,10,35)=35 → N=35
    assert len(item["keyword_performance"]) == 35, \
        f"Expected 35 (floor from keyword_actions), got {len(item['keyword_performance'])}"
    print("PASS  floor driven by len(keyword_actions)=35")


def test_no_trim_when_below_floor():
    """15 keywords total → below floor=20, all preserved, no original_count."""
    item = {"keyword_performance": _kw_perf(15, "equal"),
            "lp_summary": {"keywords_in_lp": 0}, "keyword_actions": []}
    _trim_keyword_performance(item)
    assert len(item["keyword_performance"]) == 15
    assert "keyword_performance_original_count" not in item
    print("PASS  15 keywords below floor=20 → all preserved, no original_count")


def test_sorted_by_spend_descending():
    """After trim, keywords are sorted by total_spend descending."""
    item = {"keyword_performance": _kw_perf(50, [float(50 - i) for i in range(50)]),
            "lp_summary": {"keywords_in_lp": 0}, "keyword_actions": []}
    _trim_keyword_performance(item)
    spends = [k["total_spend"] for k in item["keyword_performance"]]
    assert spends == sorted(spends, reverse=True), "keywords not sorted by spend descending"
    print("PASS  keyword_performance sorted by total_spend descending")


def test_zero_spend_falls_back_to_floor():
    """All keywords have zero spend → pareto loop returns floor."""
    item = {"keyword_performance": [
                {"keyword_text": f"kw_{i}", "total_spend": 0} for i in range(100)
            ],
            "lp_summary": {"keywords_in_lp": 0}, "keyword_actions": []}
    _trim_keyword_performance(item)
    assert len(item["keyword_performance"]) == 20  # floor
    print("PASS  zero-spend fallback to floor=20")


def test_must_keep_causal_keyword_rescued():
    """A keyword in change_attributions but outside top-N spend is rescued."""
    # 50 keywords with high spend, 1 with near-zero spend but in change_attributions
    kw_perf = [{"keyword_text": f"kw_{i}", "match_type": "EXACT",
                 "total_spend": 100.0, "total_orders": 1, "acos": 30.0}
               for i in range(50)]
    kw_perf.append({"keyword_text": "low_spend_causal_kw", "match_type": "BROAD",
                    "total_spend": 0.5, "total_orders": 0, "acos": None})

    item = {
        "keyword_performance": kw_perf,
        "lp_summary": {"keywords_in_lp": 0},
        "keyword_actions": [],
        "change_attributions": [
            {"keyword": "low_spend_causal_kw", "delta_orders": -8.0,
             "consensus": "Strong evidence", "changed_at": "2026-04-28"},
        ],
    }
    _trim_keyword_performance(item)
    texts = {k["keyword_text"] for k in item["keyword_performance"]}
    assert "low_spend_causal_kw" in texts, \
        "causal-attributed keyword must be rescued even if outside Pareto window"
    print("PASS  must-keep causal keyword rescued from outside Pareto window")


def test_must_keep_does_not_duplicate():
    """A must-keep keyword already in top-N should not appear twice."""
    kw_perf = [{"keyword_text": f"kw_{i}", "total_spend": float(100 - i),
                 "total_orders": 1, "acos": 30.0}
               for i in range(50)]
    # kw_0 has highest spend (100) → already in top-N
    item = {
        "keyword_performance": kw_perf,
        "lp_summary": {"keywords_in_lp": 0},
        "keyword_actions": [],
        "change_attributions": [{"keyword": "kw_0", "delta_orders": -5.0}],
    }
    _trim_keyword_performance(item)
    texts = [k["keyword_text"] for k in item["keyword_performance"]]
    assert texts.count("kw_0") == 1, "must-keep keyword already in top-N must not be duplicated"
    print("PASS  must-keep keyword already in top-N not duplicated")


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_performance_records_stripped,
        test_auto_mining_stripped,
        test_summary_json_present_and_parseable,
        test_auto_mining_data_captured_before_pop,
        test_negatives_capped_at_30,
        test_harvest_capped_at_20,
        test_multiple_items_all_stripped,
        # keyword_performance trim
        test_pareto_concentrated_spend,
        test_pareto_equal_spend_high_lp,
        test_ceiling_enforced,
        test_floor_from_keyword_actions,
        test_no_trim_when_below_floor,
        test_sorted_by_spend_descending,
        test_zero_spend_falls_back_to_floor,
        test_must_keep_causal_keyword_rescued,
        test_must_keep_does_not_duplicate,
    ]
    failed = 0
    for t in tests:
        try:
            t()
        except AssertionError as e:
            print(f"FAIL  {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"ERROR {t.__name__}: {e}")
            failed += 1
    print(f"\n{'All tests passed.' if not failed else f'{failed} test(s) FAILED.'}")
    sys.exit(0 if not failed else 1)
