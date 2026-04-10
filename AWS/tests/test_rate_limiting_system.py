"""
Tests for the three-layer rate limiting system.

Layer 1a — Per-chat cooldown debounce      (_check_chat_cooldown / check_limit)
Layer 1b — Concurrent slot context manager (concurrent_slot)
Layer 2  — Tenant daily quota              (_check_tenant_quota / check_limit)
Layer 3  — External API token bucket       (acquire_source)
Integration — Gateway → UnifiedRequest metadata propagation
"""
from __future__ import annotations
import asyncio
import os
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.gateway.rate_limit import RateLimiter
from src.core.models.request import UnifiedRequest
from src.core.errors.exceptions import AWSBaseError


# ── Helpers ───────────────────────────────────────────────────────────────────

def reset_limiter() -> RateLimiter:
    """
    Return the singleton with all mutable runtime state cleared.
    Token-bucket tokens are reset to full burst so tests start from a known state
    regardless of execution order.
    """
    rl = RateLimiter()
    rl._concurrent = {}
    rl._tenant_counters = {}
    rl._chat_last = {}
    for bucket in rl._source_buckets.values():
        with bucket.lock:
            bucket.tokens = bucket.capacity
    return rl


# ── Layer 3: Token bucket ─────────────────────────────────────────────────────

class TestLayer3TokenBucket(unittest.TestCase):
    """acquire_source — token-bucket throttling for external API calls."""

    def setUp(self):
        self.rl = reset_limiter()

    def test_known_source_returns_true(self):
        self.assertTrue(self.rl.acquire_source("xiyouzhaoci"))

    def test_unknown_source_always_allowed(self):
        self.assertTrue(self.rl.acquire_source("source_not_in_config"))

    def test_burst_depletes(self):
        """After draining burst=5, next immediate call must time out."""
        for _ in range(5):
            self.rl.acquire_source("xiyouzhaoci")
        self.assertFalse(self.rl.acquire_source("xiyouzhaoci", timeout=0.05))

    def test_bucket_refills_over_time(self):
        """xiyouzhaoci: 30 req/min = 0.5 token/sec → 2.2 s refills ≥1 token."""
        for _ in range(5):
            self.rl.acquire_source("xiyouzhaoci")
        time.sleep(2.2)
        self.assertTrue(self.rl.acquire_source("xiyouzhaoci", timeout=0.05))

    def test_different_sources_are_independent(self):
        """Depleting xiyouzhaoci must not affect sellersprite."""
        for _ in range(5):
            self.rl.acquire_source("xiyouzhaoci")
        self.rl.acquire_source("xiyouzhaoci", timeout=0.05)   # should fail / drain
        self.assertTrue(self.rl.acquire_source("sellersprite"))


# ── Layer 2: Tenant daily quota ───────────────────────────────────────────────

class TestLayer2TenantQuota(unittest.TestCase):
    """_check_tenant_quota — daily request budget per plan tier."""

    def setUp(self):
        self.rl = reset_limiter()
        self.today = time.strftime("%Y-%m-%d")

    def test_first_request_allowed(self):
        self.assertTrue(self.rl._check_tenant_quota("t_new", "free"))

    def test_counter_increments_on_success(self):
        self.rl._tenant_counters["t_inc"] = {self.today: 10}
        self.rl._check_tenant_quota("t_inc", "free")
        self.assertEqual(self.rl._tenant_counters["t_inc"][self.today], 11)

    def test_at_daily_limit_blocked(self):
        """free tier: 50 requests/day."""
        self.rl._tenant_counters["t_full"] = {self.today: 50}
        self.assertFalse(self.rl._check_tenant_quota("t_full", "free"))

    def test_one_under_limit_allowed(self):
        self.rl._tenant_counters["t_almost"] = {self.today: 49}
        self.assertTrue(self.rl._check_tenant_quota("t_almost", "free"))

    def test_pro_tier_higher_limit(self):
        """pro tier: 500 requests/day."""
        self.rl._tenant_counters["t_pro"] = {self.today: 499}
        self.assertTrue(self.rl._check_tenant_quota("t_pro", "pro"))
        self.rl._tenant_counters["t_pro"][self.today] = 500
        self.assertFalse(self.rl._check_tenant_quota("t_pro", "pro"))

    def test_unknown_tier_is_unlimited(self):
        """Tier absent from config → daily_requests defaults to -1 (allow all)."""
        self.assertTrue(self.rl._check_tenant_quota("t_ghost", "ghost_tier"))

    def test_different_tenants_are_isolated(self):
        self.rl._tenant_counters["t_a"] = {self.today: 50}
        self.assertFalse(self.rl._check_tenant_quota("t_a", "free"))
        self.assertTrue(self.rl._check_tenant_quota("t_b", "free"))


# ── Layer 1a: Cooldown debounce ───────────────────────────────────────────────

class TestLayer1aCooldown(unittest.TestCase):
    """_check_chat_cooldown — per-chat debounce window."""

    def setUp(self):
        self.rl = reset_limiter()

    def test_first_trigger_always_allowed(self):
        self.assertTrue(self.rl._check_chat_cooldown("chat_new", 10.0))

    def test_immediate_repeat_blocked(self):
        self.rl._check_chat_cooldown("chat_spam", 10.0)
        self.assertFalse(self.rl._check_chat_cooldown("chat_spam", 10.0))

    def test_different_chats_are_independent(self):
        self.rl._check_chat_cooldown("chat_x", 10.0)
        self.assertTrue(self.rl._check_chat_cooldown("chat_y", 10.0))

    def test_zero_cooldown_never_blocks(self):
        """CLI entry has cooldown_seconds=0 — must never gate any chat."""
        self.rl._check_chat_cooldown("chat_cli", 0)
        self.assertTrue(self.rl._check_chat_cooldown("chat_cli", 0))

    def test_empty_chat_id_skips_cooldown(self):
        self.assertTrue(self.rl._check_chat_cooldown("", 10.0))

    def test_after_window_expires_allowed_again(self):
        self.rl._check_chat_cooldown("chat_wait", 0.1)
        time.sleep(0.15)
        self.assertTrue(self.rl._check_chat_cooldown("chat_wait", 0.1))

    def test_blocked_call_does_not_reset_timer(self):
        """A rejected call must not update the timestamp — window stays fixed."""
        self.rl._check_chat_cooldown("chat_timer", 0.3)   # t=0, allowed
        self.rl._check_chat_cooldown("chat_timer", 0.3)   # t≈0, blocked
        time.sleep(0.15)
        # Still within the original 0.3s window → must still be blocked
        self.assertFalse(self.rl._check_chat_cooldown("chat_timer", 0.3))
        time.sleep(0.2)
        # Now past 0.3s from the FIRST call → allowed
        self.assertTrue(self.rl._check_chat_cooldown("chat_timer", 0.3))


# ── Layer 1b: Concurrent slot ─────────────────────────────────────────────────

class TestLayer1bConcurrentSlot(unittest.IsolatedAsyncioTestCase):
    """concurrent_slot — async context manager with guaranteed release."""

    async def asyncSetUp(self):
        self.rl = reset_limiter()

    async def test_slot_increments_inside_block(self):
        async with self.rl.concurrent_slot("feishu_workflow", "chat_1"):
            self.assertEqual(self.rl._concurrent.get("feishu_workflow", 0), 1)
            self.assertEqual(self.rl._concurrent.get("feishu_workflow:chat_1", 0), 1)

    async def test_slot_released_after_normal_exit(self):
        async with self.rl.concurrent_slot("feishu_workflow", "chat_2"):
            pass
        self.assertEqual(self.rl._concurrent.get("feishu_workflow", 0), 0)
        self.assertEqual(self.rl._concurrent.get("feishu_workflow:chat_2", 0), 0)

    async def test_slot_released_on_exception(self):
        """Deadlock prevention: finally must run even on unhandled exception."""
        try:
            async with self.rl.concurrent_slot("feishu_workflow", "chat_crash"):
                raise ValueError("simulated job crash")
        except ValueError:
            pass
        self.assertEqual(self.rl._concurrent.get("feishu_workflow", 0), 0)
        self.assertEqual(self.rl._concurrent.get("feishu_workflow:chat_crash", 0), 0)

    async def test_slot_released_on_cancellation(self):
        """asyncio.CancelledError (BaseException) must not leak the slot."""
        try:
            async with self.rl.concurrent_slot("feishu_workflow", "chat_cancel"):
                raise asyncio.CancelledError()
        except asyncio.CancelledError:
            pass
        self.assertEqual(self.rl._concurrent.get("feishu_workflow", 0), 0)
        self.assertEqual(self.rl._concurrent.get("feishu_workflow:chat_cancel", 0), 0)

    async def test_per_chat_limit_enforced(self):
        """feishu_explore per_chat_concurrent=1: 2nd job for same chat must fail."""
        async with self.rl.concurrent_slot("feishu_explore", "chat_heavy"):
            with self.assertRaises(RuntimeError) as cm:
                async with self.rl.concurrent_slot("feishu_explore", "chat_heavy"):
                    pass
            self.assertIn("Per-chat", str(cm.exception))

    async def test_per_chat_limit_does_not_block_other_chats(self):
        """Holding a slot for chat_a must not block chat_b."""
        async with self.rl.concurrent_slot("feishu_explore", "chat_a"):
            async with self.rl.concurrent_slot("feishu_explore", "chat_b"):
                self.assertEqual(self.rl._concurrent.get("feishu_explore", 0), 2)

    async def test_global_limit_enforced(self):
        """feishu_explore concurrent_jobs=3: 4th job must fail regardless of chat."""
        self.rl._concurrent["feishu_explore"] = 3
        with self.assertRaises(RuntimeError) as cm:
            async with self.rl.concurrent_slot("feishu_explore", "chat_4th"):
                pass
        self.assertIn("Global", str(cm.exception))
        # Counter must not have been incremented past the limit
        self.assertEqual(self.rl._concurrent["feishu_explore"], 3)

    async def test_global_counter_tracks_all_chats(self):
        """Global counter reflects sum of all active chat slots."""
        async with self.rl.concurrent_slot("feishu_workflow", "chat_p"):
            async with self.rl.concurrent_slot("feishu_workflow", "chat_q"):
                self.assertEqual(self.rl._concurrent.get("feishu_workflow", 0), 2)
            self.assertEqual(self.rl._concurrent.get("feishu_workflow", 0), 1)
        self.assertEqual(self.rl._concurrent.get("feishu_workflow", 0), 0)

    async def test_no_entry_type_skips_all_checks(self):
        """Requests without entry_type (legacy / unconfigured) must not raise."""
        async with self.rl.concurrent_slot(None, None):
            pass


# ── check_limit: combined gateway gate ───────────────────────────────────────

class TestCheckLimit(unittest.TestCase):
    """check_limit — unified dispatch gate: cooldown + daily quota."""

    def setUp(self):
        self.rl = reset_limiter()
        self.identity = {"tenant_id": "gw_t", "user_id": "u1", "plan_tier": "free"}

    def test_cli_first_request_allowed(self):
        self.assertTrue(self.rl.check_limit(self.identity, "cli_workflow"))

    def test_feishu_cooldown_blocks_repeat_for_same_chat(self):
        self.rl.check_limit(self.identity, "feishu_workflow", chat_id="chat_gw")
        self.assertFalse(
            self.rl.check_limit(self.identity, "feishu_workflow", chat_id="chat_gw")
        )

    def test_feishu_cooldown_does_not_affect_other_chats(self):
        self.rl.check_limit(self.identity, "feishu_workflow", chat_id="chat_a")
        self.assertTrue(
            self.rl.check_limit(self.identity, "feishu_workflow", chat_id="chat_b")
        )

    def test_daily_quota_exhausted_blocks(self):
        today = time.strftime("%Y-%m-%d")
        self.rl._tenant_counters["gw_t"] = {today: 50}
        self.assertFalse(self.rl.check_limit(self.identity, "cli_workflow"))

    def test_cooldown_blocks_before_quota_is_incremented(self):
        """When cooldown rejects, the daily quota counter must NOT advance."""
        self.rl.check_limit(self.identity, "feishu_workflow", chat_id="chat_order")
        today = time.strftime("%Y-%m-%d")
        count_before = self.rl._tenant_counters.get("gw_t", {}).get(today, 0)
        self.rl.check_limit(self.identity, "feishu_workflow", chat_id="chat_order")
        count_after = self.rl._tenant_counters.get("gw_t", {}).get(today, 0)
        self.assertEqual(count_before, count_after)


# ── UnifiedRequest metadata ───────────────────────────────────────────────────

class TestUnifiedRequestMetadata(unittest.TestCase):
    """entry_type and chat_id must propagate from gateway through to JobManager."""

    def test_feishu_workflow_fields_set(self):
        req = UnifiedRequest(
            workflow_name="product_screening",
            entry_type="feishu_workflow",
            chat_id="chat_123",
        )
        self.assertEqual(req.entry_type, "feishu_workflow")
        self.assertEqual(req.chat_id, "chat_123")

    def test_cli_workflow_has_no_chat(self):
        req = UnifiedRequest(
            workflow_name="product_screening",
            entry_type="cli_workflow",
        )
        self.assertEqual(req.entry_type, "cli_workflow")
        self.assertIsNone(req.chat_id)

    def test_defaults_are_none(self):
        req = UnifiedRequest(workflow_name="test")
        self.assertIsNone(req.entry_type)
        self.assertIsNone(req.chat_id)

    def test_fields_survive_model_serialisation(self):
        req = UnifiedRequest(
            intent="analyse this product",
            entry_type="feishu_explore",
            chat_id="chat_explore_99",
        )
        data = req.model_dump()
        self.assertEqual(data["entry_type"], "feishu_explore")
        self.assertEqual(data["chat_id"], "chat_explore_99")


if __name__ == "__main__":
    unittest.main(verbosity=2)
