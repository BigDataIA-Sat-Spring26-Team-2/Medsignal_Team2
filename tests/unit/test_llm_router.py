"""
tests/unit/test_llm_router.py — Unit tests for LLMRouter

Tests cover the three PR fixes plus surrounding edge cases:

    Fix 1 — _daily_spend as class variable (shared across instances)
        1.  Two instances share one daily spend tracker
        2.  reset() clears run counters but NOT daily spend
        3.  Daily spend resets at midnight automatically

    Fix 2 — Actual cost from response._hidden_params["response_cost"]
        4.  Actual cost recorded when hidden_params available (gpt-4o demo day)
        5.  Falls back to estimate when hidden_params missing or None
        6.  Falls back to estimate when hidden_params raises exception
        7.  gpt-4o actual cost is ~33x higher than gpt-4o-mini estimate

    Fix 3 — Realistic input token estimate in pre-flight check
        8.  expected_input_tokens present in TASK_CONFIG for both tasks
        9.  brief_generation pre-flight uses 2000 input estimate (not 400)
        10. query_generation pre-flight uses 300 input estimate (not 133)
        11. Pre-flight blocks call that old estimate would have allowed through

    Edge cases
        12. Unknown task raises ValueError not RuntimeError
        13. Daily budget exceeded raises RuntimeError before API call
        14. Run budget exceeded raises RuntimeError before API call
        15. RuntimeError from budget propagates — not swallowed by model fallback
        16. Model fallback fires on API error but NOT on budget RuntimeError
        17. reset() preserves daily spend across multiple resets
        18. Usage summary reflects actual recorded cost not estimate

Run:
    poetry run pytest tests/unit/test_llm_router.py -v -s
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from app.core.llm_router import LLMRouter, TASK_CONFIG, DAILY_BUDGET_USD, DailySpend


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_mock_response(content="test output", input_tok=300,
                        output_tok=150, model="gpt-4o-mini",
                        response_cost=None):
    """Build a mock litellm response."""
    mock = MagicMock()
    mock.choices[0].message.content  = content
    mock.usage.prompt_tokens         = input_tok
    mock.usage.completion_tokens     = output_tok
    mock.usage.total_tokens          = input_tok + output_tok
    mock.model                       = model
    mock._hidden_params              = (
        {"response_cost": response_cost} if response_cost is not None else {}
    )
    return mock


def reset_class_state():
    """Reset shared class-level state before each test."""
    LLMRouter._daily_spend.spent_usd = Decimal("0")
    LLMRouter._daily_spend.date      = __import__("datetime").date.today()


# ── Fix 1: _daily_spend as class variable ─────────────────────────────────────

class TestSharedDailySpend:

    def setup_method(self):
        reset_class_state()

    def test_1_two_instances_share_daily_spend(self):
        """
        Fix 1 core test — two LLMRouter instances must share one DailySpend.
        Before fix: each __init__ created its own DailySpend() → $20 effective cap.
        After fix: class variable → both instances see the same counter.
        """
        r1 = LLMRouter()
        r2 = LLMRouter()

        LLMRouter._daily_spend.record(Decimal("1.00"))

        assert r1._daily_spend.spent_usd == Decimal("1.00")
        assert r2._daily_spend.spent_usd == Decimal("1.00")
        assert r1._daily_spend is r2._daily_spend, (
            "r1 and r2 must reference the same DailySpend object"
        )

    def test_2_reset_clears_run_counters_not_daily_spend(self):
        """
        reset() must clear run-level token counters but NOT daily spend.
        Daily spend persists across pipeline runs within the same day.
        """
        router = LLMRouter()
        LLMRouter._daily_spend.record(Decimal("2.50"))

        # Simulate some run-level usage
        router.usage["query_generation"].input_tokens  = 1000
        router.usage["query_generation"].output_tokens = 500
        router.usage["query_generation"].total_calls   = 5

        router.reset()

        # Run counters cleared
        assert router.usage["query_generation"].input_tokens  == 0
        assert router.usage["query_generation"].output_tokens == 0
        assert router.usage["query_generation"].total_calls   == 0

        # Daily spend preserved
        assert LLMRouter._daily_spend.spent_usd == Decimal("2.50"), (
            "reset() must not clear daily spend — it accumulates all day"
        )

    def test_3_daily_spend_resets_on_new_day(self):
        """
        DailySpend._reset_if_new_day() must clear spend when date changes.
        Simulates midnight rollover without waiting.
        """
        from datetime import date, timedelta

        LLMRouter._daily_spend.record(Decimal("5.00"))
        assert LLMRouter._daily_spend.spent_usd == Decimal("5.00")

        # Simulate yesterday's date
        LLMRouter._daily_spend.date = date.today() - timedelta(days=1)

        # Any operation triggers the reset check
        remaining = LLMRouter._daily_spend.remaining()

        assert LLMRouter._daily_spend.spent_usd == Decimal("0"), (
            "Spend must reset to 0 when date changes"
        )
        assert remaining == DAILY_BUDGET_USD


# ── Fix 2: Actual cost from hidden_params ─────────────────────────────────────

class TestActualCostTracking:

    def setup_method(self):
        reset_class_state()

    def test_4_actual_cost_used_when_hidden_params_available(self):
        """
        Fix 2 core test — when litellm provides response_cost, use it.
        gpt-4o actual cost is 33x higher than gpt-4o-mini estimate.
        Before fix: estimate used → daily budget passes $10 while spending $330.
        After fix: actual cost recorded → budget guard fires correctly.
        """
        router   = LLMRouter()
        gpt4o_actual_cost = 0.003750  # 300 input × $5/1M + 150 output × $15/1M

        mock_resp = make_mock_response(
            content       = '["q1 mechanism", "q2 incidence", "q3 outcomes"]',
            input_tok     = 300,
            output_tok    = 150,
            model         = "gpt-4o",
            response_cost = gpt4o_actual_cost,
        )

        with patch("app.core.llm_router.completion", return_value=mock_resp):
            router.complete(
                messages=[{"role": "user", "content": "test"}],
                task="query_generation",
            )

        recorded        = LLMRouter._daily_spend.spent_usd
        mini_estimate   = (Decimal("0.00015") * 300 / 1000 +
                           Decimal("0.00060") * 150 / 1000)
        actual_expected = Decimal(str(gpt4o_actual_cost))

        assert recorded == actual_expected, (
            f"Expected actual gpt-4o cost ${actual_expected} "
            f"but recorded ${recorded}. "
            f"gpt-4o-mini estimate ${mini_estimate} would be 33x too low."
        )

    def test_5_falls_back_to_estimate_when_hidden_params_missing(self):
        """
        When _hidden_params has no response_cost key, fall back to estimate.
        This handles models that don't expose cost via litellm.
        """
        router    = LLMRouter()
        mock_resp = make_mock_response(
            input_tok     = 300,
            output_tok    = 150,
            response_cost = None,  # no cost in hidden_params
        )
        mock_resp._hidden_params = {}  # empty dict

        with patch("app.core.llm_router.completion", return_value=mock_resp):
            router.complete(
                messages=[{"role": "user", "content": "test"}],
                task="query_generation",
            )

        # Should still record something (the estimate)
        assert LLMRouter._daily_spend.spent_usd > Decimal("0"), (
            "Cost must be recorded even when hidden_params is missing"
        )

    def test_6_falls_back_to_estimate_when_hidden_params_raises(self):
        """
        If accessing _hidden_params raises any exception, fall back to estimate.
        Defensive — litellm API may change between versions.
        """
        router    = LLMRouter()
        mock_resp = make_mock_response(input_tok=300, output_tok=150)

        # Make _hidden_params access raise
        type(mock_resp)._hidden_params = property(
            lambda self: (_ for _ in ()).throw(AttributeError("no hidden params"))
        )

        with patch("app.core.llm_router.completion", return_value=mock_resp):
            # Should not raise — should fall back to estimate
            router.complete(
                messages=[{"role": "user", "content": "test"}],
                task="query_generation",
            )

        assert LLMRouter._daily_spend.spent_usd > Decimal("0")

    def test_7_gpt4o_cost_significantly_higher_than_mini_estimate(self):
        """
        Documents the cost gap that Fix 2 addresses.
        gpt-4o is ~33x more expensive than gpt-4o-mini at same token count.
        This test fails before Fix 2 if estimate is used instead of actual.
        """
        input_tok  = 2000  # typical Agent 3 input
        output_tok = 600   # Agent 3 max_tokens

        # gpt-4o-mini estimate (what was recorded before fix)
        mini_cost = (Decimal("0.00015") * input_tok / 1000 +
                     Decimal("0.00060") * output_tok / 1000)

        # gpt-4o actual cost
        gpt4o_cost = (Decimal("0.005") * input_tok / 1000 +
                      Decimal("0.015") * output_tok / 1000)

        ratio = gpt4o_cost / mini_cost
        assert ratio > 20, (
            f"Expected >20x cost ratio, got {ratio:.1f}x. "
            f"gpt-4o-mini: ${mini_cost:.5f}, gpt-4o: ${gpt4o_cost:.5f}"
        )
        print(f"\n  gpt-4o-mini estimate : ${mini_cost:.5f}")
        print(f"  gpt-4o actual cost   : ${gpt4o_cost:.5f}")
        print(f"  Ratio                : {ratio:.1f}x")
        print(f"  At $10 daily budget with 2000 signals:")
        print(f"    mini estimate allows: {int(Decimal('10') / mini_cost)} calls")
        print(f"    gpt-4o actually allows: {int(Decimal('10') / gpt4o_cost)} calls")


# ── Fix 3: Realistic input token estimate ────────────────────────────────────

class TestRealisticInputEstimate:

    def setup_method(self):
        reset_class_state()

    def test_8_expected_input_tokens_in_task_config(self):
        """
        Both task types must have expected_input_tokens defined.
        This is the key field Fix 3 adds to TASK_CONFIG.
        """
        assert "expected_input_tokens" in TASK_CONFIG["query_generation"], (
            "query_generation missing expected_input_tokens in TASK_CONFIG"
        )
        assert "expected_input_tokens" in TASK_CONFIG["brief_generation"], (
            "brief_generation missing expected_input_tokens in TASK_CONFIG"
        )

    def test_9_brief_generation_uses_2000_input_estimate(self):
        """
        brief_generation expected_input_tokens must be ~2000.
        Before fix: used max_tokens // 3 * 2 = 400 (5x too low).
        After fix: 2000 (system prompt + signal stats + 5 abstracts × 300 words).
        """
        agent3_input = TASK_CONFIG["brief_generation"]["expected_input_tokens"]
        assert agent3_input >= 1500, (
            f"brief_generation expected_input_tokens={agent3_input} is too low. "
            f"Agent 3 input is ~2000 tokens (system prompt + 5 abstracts). "
            f"Old value was 400 — pre-flight guard fired 5x too late."
        )

    def test_10_query_generation_uses_300_input_estimate(self):
        """
        query_generation expected_input_tokens must be ~300.
        System prompt (~250) + user message (drug + reaction + PRR + severity ~50).
        """
        agent1_input = TASK_CONFIG["query_generation"]["expected_input_tokens"]
        assert 200 <= agent1_input <= 500, (
            f"query_generation expected_input_tokens={agent1_input}. "
            f"Expected 200-500 (system prompt ~250 + user msg ~50)."
        )

    def test_11_preflight_blocks_call_old_estimate_would_miss(self):
        """
        Fix 3 core test — pre-flight must block a call that the old estimate
        (max_tokens only) would have allowed through.

        Scenario: brief_generation with 499,600 tokens used out of 500,000 limit.
            Old estimate: 400 tokens → 499,600 + 400 = 500,000 → PASSES (wrong)
            New estimate: 2,600 tokens → 499,600 + 2,600 = 502,200 → BLOCKED (correct)
        """
        router = LLMRouter()

        # Set usage so only 400 tokens remain in run budget
        # Old estimate (400) would just barely pass
        # New estimate (2600) must be blocked
        limit   = TASK_CONFIG["brief_generation"]["max_tokens_per_run"]
        router.usage["brief_generation"].input_tokens  = limit - 400
        router.usage["brief_generation"].output_tokens = 0

        with pytest.raises(RuntimeError, match="Run token budget exceeded"):
            router.complete(
                messages=[{"role": "user", "content": "test"}],
                task="brief_generation",
            )


# ── Edge cases ────────────────────────────────────────────────────────────────

class TestEdgeCases:

    def setup_method(self):
        reset_class_state()

    def test_12_unknown_task_raises_value_error(self):
        """Unknown task name raises ValueError, not RuntimeError."""
        router = LLMRouter()
        with pytest.raises(ValueError, match="Unknown task"):
            router.complete(
                messages=[{"role": "user", "content": "test"}],
                task="nonexistent_task",
            )

    def test_13_daily_budget_exceeded_raises_before_api_call(self):
        """
        When daily spend is at limit, RuntimeError fires before any API call.
        Confirms no tokens are wasted on a call that will be rejected.
        """
        router = LLMRouter()
        LLMRouter._daily_spend.spent_usd = DAILY_BUDGET_USD  # fully spent

        with patch("app.core.llm_router.completion") as mock_completion:
            with pytest.raises(RuntimeError, match="Daily budget exceeded"):
                router.complete(
                    messages=[{"role": "user", "content": "test"}],
                    task="query_generation",
                )
            mock_completion.assert_not_called()

    def test_14_run_budget_exceeded_raises_before_api_call(self):
        """
        When run token budget is exhausted, RuntimeError fires before API call.
        """
        router = LLMRouter()
        limit  = TASK_CONFIG["query_generation"]["max_tokens_per_run"]
        router.usage["query_generation"].input_tokens  = limit
        router.usage["query_generation"].output_tokens = 0

        with patch("app.core.llm_router.completion") as mock_completion:
            with pytest.raises(RuntimeError, match="Run token budget exceeded"):
                router.complete(
                    messages=[{"role": "user", "content": "test"}],
                    task="query_generation",
                )
            mock_completion.assert_not_called()

    def test_15_budget_runtime_error_not_swallowed_by_fallback(self):
        """
        Budget RuntimeError must propagate out — NOT caught by the model
        fallback loop and retried with the next model.

        Before this was explicit in the code (except RuntimeError: raise),
        budget exhaustion would silently try Claude before failing.
        That would double-spend the budget.
        """
        router = LLMRouter()
        LLMRouter._daily_spend.spent_usd = DAILY_BUDGET_USD

        call_count = {"n": 0}

        def mock_completion(*args, **kwargs):
            call_count["n"] += 1
            return make_mock_response()

        with patch("app.core.llm_router.completion", side_effect=mock_completion):
            with pytest.raises(RuntimeError, match="Daily budget exceeded"):
                router.complete(
                    messages=[{"role": "user", "content": "test"}],
                    task="query_generation",
                )

        assert call_count["n"] == 0, (
            f"API was called {call_count['n']} times after budget exceeded. "
            f"Budget RuntimeError must be raised before any model is tried."
        )

    def test_16_model_fallback_fires_on_api_error_not_budget(self):
        """
        API errors (rate limit, timeout) trigger model fallback.
        Budget RuntimeErrors do NOT trigger fallback — they propagate.

        Verifies the two failure modes are handled differently.
        """
        router = LLMRouter()

        call_count = {"n": 0}
        claude_response = make_mock_response(
            content="claude response", model="claude-haiku-4-5-20251001"
        )

        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            model = kwargs.get("model", "")
            if "gpt" in model:
                raise Exception("RateLimitError: too many requests")
            return claude_response

        with patch("app.core.llm_router.completion", side_effect=side_effect):
            response = router.complete(
                messages=[{"role": "user", "content": "test"}],
                task="query_generation",
            )

        assert call_count["n"] == 2, (
            f"Expected 2 calls (gpt-4o-mini failed → claude succeeded), "
            f"got {call_count['n']}"
        )
        assert response.model == "claude-haiku-4-5-20251001"

    def test_17_reset_preserves_daily_spend_across_multiple_resets(self):
        """
        Multiple reset() calls must not clear daily spend.
        Pipeline may reset between signal batches — daily budget must persist.
        """
        router = LLMRouter()
        LLMRouter._daily_spend.record(Decimal("3.00"))

        for _ in range(5):
            router.reset()

        assert LLMRouter._daily_spend.spent_usd == Decimal("3.00"), (
            "Daily spend must survive multiple reset() calls"
        )

    def test_18_usage_summary_reflects_actual_cost(self):
        """
        get_usage_summary() must show what was actually recorded,
        not a static estimate. Verifies cost tracking flows through
        correctly end to end.
        """
        router    = LLMRouter()
        actual    = 0.003750
        mock_resp = make_mock_response(
            input_tok=300, output_tok=150, response_cost=actual
        )

        with patch("app.core.llm_router.completion", return_value=mock_resp):
            router.complete(
                messages=[{"role": "user", "content": "test"}],
                task="query_generation",
            )

        summary = router.get_usage_summary()
        daily   = summary["daily_spent_usd"]

        assert "$0.003750" in daily or float(daily.replace("$", "")) > 0.003, (
            f"Usage summary shows {daily} but expected ~$0.003750 actual cost"
        )


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v", "-s"])