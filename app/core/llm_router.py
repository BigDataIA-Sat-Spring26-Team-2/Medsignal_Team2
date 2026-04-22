"""
app/core/llm_router.py — Shared LLM router for MedSignal agent pipeline.

Used by:
    app/agents/agent1_detector.py  — query generation (~220 tokens/call)
    app/agents/agent3_assessor.py  — SafetyBrief generation (~2,100 tokens/call)

Responsibilities:
    1. Model fallback chain — tries primary model first, falls back to secondary
       if the primary fails (rate limit, timeout, API error).
    2. Run-level token budget — guards against runaway costs when running
       against all signals in signals_flagged (potentially 2,000+ signals).
    3. Daily spend budget — hard cap on total USD spent per day across all calls.
       Matches the proposal's "$10 hard spend limit" but enforced in code
       rather than relying solely on the OpenAI dashboard.
    4. Structured logging — logs every call with model, tokens, cost, budget.

Model fallback chain:
    Primary  : gpt-4o-mini (default) — set OPENAI_MODEL in .env
    Fallback : claude-haiku-4-5-20251001 — set FALLBACK_MODEL in .env
    If both fail → raises RuntimeError (callers fall back to template/error handling)

    Note: Claude fallback is beyond the original proposal scope (proposal only
    mentions OpenAI). It is included as a reliability improvement for large runs.

Task configuration (aligned with proposal p27):
    Agent 1 (query_generation) : temperature=0, max_tokens=200
    Agent 3 (brief_generation) : temperature=0, max_tokens=600
    Both use temperature=0 for reproducibility per proposal p27.

Daily budget:
    Default : $10.00 — matches proposal's documented hard spend limit
    Demo day: set DAILY_BUDGET_USD=50.00 in .env before switching to gpt-4o

    Cost estimates at scale (all signals ~2,000):
        Development (gpt-4o-mini) : ~$0.93 per full run
        Demo day (gpt-4o)         : ~$32 per full run

    Recommended .env settings:
        Development : DAILY_BUDGET_USD=10.00  (covers ~10 full runs)
        Demo day    : DAILY_BUDGET_USD=50.00  (covers 1 full run with 1.5x buffer)

Token budgets per run (separate per task type):
    query_generation : 500,000 tokens  (~2,270 Agent 1 calls)
    brief_generation : 5,000,000 tokens (~2,380 Agent 3 calls)
    Override via MAX_TOKENS_QUERY and MAX_TOKENS_BRIEF in .env.

Usage:
    from app.core.llm_router import LLMRouter

    router = LLMRouter()                        # one instance per pipeline run
    response = router.complete(
        messages = [...],
        task     = "query_generation",          # or "brief_generation"
    )

    # pipeline.py resets run-level counters before each run:
    router.reset()

Cost reference (approximate as of proposal date):
    gpt-4o-mini  : $0.15/1M input + $0.60/1M output  → blended ~$0.20/1M
    gpt-4o       : $5.00/1M input + $15.00/1M output → blended ~$7.00/1M
    claude-haiku : $0.25/1M input + $1.25/1M output  → blended ~$0.50/1M
"""

import logging
import os
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

import structlog
from dotenv import load_dotenv
from litellm import completion

load_dotenv()

log = structlog.get_logger()


# ── Model configuration ───────────────────────────────────────────────────────

# Primary model — gpt-4o-mini by default, override via OPENAI_MODEL in .env
# Switch to gpt-4o for final evaluation and demo run
PRIMARY_MODEL  = os.getenv("OPENAI_MODEL",   "gpt-4o-mini")

# Fallback model — Claude Haiku, override via FALLBACK_MODEL in .env
# Provides resilience when OpenAI is rate-limited or unavailable
FALLBACK_MODEL = os.getenv("FALLBACK_MODEL", "claude-haiku-4-5-20251001")

# Model chain — tried in order, first success wins
MODEL_CHAIN = [PRIMARY_MODEL, FALLBACK_MODEL]


# ── Task configuration ────────────────────────────────────────────────────────
# Aligned with proposal p27:
#   "Both GPT-4o calls use temperature set to 0 for reproducibility."
#   "Agent 1 uses a max token limit of 200."
#   "Agent 3 uses a max token limit of 600."

TASK_CONFIG = {
    "query_generation": {
        "temperature"        : 0,      # proposal p27: temperature=0
        "max_tokens"         : 200,    # proposal p27: max_tokens=200
        "max_tokens_per_run" : int(os.getenv("MAX_TOKENS_QUERY", "500000")),
        "expected_input_tokens" : 300,
        # 500K tokens ≈ 2,270 Agent 1 calls — covers 2,000+ signals with headroom
        "cost_per_1k_input"  : Decimal("0.00015"),  # gpt-4o-mini: $0.15/1M input
        "cost_per_1k_output" : Decimal("0.00060"),  # gpt-4o-mini: $0.60/1M output
    },
    "brief_generation": {
        "temperature"        : 0,      # proposal p27: temperature=0
        "max_tokens"         : 1000,    # proposal p27: max_tokens=600
        "max_tokens_per_run" : int(os.getenv("MAX_TOKENS_BRIEF", "5000000")),
        # 5M tokens ≈ 2,380 Agent 3 calls — covers 2,000+ signals with headroom
        "expected_input_tokens" : 2000,
        "cost_per_1k_input"  : Decimal("0.00015"),
        "cost_per_1k_output" : Decimal("0.00060"),
    },
}


# ── Daily budget ──────────────────────────────────────────────────────────────
# Hard cap on total USD spent per day across all LLM calls.
# Default $10.00 matches proposal's documented hard spend limit (proposal p42).
#
# Development (gpt-4o-mini): ~$0.93 per full run → $10 covers ~10 runs
# Demo day (gpt-4o)        : ~$32 per full run   → set DAILY_BUDGET_USD=50.00

DAILY_BUDGET_USD = Decimal(os.getenv("DAILY_BUDGET_USD", "10.00"))


# ── Daily spend tracker ───────────────────────────────────────────────────────

@dataclass
class DailySpend:
    """
    Tracks total USD spent today across all tasks and models.

    Midnight reset: if today's date differs from self.date, the counter
    resets automatically — no manual intervention needed between days.

    Shared across all task types — Agent 1 and Agent 3 draw from the
    same daily budget so the total pipeline cost is capped, not each
    agent individually.
    """
    date     : date    = field(default_factory=date.today)
    spent_usd: Decimal = Decimal("0")

    def _reset_if_new_day(self) -> None:
        if self.date != date.today():
            self.date      = date.today()
            self.spent_usd = Decimal("0")
            log.info("daily_budget_reset", new_date=str(self.date),
                     limit_usd=str(DAILY_BUDGET_USD))

    def can_spend(self, amount: Decimal) -> bool:
        self._reset_if_new_day()
        return self.spent_usd + amount <= DAILY_BUDGET_USD

    def record(self, amount: Decimal) -> None:
        self._reset_if_new_day()
        self.spent_usd += amount

    def remaining(self) -> Decimal:
        self._reset_if_new_day()
        return DAILY_BUDGET_USD - self.spent_usd


# ── Token usage tracker ───────────────────────────────────────────────────────

@dataclass
class TokenUsage:
    """
    Tracks token consumption per task type for a single pipeline run.
    Reset by LLMRouter.reset() before each run.

    input_tokens and output_tokens tracked separately for accurate cost
    calculation — input and output have different pricing on all models.
    """
    input_tokens : int     = 0
    output_tokens: int     = 0
    total_calls  : int     = 0
    total_cost   : Decimal = field(default_factory=lambda: Decimal("0"))

    @property
    def total_tokens(self) -> int:
        return self.input_tokens + self.output_tokens

    def record(self, input_tok: int, output_tok: int, cost: Decimal) -> None:
        self.input_tokens  += input_tok
        self.output_tokens += output_tok
        self.total_calls   += 1
        self.total_cost    += cost

    def summary(self) -> dict:
        return {
            "input_tokens"  : self.input_tokens,
            "output_tokens" : self.output_tokens,
            "total_tokens"  : self.total_tokens,
            "total_calls"   : self.total_calls,
            "total_cost_usd": f"${self.total_cost:.4f}",
        }


# ── LLM Router ────────────────────────────────────────────────────────────────

class LLMRouter:
    """
    Shared LLM router for Agent 1 and Agent 3.

    Manages:
        - Model fallback chain (primary → fallback → RuntimeError)
        - Per-task per-run token budget enforcement
        - Daily USD spend cap across all tasks
        - Structured logging per call

    One instance per pipeline run. Call reset() before each run to
    clear run-level token counters. Daily spend persists across resets
    (it tracks the day's total, not just one run).

    pipeline.py owns the instance and passes it into agents via
    the SignalState or directly.

    Example:
        router   = LLMRouter()
        response = router.complete(messages=[...], task="query_generation")
        router.reset()   # before next pipeline run
        print(router.get_usage_summary())
    """
    _daily_spend: "DailySpend" = None
    
    def __init__(self):
        # Run-level counters — reset before each pipeline run
        self.usage: dict[str, TokenUsage] = {
            task: TokenUsage() for task in TASK_CONFIG
        }

    def reset(self) -> None:
        """
        Resets run-level token counters for a new pipeline run.
        Daily spend is NOT reset — it accumulates across the day.
        Called by pipeline.py before processing signals.
        """
        for task in self.usage:
            self.usage[task] = TokenUsage()
        log.info(
            "llm_router_reset",
            tasks          =list(TASK_CONFIG.keys()),
            daily_spent_usd=str(self._daily_spend.spent_usd),
            daily_remaining=str(self._daily_spend.remaining()),
        )

    def get_usage_summary(self) -> dict:
        """Returns token usage summary for all task types plus daily spend."""
        return {
            "tasks"         : {task: self.usage[task].summary() for task in self.usage},
            "daily_spent_usd": f"${self._daily_spend.spent_usd:.4f}",
            "daily_limit_usd": f"${DAILY_BUDGET_USD:.2f}",
            "daily_remaining": f"${self._daily_spend.remaining():.4f}",
        }

    def _estimate_cost(
        self,
        task      : str,
        input_tok : int,
        output_tok: int,
    ) -> Decimal:
        """Estimates cost from input/output token counts."""
        config = TASK_CONFIG[task]
        return (
            config["cost_per_1k_input"]  * Decimal(input_tok)  / Decimal("1000") +
            config["cost_per_1k_output"] * Decimal(output_tok) / Decimal("1000")
        )

    def _check_run_budget(self, task: str, estimated_tokens: int) -> None:
        """
        Raises RuntimeError if adding estimated_tokens would exceed the
        per-run token budget for this task type.
        Called before each LLM call — fail fast before spending tokens.
        """
        config        = TASK_CONFIG[task]
        limit         = config["max_tokens_per_run"]
        current_total = self.usage[task].total_tokens

        if current_total + estimated_tokens > limit:
            raise RuntimeError(
                f"Run token budget exceeded for task={task}. "
                f"Used: {current_total:,} + estimated: {estimated_tokens:,} "
                f"> limit: {limit:,}. "
                f"Increase MAX_TOKENS_QUERY or MAX_TOKENS_BRIEF in .env."
            )

    def _check_daily_budget(self, estimated_cost: Decimal) -> None:
        """
        Raises RuntimeError if making this call would exceed the daily USD budget.
        Called before each LLM call.

        Default limit $10.00 — matches proposal's hard spend limit (p42).
        Override via DAILY_BUDGET_USD in .env.
        Demo day recommendation: set DAILY_BUDGET_USD=50.00
        """
        if not self._daily_spend.can_spend(estimated_cost):
            raise RuntimeError(
                f"Daily budget exceeded. "
                f"Spent: ${self._daily_spend.spent_usd:.4f}, "
                f"Limit: ${DAILY_BUDGET_USD:.2f}. "
                f"For demo day set DAILY_BUDGET_USD=50.00 in .env."
            )

    def complete(
        self,
        messages: list[dict],
        task    : str,
        **kwargs,
    ) -> Any:
        """
        Calls the LLM with model fallback chain and budget enforcement.

        Budget checks (both applied before any API call):
            1. Run-level token budget — prevents unbounded token usage per run
            2. Daily USD budget — hard cap on total spend per day

        Args:
            messages : list of {"role": ..., "content": ...} dicts
            task     : "query_generation" or "brief_generation"
            **kwargs : passed through to litellm.completion

        Returns:
            litellm completion response object.
            response.choices[0].message.content — the model's text output.
            response.usage.prompt_tokens        — input token count.
            response.usage.completion_tokens    — output token count.

        Raises:
            RuntimeError — token budget exceeded (do not catch in agents)
            RuntimeError — daily budget exceeded (do not catch in agents)
            RuntimeError — all models in MODEL_CHAIN failed
        """
        if task not in TASK_CONFIG:
            raise ValueError(
                f"Unknown task: '{task}'. "
                f"Valid tasks: {list(TASK_CONFIG.keys())}"
            )

        config     = TASK_CONFIG[task]
        last_error = None

        expected_input  = config["expected_input_tokens"]
        expected_output = config["max_tokens"]
        self._check_run_budget(task, expected_input + expected_output)

        estimated_cost = self._estimate_cost(
            task,
            input_tok  = expected_input,
            output_tok = expected_output,
        )
        self._check_daily_budget(estimated_cost)

        for model in MODEL_CHAIN:
            try:
                log.info(
                    "llm_call_start",
                    task           = task,
                    model          = model,
                    run_tokens_used= self.usage[task].total_tokens,
                    run_token_limit= config["max_tokens_per_run"],
                    daily_spent    = f"${self._daily_spend.spent_usd:.4f}",
                    daily_limit    = f"${DAILY_BUDGET_USD:.2f}",
                    daily_remaining= f"${self._daily_spend.remaining():.4f}",
                )

                response = completion(
                    model      = model,
                    messages   = messages,
                    temperature= config["temperature"],
                    max_tokens = config["max_tokens"],
                    **kwargs,
                )

                # Extract actual token usage from response
                usage      = response.usage
                input_tok  = getattr(usage, "prompt_tokens",     config["max_tokens"] // 3 * 2)
                output_tok = getattr(usage, "completion_tokens", config["max_tokens"] // 3)
                actual_cost = None
                try:
                    actual_cost = response._hidden_params.get("response_cost")
                    if actual_cost is not None:
                        cost = Decimal(str(actual_cost))
                    else:
                        cost = self._estimate_cost(task, input_tok, output_tok)
                except Exception:
                    cost = self._estimate_cost(task, input_tok, output_tok)

                # Record usage
                self.usage[task].record(input_tok, output_tok, cost)
                self._daily_spend.record(cost)

                log.info(
                    "llm_call_success",
                    task          = task,
                    model         = model,
                    input_tokens  = input_tok,
                    output_tokens = output_tok,
                    cost_usd      = f"${cost:.5f}",
                    run_total     = self.usage[task].total_tokens,
                    run_cost      = f"${self.usage[task].total_cost:.4f}",
                    daily_spent   = f"${self._daily_spend.spent_usd:.4f}",
                    daily_remaining= f"${self._daily_spend.remaining():.4f}",
                )

                return response

            except RuntimeError:
                # Budget exceeded — propagate immediately, do not try fallback
                raise

            except Exception as e:
                last_error = e
                log.warning(
                    "llm_call_failed",
                    task  = task,
                    model = model,
                    error = str(e),
                    next  = "trying fallback" if model != MODEL_CHAIN[-1] else "all models exhausted",
                )
                continue

        # All models exhausted
        raise RuntimeError(
            f"All models failed for task={task}. "
            f"Models tried: {MODEL_CHAIN}. "
            f"Last error: {last_error}. "
            f"Agents will fall back to template/error handling."
        )
    
LLMRouter._daily_spend = DailySpend()