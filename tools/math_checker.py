# tools/math_checker.py
#
# Tool 1 of 7 in the investigation agent's toolkit.
# Verifies invoice arithmetic deterministically.
#
# Why deterministic and not LLM?
# LLMs are unreliable at arithmetic.
# A rule that always works is better than a model that usually works
# when the stakes are financial.
#
# Called by: investigation agent via LangChain tool wrapper
# Reports to: supervisor via agent state

import logging
from dataclasses import dataclass

logger = logging.getLogger("finsight.math_checker")

TOLERANCE = 1.0  # 1 unit — handles legitimate rounding differences


@dataclass
class MathCheckResult:
    status: str  # "pass" | "warning" | "fail"
    expected_total: float
    actual_total: float
    difference: float
    message: str


def check_invoice_math(
    line_items: list[dict],
    total_amount: float,
    tax_amount: float | None = None,
) -> MathCheckResult:
    """
    Verifies line items sum to total_amount.

    The LLM calls this tool and receives the result as a string.
    It then reasons about whether to investigate further or flag immediately.

    Three outcomes:
      pass    — arithmetic correct, proceed to next investigation
      warning — mismatch found, LLM decides if significant enough to flag
      fail    — no line items, LLM cannot verify, flags for human review
    """
    if not line_items:
        return MathCheckResult(
            status="fail",
            expected_total=total_amount,
            actual_total=0.0,
            difference=total_amount,
            message="No line items found. Cannot verify total.",
        )

    line_sum = round(sum(float(item.get("total", 0) or 0) for item in line_items), 2)

    tax = round(float(tax_amount or 0), 2)
    computed_total = round(line_sum + tax, 2)
    difference = round(abs(computed_total - total_amount), 2)

    if difference <= TOLERANCE:
        return MathCheckResult(
            status="pass",
            expected_total=total_amount,
            actual_total=computed_total,
            difference=difference,
            message=(
                f"Arithmetic verified. "
                f"Line items {line_sum} + tax {tax} = {computed_total} "
                f"matches invoice total {total_amount}."
            ),
        )

    return MathCheckResult(
        status="warning",
        expected_total=total_amount,
        actual_total=computed_total,
        difference=difference,
        message=(
            f"Arithmetic mismatch. "
            f"Line items {line_sum} + tax {tax} = {computed_total} "
            f"but invoice total is {total_amount}. "
            f"Difference of {difference}. "
            f"Possible causes: undisclosed discount, overbilling, "
            f"missing line item, or data extraction error."
        ),
    )


def as_langchain_tool():
    """
    Wraps check_invoice_math as a LangChain tool.
    The investigation agent calls this directly.
    Returns result as a string so the LLM can reason about it.
    """
    from langchain_core.tools import tool

    @tool
    def math_checker(
        line_items: list[dict],
        total_amount: float,
        tax_amount: float | None = None,
    ) -> str:
        """
        Verifies that invoice line items sum to the total amount.
        Use this tool first on every invoice before any other investigation.
        Returns arithmetic verification result with explanation.
        """
        result = check_invoice_math(line_items, total_amount, tax_amount)
        return (
            f"MATH CHECK: {result.status.upper()}\n"
            f"Expected: {result.expected_total}\n"
            f"Computed: {result.actual_total}\n"
            f"Difference: {result.difference}\n"
            f"Detail: {result.message}"
        )

    return math_checker
