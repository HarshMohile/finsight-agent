# tools/risk_scorer.py
#
# Aggregates investigation findings into a single risk score.
# Pure calculation — no Azure, no LLM, no I/O.
#
# Called by: investigation agent as final step
# Score drives supervisor routing decision.

from dataclasses import dataclass, field

WEIGHTS = {
    "math_failed": 0.30,
    "math_warning": 0.15,
    "date_failed": 0.25,
    "date_warning": 0.10,
    "field_issue": 0.20,
    "rate_mismatch": 0.20,
    "vendor_missing": 0.40,
    "no_contract": 0.20,
    "bad_history": 0.25,
}


@dataclass
class RiskScore:
    score: float
    band: str
    recommendation: str
    reasons: list[str] = field(default_factory=list)


def calculate_risk_score(findings: dict) -> RiskScore:
    """
    findings keys:
      math_status       "pass" | "warning" | "fail"
      date_status       "pass" | "warning" | "fail"
      field_status      "clean" | "suspicious" | "failed"
      rate_status       "pass" | "warning"
      vendor_found      bool
      contract_found    bool
      bad_history       bool
    """
    score = 0.0
    reasons = []

    checks = [
        (findings.get("math_status") == "fail", "math_failed", "Arithmetic failed"),
        (findings.get("math_status") == "warning", "math_warning", "Arithmetic mismatch"),
        (findings.get("date_status") == "fail", "date_failed", "Date validation failed"),
        (findings.get("date_status") == "warning", "date_warning", "Date issues found"),
        (findings.get("field_status") != "clean", "field_issue", "Field verification failed"),
        (findings.get("rate_status") == "warning", "rate_mismatch", "Rate overbilling detected"),
        (not findings.get("vendor_found", True), "vendor_missing", "Vendor not in registry"),
        (not findings.get("contract_found", True), "no_contract", "No active contract found"),
        (findings.get("bad_history", False), "bad_history", "Vendor has flagged history"),
    ]

    for condition, weight_key, reason in checks:
        if condition:
            score += WEIGHTS[weight_key]
            reasons.append(reason)

    score = round(min(score, 1.0), 3)

    if score <= 0.30:
        band, recommendation = "low", "approve"
    elif score <= 0.60:
        band, recommendation = "medium", "review"
    elif score <= 0.80:
        band, recommendation = "high", "reject"
    else:
        band, recommendation = "critical", "escalate"

    return RiskScore(score, band, recommendation, reasons)


def as_langchain_tool():
    from langchain_core.tools import tool

    @tool
    def risk_scorer(findings: dict) -> str:
        """
        Calculates final risk score from all investigation findings.
        Call this last after all other tools have run.
        Returns score, band and routing recommendation for supervisor.
        """
        result = calculate_risk_score(findings)
        return (
            f"RISK SCORE: {result.score} — {result.band.upper()}\n"
            f"Recommendation: {result.recommendation.upper()}\n"
            f"Reasons: {result.reasons or 'none'}"
        )

    return risk_scorer
