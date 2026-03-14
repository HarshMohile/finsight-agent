# tests/test_risk_scorer.py

from tools.risk_scorer import calculate_risk_score


def test_clean_invoice_approves():
    result = calculate_risk_score(
        {
            "math_status": "pass",
            "date_status": "pass",
            "field_status": "clean",
            "rate_status": "pass",
            "vendor_found": True,
            "contract_found": True,
            "bad_history": False,
        }
    )
    assert result.recommendation == "approve"


def test_unknown_vendor_rejects():
    result = calculate_risk_score(
        {
            "vendor_found": False,
            "contract_found": False,
        }
    )
    assert result.recommendation in ("reject", "escalate")


def test_multiple_issues_escalates():
    result = calculate_risk_score(
        {
            "math_status": "fail",
            "date_status": "fail",
            "field_status": "suspicious",
            "rate_status": "warning",
            "vendor_found": False,
            "bad_history": True,
        }
    )
    assert result.recommendation == "escalate"
    assert result.score == 1.0


def test_score_never_exceeds_one():
    result = calculate_risk_score(
        {
            "math_status": "fail",
            "date_status": "fail",
            "field_status": "failed",
            "rate_status": "warning",
            "vendor_found": False,
            "contract_found": False,
            "bad_history": True,
        }
    )
    assert result.score <= 1.0
