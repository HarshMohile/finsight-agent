# tests/test_date_checker.py

from tools.date_checker import check_invoice_dates


def test_valid_dates_pass(valid_dates):
    result = check_invoice_dates(
        valid_dates["invoice_date"],
        valid_dates["due_date"],
    )
    assert result.status == "pass"
    assert result.flags == []


def test_missing_invoice_date_fails():
    result = check_invoice_dates(None, "2024-12-18")
    assert result.status == "fail"
    assert "missing_invoice_date" in result.flags


def test_missing_due_date_warns():
    result = check_invoice_dates("2024-11-18", None)
    assert result.status == "warning"
    assert "missing_due_date" in result.flags


def test_due_before_invoice_fails():
    result = check_invoice_dates("2024-11-18", "2024-10-01")
    assert result.status == "fail"
    assert "due_date_before_invoice_date" in result.flags


def test_excessive_payment_terms_warns():
    result = check_invoice_dates("2024-01-01", "2024-06-01")
    assert result.status == "warning"
    assert any("excessive_payment_terms" in f for f in result.flags)


def test_various_date_formats_handled():
    result = check_invoice_dates("18 November 2024", "18 December 2024")
    assert result.status == "pass"
