# tests/test_math_checker.py

from tools.math_checker import as_langchain_tool, check_invoice_math


def test_correct_math_passes(clean_line_items, clean_total, clean_tax):
    result = check_invoice_math(clean_line_items, clean_total, clean_tax)
    assert result.status == "pass"
    assert result.difference <= 1.0


def test_no_line_items_fails():
    result = check_invoice_math([], 100000.0)
    assert result.status == "fail"
    assert "No line items" in result.message


def test_overbilling_returns_warning():
    result = check_invoice_math(
        [{"total": 50000.0}],
        100000.0,
    )
    assert result.status == "warning"
    assert result.difference == 50000.0


def test_rounding_within_tolerance():
    result = check_invoice_math(
        [{"total": 999.99}],
        1000.0,
    )
    assert result.status == "pass"


def test_zero_tax_passes():
    result = check_invoice_math(
        [{"total": 1000.0}],
        1000.0,
        tax_amount=0.0,
    )
    assert result.status == "pass"


def test_none_total_in_line_item_handled():
    result = check_invoice_math(
        [{"description": "Service", "total": None}],
        1000.0,
    )
    assert result.status == "warning"


def test_langchain_tool_returns_string():
    tool = as_langchain_tool()
    result = tool.invoke(
        {
            "line_items": [{"total": 1000.0}],
            "total_amount": 1000.0,
        }
    )
    assert isinstance(result, str)
    assert "MATH CHECK" in result
