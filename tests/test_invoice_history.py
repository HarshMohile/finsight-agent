# tests/test_invoice_history.py

from tools.invoice_history import get_invoice_history


def test_no_history_returns_summary(sample_history_vendor_id):
    result = get_invoice_history(sample_history_vendor_id)
    assert result.vendor_id == sample_history_vendor_id
    assert isinstance(result.total_invoices, int)
    assert isinstance(result.message, str)


def test_empty_vendor_id_handled():
    result = get_invoice_history("")
    assert result.total_invoices == 0
    assert "No vendor ID" in result.message


def test_invalid_vendor_returns_gracefully():
    result = get_invoice_history("INVALID-VENDOR-999")
    assert result.total_invoices == 0
    assert result.flagged_count == 0
