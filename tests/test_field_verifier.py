# tests/test_field_verifier.py

from tools.field_verifier import verify_all_fields, verify_field


def test_present_field_verified(sample_raw_text):
    result = verify_field(
        "vendor_name",
        "Oroboros Solutions Pvt Ltd",
        sample_raw_text,
    )
    assert result.status == "verified"
    assert result.confidence >= 0.8


def test_hallucinated_field_suspicious(sample_raw_text):
    result = verify_field(
        "vendor_name",
        "TechBridge Solutions",
        sample_raw_text,
    )
    assert result.status == "suspicious"


def test_none_value_fails(sample_raw_text):
    result = verify_field("vendor_name", None, sample_raw_text)
    assert result.status == "failed"


def test_all_fields_clean(sample_extraction, sample_raw_text):
    summary = verify_all_fields(sample_extraction, sample_raw_text)
    assert summary["overall_status"] == "clean"
    assert summary["suspicious_fields"] == []


def test_hallucinated_vendor_flagged(sample_raw_text):
    bad_extraction = {
        "vendor_name": "Completely Wrong Vendor",
        "invoice_number": "INV-2024-00892",
        "total_amount": "2,36,000.00",
        "invoice_date": "2024-11-18",
    }
    summary = verify_all_fields(bad_extraction, sample_raw_text)
    assert "vendor_name" in summary["suspicious_fields"]


def test_multiple_hallucinations_fails(sample_raw_text):
    bad_extraction = {
        "vendor_name": "Wrong Vendor",
        "invoice_number": "INV-FAKE-999",
        "total_amount": "2,36,000.00",
        "invoice_date": "2024-11-18",
    }
    summary = verify_all_fields(bad_extraction, sample_raw_text)
    assert summary["overall_status"] == "failed"
