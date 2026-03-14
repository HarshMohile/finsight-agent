# tests/test_metadata_lookup.py

from tools.metadata_lookup import (
    check_rates,
    clear_cache,
    get_contract,
    get_policy,
    get_vendor,
    get_vendor_by_name,
)


def test_vendor_found_by_gstin(sample_gstin):
    clear_cache()
    vendor = get_vendor(sample_gstin)
    assert vendor is not None
    assert vendor["status"] == "active"
    assert "legal_name" in vendor


def test_vendor_found_by_name(sample_gstin):
    clear_cache()
    vendor = get_vendor(sample_gstin)
    assert vendor is not None

    result = get_vendor_by_name(vendor["legal_name"])
    assert result is not None
    assert result["vendor_id"] == vendor["vendor_id"]


def test_unknown_vendor_returns_none():
    clear_cache()
    result = get_vendor("INVALID-000")
    assert result is None


def test_contract_found(sample_vendor_id):
    clear_cache()
    contract = get_contract(sample_vendor_id)
    assert contract is not None
    assert "agreed_rates" in contract
    assert "payment_terms" in contract


def test_unknown_contract_returns_none():
    clear_cache()
    result = get_contract("INVALID-VENDOR")
    assert result is None


def test_policy_always_returns_dict():
    clear_cache()
    policy = get_policy()
    assert isinstance(policy, dict)
    assert "approval_thresholds" in policy
    assert "payment_terms" in policy


def test_cache_clear_does_not_crash():
    clear_cache()


def test_rate_mismatch_detected(sample_vendor_id, sample_line_items_with_rates):
    result = check_rates(sample_line_items_with_rates, sample_vendor_id)
    assert result["status"] == "warning"
    assert result["total_overbilled"] > 0


def test_correct_rates_pass(sample_vendor_id):
    line_items = [
        {"description": "data_engineering", "unit_price": 1200.0, "quantity": 1, "total": 1200.0},
    ]
    result = check_rates(line_items, sample_vendor_id)
    assert result["status"] == "pass"


def test_unknown_vendor_fails():
    result = check_rates([], "INVALID-VENDOR")
    assert result["status"] == "fail"
