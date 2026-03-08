# tests/test_metadata_lookup.py

from tools.metadata_lookup import (
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
