# tools/metadata_lookup.py
#
# Reads vendor, contract and policy metadata from Azure Blob Storage.
# Called by verification_agent to get ground truth for comparison.
#
# Azure connection: metadata container in finsightstorage
# Caches downloads in memory for duration of one pipeline run.

import json
import logging
import os
from functools import lru_cache

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("finsight.metadata_lookup")
logging.getLogger("azure").setLevel(logging.WARNING)


def _get_blob_client() -> BlobServiceClient:
    """
    Returns a BlobServiceClient using the connection string from .env
    In Month 3 this swaps to Key Vault — nothing else changes.
    """
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not set in .env")
    return BlobServiceClient.from_connection_string(conn_str)


def _download_json(blob_name: str) -> list | dict:
    """
    Downloads a JSON file from the metadata container.
    Called once per file per pipeline run — lru_cache handles the rest.
    """
    client = _get_blob_client()
    container = client.get_container_client("metadata")
    blob = container.get_blob_client(blob_name)
    raw_bytes = blob.download_blob().readall()
    return json.loads(raw_bytes)


@lru_cache(maxsize=4)
def _load_vendors() -> list:
    """
    Downloads vendors.json from Azure Blob once per process.
    lru_cache means second call returns from memory instantly.
    """
    data = _download_json("vendors.json")
    logger.info(f"Loaded {len(data)} vendors from Azure Blob")
    return data


@lru_cache(maxsize=4)
def _load_contracts() -> list:
    """
    Downloads contracts.json from Azure Blob once per process.
    """
    data = _download_json("contracts.json")
    logger.info(f"Loaded {len(data)} contracts from Azure Blob")
    return data


@lru_cache(maxsize=4)
def _load_policies() -> dict:
    """
    Downloads policies.json from Azure Blob once per process.
    """
    return _download_json("policies.json")


# ── Public functions — called by agents ───────────────────────────────────────


def get_vendor(gstin: str) -> dict | None:
    """
    Finds a vendor by GSTIN number.
    Returns vendor dict or None if not in approved registry.

    Called by: verification_agent
    Used for: checking if vendor is approved, verifying bank details
    """
    if not gstin:
        return None

    vendors = _load_vendors()
    match = next((v for v in vendors if v.get("gstin") == gstin.strip()), None)

    if not match:
        logger.warning(f"Vendor not found in registry: gstin={gstin}")

    return match


def get_vendor_by_name(name: str) -> dict | None:
    """
    Fallback lookup by legal name when GSTIN is not extracted.
    Uses exact match — fuzzy matching handled by field_verifier.

    Called by: verification_agent when gstin is None
    """
    if not name:
        return None

    vendors = _load_vendors()
    name_clean = name.strip().lower()

    return next((v for v in vendors if v.get("legal_name", "").lower() == name_clean), None)


def get_contract(vendor_id: str) -> dict | None:
    """
    Finds active contract for a vendor.
    Returns contract dict or None if no active contract found.

    Called by: verification_agent
    Used for: checking agreed rates, payment terms, contract validity
    """
    if not vendor_id:
        return None

    contracts = _load_contracts()
    return next((c for c in contracts if c.get("vendor_id") == vendor_id), None)


def get_policy() -> dict:
    """
    Returns company payment policies.
    Always returns a dict — never None.

    Called by: validation_agent, verification_agent
    Used for: approval thresholds, payment term rules
    """
    return _load_policies()


def clear_cache() -> None:
    """
    Clears the in-memory cache.
    Call this in tests to force fresh downloads.
    Call this if metadata is updated mid-run.
    """
    _load_vendors.cache_clear()
    _load_contracts.cache_clear()
    _load_policies.cache_clear()
    logger.info("Metadata cache cleared")


def check_rates(
    line_items: list[dict],
    vendor_id: str,
) -> dict:
    """
    Compares invoice line item rates against contracted rates.
    Called by investigation agent after get_contract().

    Returns:
      {
        "status":        "pass" | "warning" | "fail"
        "mismatches":    list of dicts with field level detail
        "total_overbilled": float
        "message":       plain English summary for LLM
      }
    """
    contract = get_contract(vendor_id)

    if not contract:
        return {
            "status": "fail",
            "mismatches": [],
            "total_overbilled": 0.0,
            "message": f"No contract found for vendor {vendor_id}. Cannot verify rates.",
        }

    agreed_rates = contract.get("agreed_rates", {})
    mismatches = []

    for item in line_items:
        category = item.get("category") or item.get("description", "").lower()
        unit_price = float(item.get("unit_price") or 0)
        quantity = float(item.get("quantity") or 1)

        # find matching category in agreed rates
        matched_rate = None
        for key in agreed_rates:
            if key.lower() in category.lower():
                matched_rate = agreed_rates[key]
                break

        if matched_rate is None:
            continue

        if unit_price > matched_rate:
            overbilled = round((unit_price - matched_rate) * quantity, 2)
            mismatches.append(
                {
                    "category": category,
                    "invoiced_rate": unit_price,
                    "agreed_rate": matched_rate,
                    "quantity": quantity,
                    "overbilled": overbilled,
                }
            )

    total_overbilled = round(sum(m["overbilled"] for m in mismatches), 2)

    if not mismatches:
        return {
            "status": "pass",
            "mismatches": [],
            "total_overbilled": 0.0,
            "message": "All line item rates match contracted rates.",
        }

    return {
        "status": "warning",
        "mismatches": mismatches,
        "total_overbilled": total_overbilled,
        "message": (
            f"{len(mismatches)} rate mismatch(es) found. "
            f"Total overbilled: {total_overbilled}. "
            f"Details: {mismatches}"
        ),
    }


def as_langchain_tool():
    from langchain_core.tools import tool

    @tool
    def metadata_lookup(
        gstin: str | None = None,
        vendor_name: str | None = None,
        vendor_id: str | None = None,
    ) -> str:
        """
        Looks up vendor, contract and policy metadata from Azure Blob.
        Use this tool first on every invoice to verify vendor is approved
        and retrieve contracted rates and payment terms.
        Pass gstin if available, vendor_name as fallback.
        """
        vendor = None
        if gstin:
            vendor = get_vendor(gstin)
        if not vendor and vendor_name:
            vendor = get_vendor_by_name(vendor_name)

        if not vendor:
            return "METADATA LOOKUP: FAIL\nVendor not found in approved registry."

        contract = get_contract(vendor["vendor_id"])
        policy = get_policy()

        return (
            f"METADATA LOOKUP: PASS\n"
            f"Vendor: {vendor['legal_name']} — status: {vendor['status']}\n"
            f"Contract: {contract['contract_id'] if contract else 'None found'}\n"
            f"Agreed rates: {contract['agreed_rates'] if contract else 'N/A'}\n"
            f"Payment terms: {contract['payment_terms'] if contract else 'N/A'}\n"
            f"Auto approve below: {policy['approval_thresholds']['auto_approve_below']}"
        )

    return metadata_lookup


def as_langchain_rate_tool():
    from langchain_core.tools import tool

    @tool
    def rate_checker(
        line_items: list[dict],
        vendor_id: str,
    ) -> str:
        """
        Compares invoice line item rates against contracted rates.
        vendor_id is the internal vendor ID like "VND-001", NOT the GSTIN.
        First call metadata_lookup to get the vendor_id, then call this tool.
        Returns overbilling amount and detail per line item.
        """
        result = check_rates(line_items, vendor_id)
        return (
            f"RATE CHECK: {result['status'].upper()}\n"
            f"Total overbilled: {result['total_overbilled']}\n"
            f"Detail: {result['message']}"
        )

    return rate_checker


# ── Smoke test ─────────────────────────────────────────────────────────────────
# Run: uv run python -m tools.metadata_lookup
