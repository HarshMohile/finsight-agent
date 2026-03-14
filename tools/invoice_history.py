# tools/invoice_history.py
#
# Tool 5 of 6 in the investigation agent's toolkit.
# Reads past processed invoices from Azure Blob.
# Gives the LLM pattern detection across invoice history.
#
# Called by: investigation agent
# Reports to: supervisor via agent state

import json
import logging
import os
from dataclasses import dataclass, field

from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("finsight.invoice_history")

logging.getLogger("azure").setLevel(logging.WARNING)


@dataclass
class HistorySummary:
    vendor_id: str
    total_invoices: int
    flagged_count: int
    total_billed: float
    avg_risk_score: float
    pattern_flags: list[str] = field(default_factory=list)
    message: str = ""


def _get_blob_client() -> BlobServiceClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not set in .env")
    return BlobServiceClient.from_connection_string(conn_str)


def get_invoice_history(vendor_id: str) -> HistorySummary:
    """
    Reads past processed invoices for a vendor from Azure Blob.

    Looks in processed/ container for JSON invoice records.
    Parquet support added in Month 2 when pipeline writes structured output.
    For now reads JSON summary records written by supervisor after each run.

    Returns HistorySummary with pattern analysis.
    LLM uses this to detect repeat overbilling or fraud patterns.
    """
    if not vendor_id:
        return HistorySummary(
            vendor_id="unknown",
            total_invoices=0,
            flagged_count=0,
            total_billed=0.0,
            avg_risk_score=0.0,
            message="No vendor ID provided. Cannot retrieve history.",
        )

    try:
        client = _get_blob_client()
        container = client.get_container_client("processed")
        blobs = list(container.list_blobs(name_starts_with=f"history/{vendor_id}/"))

        if not blobs:
            return HistorySummary(
                vendor_id=vendor_id,
                total_invoices=0,
                flagged_count=0,
                total_billed=0.0,
                avg_risk_score=0.0,
                message=f"No invoice history found for vendor {vendor_id}. First invoice.",
            )

        records = []
        for blob in blobs:
            raw = container.get_blob_client(blob.name).download_blob().readall()
            record = json.loads(raw)
            records.append(record)

        total_invoices = len(records)
        flagged_count = sum(1 for r in records if r.get("status") in ("flagged", "rejected"))
        total_billed = round(sum(float(r.get("total_amount", 0)) for r in records), 2)
        risk_scores = [float(r.get("risk_score", 0)) for r in records if r.get("risk_score")]
        avg_risk_score = round(sum(risk_scores) / len(risk_scores), 3) if risk_scores else 0.0

        # detect patterns across history
        pattern_flags = []

        flag_rate = flagged_count / total_invoices if total_invoices else 0
        if flag_rate >= 0.5:
            pattern_flags.append(f"high_flag_rate_{int(flag_rate * 100)}pct")

        if avg_risk_score >= 0.7:
            pattern_flags.append(f"high_avg_risk_{avg_risk_score}")

        all_flags = []
        for r in records:
            all_flags.extend(r.get("flags", []))

        recurring = [f for f in set(all_flags) if all_flags.count(f) >= 2]
        pattern_flags.extend([f"recurring_{f}" for f in recurring])

        if not pattern_flags:
            message = (
                f"Vendor has {total_invoices} past invoice(s). "
                f"No recurring issues detected. "
                f"Total billed: {total_billed}."
            )
        else:
            message = (
                f"Vendor has {total_invoices} past invoice(s), "
                f"{flagged_count} flagged. "
                f"Patterns detected: {pattern_flags}. "
                f"Total billed: {total_billed}. "
                f"Recommend elevated scrutiny."
            )

        return HistorySummary(
            vendor_id=vendor_id,
            total_invoices=total_invoices,
            flagged_count=flagged_count,
            total_billed=total_billed,
            avg_risk_score=avg_risk_score,
            pattern_flags=pattern_flags,
            message=message,
        )

    except Exception as e:
        logger.error(f"Failed to retrieve history for {vendor_id}: {e}")
        return HistorySummary(
            vendor_id=vendor_id,
            total_invoices=0,
            flagged_count=0,
            total_billed=0.0,
            avg_risk_score=0.0,
            message=f"History retrieval failed: {str(e)}",
        )


def as_langchain_tool():
    from langchain_core.tools import tool

    @tool
    def invoice_history(vendor_id: str) -> str:
        """
        Retrieves past invoice history for a vendor from Azure Blob.
        Use this tool when a rate mismatch or anomaly is found to check
        if this vendor has a pattern of overbilling or fraud.
        Returns pattern analysis across all past invoices.
        """
        result = get_invoice_history(vendor_id)
        return (
            f"INVOICE HISTORY: vendor={result.vendor_id}\n"
            f"Total invoices: {result.total_invoices}\n"
            f"Flagged: {result.flagged_count}\n"
            f"Avg risk score: {result.avg_risk_score}\n"
            f"Patterns: {result.pattern_flags or 'none'}\n"
            f"Detail: {result.message}"
        )

    return invoice_history
