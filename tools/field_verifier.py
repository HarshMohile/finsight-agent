# tools/field_verifier.py
#
# Tool 3 of 7 in the investigation agent's toolkit.
# Verifies extracted fields exist in source document.
# Catches LLM hallucinations before they reach the client.
#
# Called by: investigation agent
# Reports to: supervisor via agent state

import logging
from dataclasses import dataclass

from rapidfuzz import fuzz

logger = logging.getLogger("finsight.field_verifier")

# Minimum similarity score to consider a field verified
# 80 chosen because it allows natural variations
# e.g. "Oroboros Solutions" vs "Oroboros Solutions Pvt Ltd"
SIMILARITY_THRESHOLD = 80


# Fields that must be verified — hallucination here is critical
CRITICAL_FIELDS = [
    "vendor_name",
    "invoice_number",
    "total_amount",
    "invoice_date",
]

DATE_FIELDS = {"invoice_date", "due_date"}


@dataclass
class FieldVerifyResult:
    field_name: str
    status: str  # "verified" | "suspicious" | "failed"
    confidence: float  # 0.0 to 1.0
    message: str


def verify_field(
    field_name: str,
    extracted_value: str | None,
    raw_text: str,
) -> FieldVerifyResult:
    if not extracted_value:
        return FieldVerifyResult(
            field_name=field_name,
            status="failed",
            confidence=0.0,
            message=f"{field_name} is None — nothing to verify.",
        )

    if not raw_text:
        return FieldVerifyResult(
            field_name=field_name,
            status="failed",
            confidence=0.0,
            message="Raw text is empty — cannot verify anything.",
        )

    value_str = str(extracted_value).strip()

    # Date fields — check year and day appear in text
    # LLMs normalise dates: "2024-11-18" vs "18 November 2024"
    # Fuzzy match fails on these — extract year instead
    if field_name in DATE_FIELDS:
        year = value_str[:4]
        if year in raw_text:
            return FieldVerifyResult(
                field_name=field_name,
                status="verified",
                confidence=0.9,
                message=f"{field_name} year {year} found in source document.",
            )
        return FieldVerifyResult(
            field_name=field_name,
            status="suspicious",
            confidence=0.0,
            message=f"{field_name} year {year} not found in source document.",
        )

    # All other fields — fuzzy match
    score = fuzz.partial_ratio(value_str.lower(), raw_text.lower())
    confidence = round(score / 100, 3)

    if score >= SIMILARITY_THRESHOLD:
        return FieldVerifyResult(
            field_name=field_name,
            status="verified",
            confidence=confidence,
            message=(
                f"{field_name} '{value_str}' found in source " f"document with {score}% match."
            ),
        )

    return FieldVerifyResult(
        field_name=field_name,
        status="suspicious",
        confidence=confidence,
        message=(
            f"{field_name} '{value_str}' not found in source document. "
            f"Best match score: {score}%. "
            f"Possible hallucination — review required."
        ),
    )


def verify_all_fields(
    extraction: dict,
    raw_text: str,
) -> dict:
    """
    Runs verify_field on all critical fields at once.
    Returns summary dict the supervisor reads.

    Called by investigation agent after extraction
    to check all critical fields in one tool call.

    Returns:
      {
        "overall_status":     "clean" | "suspicious" | "failed"
        "suspicious_fields":  ["vendor_name", ...]
        "results":            {field_name: FieldVerifyResult}
        "message":            summary for LLM
      }
    """
    results = {}
    suspicious_fields = []

    for field in CRITICAL_FIELDS:
        value = extraction.get(field)
        result = verify_field(field, str(value) if value else None, raw_text)
        results[field] = result

        if result.status in ("suspicious", "failed"):
            suspicious_fields.append(field)

    if not suspicious_fields:
        overall_status = "clean"
        message = "All critical fields verified against source document."
    elif len(suspicious_fields) >= 2:
        overall_status = "failed"
        message = (
            f"Multiple critical fields unverified: {suspicious_fields}. "
            f"High hallucination risk. Recommend rejection."
        )
    else:
        overall_status = "suspicious"
        message = f"Field verification issues: {suspicious_fields}. " f"Manual review recommended."

    return {
        "overall_status": overall_status,
        "suspicious_fields": suspicious_fields,
        "results": results,
        "message": message,
    }


def as_langchain_tool():
    from langchain_core.tools import tool

    @tool
    def field_verifier(
        extraction: dict,
        raw_text: str,
    ) -> str:
        """
        Verifies all critical extracted fields exist in the source document.
        Use this tool after arithmetic and date checks to detect hallucinations.
        Returns verification status for each critical field.
        """
        summary = verify_all_fields(extraction, raw_text)
        lines = [
            f"FIELD VERIFICATION: {summary['overall_status'].upper()}",
            f"Suspicious fields: {summary['suspicious_fields'] or 'none'}",
            f"Detail: {summary['message']}",
        ]
        for field, result in summary["results"].items():
            lines.append(f"  {field}: {result.status} " f"(confidence {result.confidence})")
        return "\n".join(lines)

    return field_verifier
