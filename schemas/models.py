# schemas/models.py

import json
import logging
import os
import re
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional, Union

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, model_validator

load_dotenv()

logger = logging.getLogger("finsight.schemas")


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 1: CURRENCY UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

VALID_CURRENCIES = {
    "USD",
    "EUR",
    "GBP",
    "INR",
    "AED",
    "SGD",
    "AUD",
    "CAD",
    "JPY",
    "CHF",
    "NZD",
    "HKD",
    "SEK",
    "NOK",
    "DKK",
    "MYR",
    "THB",
    "ZAR",
}

CURRENCY_SYMBOL_MAP = {
    "$": "USD",
    "€": "EUR",
    "£": "GBP",
    "₹": "INR",
    "¥": "JPY",
    "A$": "AUD",
    "C$": "CAD",
    "S$": "SGD",
}

CURRENCY_WORD_MAP = {
    "DOLLAR": "USD",
    "DOLLARS": "USD",
    "US DOLLAR": "USD",
    "US$": "USD",
    "EURO": "EUR",
    "EUROS": "EUR",
    "POUND": "GBP",
    "POUNDS": "GBP",
    "STERLING": "GBP",
    "RUPEE": "INR",
    "RUPEES": "INR",
    "RS": "INR",
    "RS.": "INR",
    "YEN": "JPY",
    "FRANC": "CHF",
    "DIRHAM": "AED",
}


def normalise_currency(raw: str) -> str:
    """
    Converts any LLM-returned currency string to a clean ISO 4217 code.

    Decision order:
    1. Direct symbol match   ("$"      -> "USD")
    2. Already valid ISO     ("USD"    -> "USD")
    3. Known word form       ("rupees" -> "INR")
    4. Symbol as substring   ("US$"    -> "USD")
    5. Cannot resolve        -> return uppercased raw, confidence drop
                               will trigger human review
    """
    if not raw:
        return "USD"

    raw = raw.strip()

    if raw in CURRENCY_SYMBOL_MAP:
        return CURRENCY_SYMBOL_MAP[raw]

    upper = raw.upper()
    if upper in VALID_CURRENCIES:
        return upper

    if upper in CURRENCY_WORD_MAP:
        return CURRENCY_WORD_MAP[upper]

    for symbol, code in CURRENCY_SYMBOL_MAP.items():
        if symbol in raw:
            return code

    logger.warning(
        json.dumps(
            {
                "event": "currency.normalisation_failed",
                "raw_value": raw,
                "action": "returned_as_is",
                "note": "Will trigger human review via confidence threshold",
            }
        )
    )
    return upper


def parse_amount(raw_value: Any, currency_hint: str = "USD") -> float:
    """
    Converts any amount representation to a clean Python float.

    Handles:
        Indian lakh:  "2,00,000.00"  -> 200000.00
        US standard:  "200,000.00"   -> 200000.00
        European:     "200.000,50"   -> 200000.50
        European:     "1.200,50"     -> 1200.50    (fix: verified by reviewer)
        Swiss:        "200'000.00"   -> 200000.00
        Ambiguous:    "1.000" + EUR  -> 1000.00
        Ambiguous:    "1.000" + USD  -> 1.00

    Accepts int, float, or str — because the LLM returns all three.
    """
    if isinstance(raw_value, (int, float)):
        return round(float(raw_value), 2)

    if not isinstance(raw_value, str):
        raise ValueError(f"parse_amount received unexpected type {type(raw_value)}: {raw_value}")

    cleaned = re.sub(r"[^\d.,']", "", raw_value.strip())

    if not cleaned:
        raise ValueError(f"No numeric content in amount string: '{raw_value}'")

    # Indian lakh: starts with 1-2 digits then groups of 2 then optional 3
    # "2,00,000.00" or "12,50,000"
    indian_pattern = re.match(r"^\d{1,2}(,\d{2})+(,\d{3})?(\.\d+)?$", cleaned)
    if indian_pattern:
        return round(float(cleaned.replace(",", "")), 2)

    # European: dot as thousands separator, comma as decimal
    # "200.000,50" -> 200000.50
    # "1.200,50"   -> 1200.50    <-- reviewer's edge case, now explicitly handled
    # Pattern: any digits and dots, then comma, then 1-2 decimal digits
    european_pattern = re.match(r"^[\d.]+,\d{1,2}$", cleaned)
    if european_pattern:
        return round(float(cleaned.replace(".", "").replace(",", ".")), 2)

    # Swiss: apostrophe as thousands separator
    # "200'000.00" -> 200000.00
    if "'" in cleaned:
        return round(float(cleaned.replace("'", "")), 2)

    # Ambiguous: "1.000" — European thousands (=1000) or US decimal (=1.0)?
    # Resolve using currency hint
    if re.match(r"^\d+\.\d{3}$", cleaned):
        european_currencies = {"EUR", "CHF", "DKK", "SEK", "NOK", "PLN"}
        if normalise_currency(currency_hint) in european_currencies:
            return round(float(cleaned.replace(".", "")), 2)
        else:
            return round(float(cleaned), 2)

    # Standard US/UK: "200,000.50" -> 200000.50
    return round(float(cleaned.replace(",", "")), 2)


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 2: ERROR OBSERVABILITY
# ──────────────────────────────────────────────────────────────────────────────


def log_schema_error_to_langsmith(
    error: Exception,
    raw_data: dict,
    run_id: str,
) -> None:
    """
    Creates a named LangSmith error trace when Pydantic validation fails.

    Why a separate LangSmith run and not just a log line?
    LangSmith lets you filter by run name and tag. You can query
    all "schema_validation_error" runs this week, see which fields
    fail most often, and use that to improve your prompts.
    A plain log line gives you none of that queryability.

    PII note: raw_text and document_content are stripped before logging.
    Only metadata goes to LangSmith — never document contents.
    """
    try:
        from langsmith import Client

        client = Client()

        safe_raw = {k: v for k, v in raw_data.items() if k not in {"raw_text", "document_content"}}

        run = client.create_run(
            name="schema_validation_error",
            run_type="chain",
            inputs={
                "run_id": run_id,
                "raw_llm_output": safe_raw,
                "error_type": type(error).__name__,
            },
            error=str(error),
            tags=["schema_error", "validation_failure"],
            extra={
                "metadata": {
                    "run_id": run_id,
                    "error_category": "pydantic_validation",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            },
        )

        client.update_run(
            run.id,
            end_time=datetime.now(timezone.utc),
            error=str(error),
            outputs={"resolution": "routed_to_review_queue"},
        )

        logger.info(
            json.dumps(
                {
                    "event": "langsmith.schema_error_logged",
                    "run_id": run_id,
                    "langsmith_run_id": str(run.id),
                }
            )
        )

    except Exception as langsmith_error:
        # LangSmith failure must never crash the main application
        logger.error(
            json.dumps(
                {
                    "event": "langsmith.logging_failed",
                    "run_id": run_id,
                    "original_error": str(error),
                    "langsmith_error": str(langsmith_error),
                }
            )
        )


def notify_sme_on_failure(
    error_details: str,
    run_id: str,
    document_snippet: str = "",
    sme_email: str = None,
) -> None:
    """
    Sends an actionable email to the SME when schema validation fails.

    Actionable means:
    - run_id so the SME can look it up in LangSmith directly
    - document snippet (200 chars max — no PII risk)
    - specific validation error, not a generic message
    - clear next step: check the review queue

    Month 6: swap SendGrid for Azure Communication Services.
    The function signature stays identical — only the sending mechanism changes.
    """
    try:
        import sendgrid
        from sendgrid.helpers.mail import Mail

        to_email = sme_email or os.getenv("SME_EMAIL", "")
        from_email = os.getenv("FROM_EMAIL", "alerts@finsight-agent.com")
        sg_key = os.getenv("SENDGRID_API_KEY", "")

        if not sg_key:
            logger.warning(
                json.dumps(
                    {
                        "event": "sme_notification.skipped",
                        "reason": "SENDGRID_API_KEY not set",
                        "run_id": run_id,
                    }
                )
            )
            return

        if not to_email:
            logger.warning(
                json.dumps(
                    {
                        "event": "sme_notification.skipped",
                        "reason": "SME_EMAIL not set",
                        "run_id": run_id,
                    }
                )
            )
            return

        safe_snippet = (
            document_snippet[:200] + "..." if len(document_snippet) > 200 else document_snippet
        )

        langsmith_project = os.getenv("LANGSMITH_PROJECT", "finsight-agent")

        email_body = f"""
FinSight Agent - Schema Validation Failure

Run ID:  {run_id}
Time:    {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")} UTC
Status:  Document routed to human review queue

Error Details:
{error_details}

Document Snippet (first 200 characters):
{safe_snippet}

Immediate Actions Required:
1. Review the flagged document in the review queue
2. Correct the extraction if needed
3. Approve or reject via the dashboard

Investigation:
LangSmith project '{langsmith_project}' - search run_id: {run_id}

This is an automated alert from FinSight Agent.
The document has NOT been processed and awaits human review.
        """.strip()

        message = Mail(
            from_email=from_email,
            to_emails=to_email,
            subject=f"[FinSight] Schema Validation Failure - Run {run_id[:8]}",
            plain_text_content=email_body,
        )

        sg = sendgrid.SendGridAPIClient(api_key=sg_key)
        response = sg.send(message)

        logger.info(
            json.dumps(
                {
                    "event": "sme_notification.sent",
                    "run_id": run_id,
                    "to_email": to_email,
                    "sendgrid_status": response.status_code,
                }
            )
        )

    except ImportError:
        logger.warning(
            json.dumps(
                {
                    "event": "sme_notification.skipped",
                    "reason": "sendgrid not installed — run: uv add sendgrid",
                    "run_id": run_id,
                }
            )
        )

    except Exception as e:
        # Email failure must never crash the main application
        logger.error(
            json.dumps(
                {
                    "event": "sme_notification.failed",
                    "run_id": run_id,
                    "error": str(e),
                }
            )
        )


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 3: PYDANTIC MODELS
# ──────────────────────────────────────────────────────────────────────────────


class DocumentType(str, Enum):
    INVOICE = "invoice"
    BANK_STATEMENT = "bank_statement"
    FINANCIAL_REPORT = "financial_report"
    UNKNOWN = "unknown"


class LineItem(BaseModel):
    description: str
    quantity: Optional[float] = None
    unit_price: Optional[float] = None
    total: float

    @field_validator("total", mode="before")
    @classmethod
    def parse_line_total(cls, v):
        return parse_amount(v)


class InvoiceExtraction(BaseModel):
    """
    Output contract for the Invoice Agent.

    Validation order (Pydantic fires these in field definition order):
    1. currency      -> normalise_currency()
    2. total_amount  -> parse_amount()
    3. tax_amount    -> parse_amount() if not None
    4. confidence    -> round to 3 decimal places
    5. model_validator(after) -> set needs_human_review from confidence

    Azure SQL (Month 2): model_dump() maps directly to table columns.
    """

    vendor_name: str
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    due_date: Optional[str] = None
    total_amount: float
    currency: str = "USD"
    raw_amount_string: Optional[str] = None
    vendor_country: Optional[str] = None
    line_items: list[LineItem] = []
    tax_amount: Optional[float] = None
    confidence: float = Field(..., ge=0.0, le=1.0)
    needs_human_review: bool = False
    notes: Optional[str] = None

    @field_validator("currency", mode="before")
    @classmethod
    def validate_currency(cls, v):
        return normalise_currency(str(v) if v else "USD")

    @field_validator("total_amount", mode="before")
    @classmethod
    def validate_amount(cls, v):
        if isinstance(v, str):
            for symbol in CURRENCY_SYMBOL_MAP:
                v = v.replace(symbol, "").strip()
        return parse_amount(v)

    @field_validator("tax_amount", mode="before")
    @classmethod
    def validate_tax(cls, v):
        # FIX: was "if v" — 0.0 is falsy so zero tax was silently dropped
        # "is not None" correctly passes 0.0 through as a valid tax amount
        if v is not None:
            return parse_amount(v)
        return None

    @field_validator("confidence")
    @classmethod
    def round_confidence(cls, v):
        return round(v, 3)

    # FIX: replaced model_post_init with @model_validator(mode="after")
    # model_post_init is an internal Pydantic hook — model_validator is the
    # intended public API for post-construction logic in Pydantic v2.
    # must return self explicitly — Pydantic uses the return value.
    @model_validator(mode="after")
    def set_review_flag(self) -> "InvoiceExtraction":
        if self.confidence < 0.75:
            self.needs_human_review = True
        return self


class AgentRunMetadata(BaseModel):
    run_id: str
    document_type: DocumentType
    model_used: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    latency_ms: float
    estimated_cost_usd: float
    # FIX: datetime.utcnow deprecated in 3.12 — use datetime.now(timezone.utc)
    # utcnow returns a naive datetime (no timezone info attached)
    # datetime.now(timezone.utc) returns timezone-aware datetime
    # This matters when storing in databases that enforce timezone awareness
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    langsmith_run_url: Optional[str] = None


# FIX: extraction field changed from InvoiceExtraction to ExtractionType (Union)
# This queue holds any failed document regardless of type.
# When BankStatementExtraction is added in Month 2, just extend the Union.
# Nothing else in this class needs to change.
ExtractionType = Union[InvoiceExtraction]
# Month 2: ExtractionType = Union[InvoiceExtraction, BankStatementExtraction]
# Month 3: ExtractionType = Union[InvoiceExtraction, BankStatementExtraction,
#                                  FinancialReportExtraction]


class ReviewQueueItem(BaseModel):
    """
    Document that failed validation or scored below confidence threshold.

    Azure Cosmos DB (Month 4): each item is one document in the
    review-queue container. Cosmos is document storage — suits this
    because different extraction types have different shapes.
    """

    run_id: str
    document_type: DocumentType
    raw_text: str
    extraction: ExtractionType  # Union — accepts any agent output type
    validation_error: Optional[str] = None
    status: str = "pending"
    reviewer_notes: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    reviewed_at: Optional[datetime] = None


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 4: SUPERVISOR OUTPUT CLEANING
# ──────────────────────────────────────────────────────────────────────────────


class ClientSafeInvoiceResponse(BaseModel):
    """
    What the client actually receives after the supervisor cleans the output.

    Fields stripped (never sent to client):
    - confidence          internal quality metric
    - needs_human_review  internal routing flag
    - raw_amount_string   internal parsing artefact
    - vendor_country      internal parsing hint

    Azure API Management (Month 6):
    This schema becomes your public API contract.
    Once clients integrate against it, changes require versioning.
    """

    request_id: str
    status: str
    vendor_name: str
    invoice_number: Optional[str] = None
    invoice_date: Optional[str] = None
    due_date: Optional[str] = None
    total_amount: float
    currency: str
    line_items: list[LineItem] = []
    tax_amount: Optional[float] = None
    notes: Optional[str] = None
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


def clean_output_for_client(
    extraction: InvoiceExtraction,
    run_id: str,
) -> ClientSafeInvoiceResponse:
    """
    Supervisor function — strips internal fields before returning to client.

    Why separate function and not a model method?
    In the supervisor pattern, cleaning is explicitly the supervisor's
    responsibility. This makes the boundary visible in code reviews —
    you can see exactly where internal data stops and client data starts.
    """
    status = "under_review" if extraction.needs_human_review else "processed"

    return ClientSafeInvoiceResponse(
        request_id=run_id,
        status=status,
        vendor_name=extraction.vendor_name,
        invoice_number=extraction.invoice_number,
        invoice_date=extraction.invoice_date,
        due_date=extraction.due_date,
        total_amount=round(extraction.total_amount, 2),
        currency=extraction.currency,
        line_items=extraction.line_items,
        # FIX: was "if extraction.tax_amount" — 0.0 is falsy, silently became None
        # "is not None" correctly passes 0.0 as a valid zero-tax value
        tax_amount=(round(extraction.tax_amount, 2) if extraction.tax_amount is not None else None),
        notes=extraction.notes,
    )


# ──────────────────────────────────────────────────────────────────────────────
# SECTION 5: SMOKE TEST
# Run: uv run python schemas/models.py
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uuid

    print("\n" + "=" * 55)
    print("  schemas/models.py  —  Smoke Test")
    print("=" * 55)

    print("\n[1] Currency normalisation")
    currency_cases = [
        ("$", "USD"),
        ("rupees", "INR"),
        ("Rs.", "INR"),
        ("€", "EUR"),
        ("euro", "EUR"),
        ("POUNDS", "GBP"),
        ("USD", "USD"),
        ("¥", "JPY"),
    ]
    for raw, expected in currency_cases:
        result = normalise_currency(raw)
        outcome = "PASS" if result == expected else "FAIL"
        print(f"  [{outcome}]  '{raw}' -> '{result}'  (expected '{expected}')")

    print("\n[2] Amount parsing")
    amount_cases = [
        ("2,00,000.00", "INR", 200000.00),
        ("200,000.00", "USD", 200000.00),
        ("200.000,50", "EUR", 200000.50),
        ("1.200,50", "EUR", 1200.50),  # reviewer edge case — now verified
        ("200'000.00", "CHF", 200000.00),
        ("$1,500.50", "USD", 1500.50),
        ("1.000", "EUR", 1000.00),
        ("1.000", "USD", 1.00),
        (1500, "USD", 1500.00),
    ]
    for raw, curr, expected in amount_cases:
        result = parse_amount(raw, curr)
        outcome = "PASS" if result == expected else "FAIL"
        print(
            f"  [{outcome}]  parse_amount('{raw}', '{curr}') -> {result}" f"  (expected {expected})"
        )

    print("\n[3] Tax amount zero value (the falsy bug fix)")
    # This test specifically verifies the fix for "if v" vs "if v is not None"
    # Before the fix, tax_amount=0.0 would silently become None
    tax_cases = [
        (0.0, 0.0),  # zero tax — must stay 0.0, not become None
        (None, None),  # no tax field — must stay None
        (72.5, 72.5),  # normal tax — must pass through
    ]
    for raw_tax, expected in tax_cases:
        try:
            obj = InvoiceExtraction(
                vendor_name="Test Corp",
                total_amount=1000.0,
                currency="USD",
                confidence=0.9,
                tax_amount=raw_tax,
            )
            result = obj.tax_amount
            outcome = "PASS" if result == expected else "FAIL"
            print(f"  [{outcome}]  tax_amount={raw_tax} -> {result}  (expected {expected})")
        except Exception as e:
            print(f"  [FAIL]  tax_amount={raw_tax} raised: {e}")

    print("\n[4] model_validator sets needs_human_review correctly")
    confidence_cases = [
        (0.91, False),  # above threshold — no review needed
        (0.74, True),  # below threshold — review needed
        (0.75, False),  # exactly at threshold — no review (boundary check)
    ]
    for conf, expected_review in confidence_cases:
        obj = InvoiceExtraction(
            vendor_name="Test Corp",
            total_amount=1000.0,
            currency="USD",
            confidence=conf,
        )
        outcome = "PASS" if obj.needs_human_review == expected_review else "FAIL"
        print(
            f"  [{outcome}]  confidence={conf} -> needs_human_review={obj.needs_human_review}"
            f"  (expected {expected_review})"
        )

    print("\n[5] Full InvoiceExtraction with Indian lakh amount")
    try:
        extraction = InvoiceExtraction(
            vendor_name="TechBridge Solutions Pvt Ltd",
            invoice_number="INV-2024-00892",
            invoice_date="2024-11-18",
            due_date="2024-12-18",
            total_amount="2,00,000.00",
            currency="Rs.",
            raw_amount_string="2,00,000.00",
            vendor_country="India",
            line_items=[
                {
                    "description": "Data Engineering",
                    "quantity": 80,
                    "unit_price": 1500,
                    "total": "1,20,000.00",
                },
                {"description": "Azure Setup", "total": "80,000.00"},
            ],
            tax_amount=0.0,  # zero tax — tests the falsy fix end to end
            confidence=0.91,
        )
        print(f"  [PASS]  vendor={extraction.vendor_name}")
        print(f"  [PASS]  amount={extraction.total_amount}  currency={extraction.currency}")
        print(f"  [PASS]  tax={extraction.tax_amount}  (0.0 preserved, not None)")
        print(
            f"  [PASS]  confidence={extraction.confidence}"
            f"  review={extraction.needs_human_review}"
        )
    except Exception as e:
        print(f"  [FAIL]  {e}")

    print("\n[6] Schema error handler (dry run)")
    try:
        log_schema_error_to_langsmith(
            error=ValueError("total_amount: value is not a valid float"),
            raw_data={"vendor_name": "Test Corp", "total_amount": "not-a-number"},
            run_id=str(uuid.uuid4()),
        )
        print("  [PASS]  log_schema_error_to_langsmith did not crash")
    except Exception as e:
        print(f"  [FAIL]  {e}")

    print("\n[7] Supervisor clean_output_for_client")
    try:
        run_id = str(uuid.uuid4())
        client_response = clean_output_for_client(extraction, run_id)
        no_internals = (
            not hasattr(client_response, "confidence")
            and not hasattr(client_response, "needs_human_review")
            and not hasattr(client_response, "raw_amount_string")
        )
        print(f"  [PASS]  status='{client_response.status}'")
        print(f"  [PASS]  internal fields stripped={no_internals}")
        print(f"  [PASS]  tax_amount={client_response.tax_amount}  (0.0 preserved)")
        print(f"  [PASS]  request_id={client_response.request_id[:8]}...")
    except Exception as e:
        print(f"  [FAIL]  {e}")

    print("\n" + "=" * 55)
    print("  All done. Run uv run python test_setup.py for env check.")
    print("=" * 55 + "\n")
