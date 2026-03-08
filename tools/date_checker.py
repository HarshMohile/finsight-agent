# tools/date_checker.py
#
# Tool 2 of 7 in the investigation agent's toolkit.
# Verifies invoice date logic deterministically.
#
# Called by: investigation agent
# Reports to: supervisor via agent state

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger("finsight.date_checker")

MAX_PAYMENT_DAYS = 120
DATE_FORMATS = [
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d %B %Y",
    "%B %d, %Y",
    "%d-%b-%Y",
]


@dataclass
class DateCheckResult:
    status: str
    message: str
    flags: list[str] = field(default_factory=list)


def _parse_date(raw: str) -> datetime | None:
    """
    Tries multiple date formats until one works.
    Returns None if nothing matches.
    LLMs return dates in many formats — this handles them all.
    """
    if not raw:
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw.strip(), fmt)
        except ValueError:
            continue

    return None


def check_invoice_dates(
    invoice_date: str | None,
    due_date: str | None,
) -> DateCheckResult:
    """
    Verifies invoice dates are logical and consistent.

    Called by investigation agent to check date validity
    before proceeding with rate or vendor verification.

    Returns DateCheckResult with status, message, and specific flags.
    Flags are used by supervisor to apply confidence penalties.
    """
    flags = []

    # check 1 — invoice_date must exist
    if not invoice_date:
        return DateCheckResult(
            status="fail",
            message="Invoice date is missing. Cannot verify document validity.",
            flags=["missing_invoice_date"],
        )

    parsed_invoice = _parse_date(invoice_date)
    if not parsed_invoice:
        return DateCheckResult(
            status="fail",
            message=f"Invoice date '{invoice_date}' could not be parsed.",
            flags=["unparseable_invoice_date"],
        )

    today = datetime.now(timezone.utc).replace(tzinfo=None)

    # check 2 — invoice_date should not be in the future
    if parsed_invoice > today:
        flags.append("future_invoice_date")

    # check 3 — due_date handling
    if not due_date:
        return DateCheckResult(
            status="warning",
            message="Due date missing. Cannot verify payment terms.",
            flags=["missing_due_date"],
        )

    parsed_due = _parse_date(due_date)
    if not parsed_due:
        return DateCheckResult(
            status="warning",
            message=f"Due date '{due_date}' could not be parsed.",
            flags=["unparseable_due_date"],
        )

    # check 4 — due_date must be after invoice_date
    if parsed_due < parsed_invoice:
        flags.append("due_date_before_invoice_date")
        return DateCheckResult(
            status="fail",
            message=(
                f"Due date {due_date} is before invoice date {invoice_date}. "
                f"This is logically impossible."
            ),
            flags=flags,
        )

    # check 5 — payment terms not excessively long
    days_gap = (parsed_due - parsed_invoice).days
    if days_gap > MAX_PAYMENT_DAYS:
        flags.append(f"excessive_payment_terms_{days_gap}_days")

    if flags:
        return DateCheckResult(
            status="warning",
            message=(
                f"Date issues found: {', '.join(flags)}. "
                f"Payment gap is {days_gap} days. Review recommended."
            ),
            flags=flags,
        )

    return DateCheckResult(
        status="pass",
        message=(
            f"Dates verified. Invoice: {invoice_date}. "
            f"Due: {due_date}. Payment terms: {days_gap} days."
        ),
        flags=[],
    )


def as_langchain_tool():
    from langchain_core.tools import tool

    @tool
    def date_checker(
        invoice_date: str | None,
        due_date: str | None,
    ) -> str:
        """
        Verifies invoice and due dates are logically consistent.
        Use this tool to check dates after verifying arithmetic.
        Returns date validation result with any flags found.
        """
        result = check_invoice_dates(invoice_date, due_date)
        return (
            f"DATE CHECK: {result.status.upper()}\n"
            f"Flags: {result.flags if result.flags else 'none'}\n"
            f"Detail: {result.message}"
        )

    return date_checker
