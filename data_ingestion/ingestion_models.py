# data_ingestion/ingestion_models.py

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class IngestionStatus(str, Enum):
    PENDING = "pending"  # extracted, not yet sent to agent
    SENT = "sent"  # sent to agent for processing
    DONE = "done"  # agent finished, output written
    FAILED = "failed"  # extraction or agent failed


class IngestionRecord(BaseModel):
    """
    One row in raw_invoices.parquet.
    Represents a document after text extraction, before LLM processing.
    """

    file_name: str
    file_path: str
    raw_text: str
    page_count: int
    file_size_kb: float
    status: IngestionStatus = IngestionStatus.PENDING
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    error: str = ""
