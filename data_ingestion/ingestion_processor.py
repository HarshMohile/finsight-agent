# data_ingestion/ingestion_processor.py
#
# Stage 1 of 2 in the pipeline.
# Job: PDF -> extracted text -> IngestionRecord -> Parquet
#
# Does NOT call the LLM.
# Does NOT touch schemas/models.py.
# Those belong to Stage 2.
#
# Usage:
#   uv run python data_ingestion/ingestion_processor.py

import json
import logging
from pathlib import Path

import pandas as pd
import pdfplumber

from data_ingestion.ingestion_models import IngestionRecord, IngestionStatus

logger = logging.getLogger("finsight.ingestion")


def _log(level: str, event: str, **kwargs) -> None:
    getattr(logger, level)(json.dumps({"event": event, **kwargs}))


# ── Core functions ─────────────────────────────────────────────────────────────


def extract_text_from_pdf(file_path: Path) -> tuple[str, int]:
    """
    Opens a PDF and returns (raw_text, page_count).

    Joins pages with a newline separator so the LLM sees
    clear page boundaries in the text.

    Returns empty string if the PDF has no extractable text
    (scanned images need OCR — out of scope for now, logged as warning).
    """
    text_parts = []

    with pdfplumber.open(file_path) as pdf:
        page_count = len(pdf.pages)
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text.strip())

    raw_text = "\n\n".join(text_parts)

    if not raw_text:
        _log(
            "warning",
            "pdf.no_text_extracted",
            file=file_path.name,
            note="PDF may be scanned image — OCR not yet supported",
        )

    return raw_text, page_count


def build_ingestion_record(file_path: Path) -> IngestionRecord:
    """
    Processes one PDF and returns an IngestionRecord.

    On extraction failure the record is returned with
    status=FAILED and the error message captured.
    The batch continues — one bad file does not stop the rest.
    """
    _log("info", "pdf.processing_start", file=file_path.name)

    try:
        raw_text, page_count = extract_text_from_pdf(file_path)
        file_size_kb = round(file_path.stat().st_size / 1024, 2)

        record = IngestionRecord(
            file_name=file_path.name,
            file_path=str(file_path.resolve()),
            raw_text=raw_text,
            page_count=page_count,
            file_size_kb=file_size_kb,
            status=IngestionStatus.PENDING if raw_text else IngestionStatus.FAILED,
            error="" if raw_text else "No extractable text found",
        )

        _log(
            "info",
            "pdf.processing_done",
            file=file_path.name,
            pages=page_count,
            chars=len(raw_text),
            status=record.status,
        )

        return record

    except Exception as e:
        _log("error", "pdf.processing_failed", file=file_path.name, error=str(e))

        return IngestionRecord(
            file_name=file_path.name,
            file_path=str(file_path.resolve()),
            raw_text="",
            page_count=0,
            file_size_kb=0.0,
            status=IngestionStatus.FAILED,
            error=str(e),
        )


def process_pdf_folder(
    input_folder: Path,
    output_parquet: Path,
) -> dict:
    """
    Processes all PDFs in a folder and writes results to Parquet.

    Returns a summary dict:
    {
      total:   int  — PDFs found
      success: int  — successfully extracted
      failed:  int  — failed extraction
      output:  str  — path to written Parquet file
    }
    """
    pdf_files = list(input_folder.glob("*.pdf"))

    if not pdf_files:
        _log("warning", "folder.no_pdfs_found", folder=str(input_folder))
        return {"total": 0, "success": 0, "failed": 0, "output": None}

    _log("info", "batch.start", total=len(pdf_files), folder=str(input_folder))

    records = [build_ingestion_record(pdf) for pdf in pdf_files]

    # Convert to DataFrame — one row per record
    df = pd.DataFrame([r.model_dump() for r in records])

    # Parquet does not support Pydantic enums directly — convert to string
    df["status"] = df["status"].astype(str)
    df["ingested_at"] = df["ingested_at"].astype(str)

    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_parquet, index=False)

    success = sum(1 for r in records if r.status == IngestionStatus.PENDING)
    failed = sum(1 for r in records if r.status == IngestionStatus.FAILED)

    _log(
        "info",
        "batch.complete",
        total=len(records),
        success=success,
        failed=failed,
        output=str(output_parquet),
    )

    return {
        "total": len(records),
        "success": success,
        "failed": failed,
        "output": str(output_parquet),
    }


# ── Local runner ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    input_folder = Path("data/raw_pdfs")
    output_parquet = Path("data/parquet/raw_invoices.parquet")

    input_folder.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 50)
    print("  FinSight — Ingestion Processor")
    print("=" * 50)

    if not list(input_folder.glob("*.pdf")):
        print(f"\n  No PDFs found in {input_folder}/")
        print("  Add some invoice PDFs and run again.\n")
    else:
        summary = process_pdf_folder(input_folder, output_parquet)
        print(f"\n  Total processed : {summary['total']}")
        print(f"  Success         : {summary['success']}")
        print(f"  Failed          : {summary['failed']}")
        print(f"  Output          : {summary['output']}")

        if summary["output"]:
            df = pd.read_parquet(summary["output"])
            print(f"\n  Parquet preview ({len(df)} rows):")
            print(df[["file_name", "page_count", "file_size_kb", "status"]].to_string(index=False))
    print()
