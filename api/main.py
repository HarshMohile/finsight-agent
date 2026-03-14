# api/main.py
#
# FastAPI wrapper around the FinSight Agent LangGraph pipeline.
# Async processing — returns run_id immediately, pipeline runs in background.
# Client polls /status/{run_id} then /result/{run_id} when complete.
#
# Endpoints:
#   POST /invoke          start pipeline with PDF upload or blob path
#   GET  /status/{run_id} check processing status
#   GET  /result/{run_id} get full investigation result
#   GET  /health          health check for Container Apps

import logging
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import pdfplumber
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

from agents.supervisor import build_graph

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("finsight.api")

# ── In memory store ───────────────────────────────────────────────────────────
# Stores pipeline results keyed by run_id.
# Month 3: replace with Azure Blob or Redis.
# Interface stays the same — swap backend only.

run_store: dict = {}

# ── Graph — built once at startup ─────────────────────────────────────────────
graph = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Builds LangGraph pipeline once at startup.
    Reused across all requests — no rebuild overhead per request.
    """
    global graph
    logger.info("Building LangGraph pipeline...")
    graph = build_graph()
    logger.info("Pipeline ready.")
    yield
    logger.info("Shutting down.")


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="FinSight Agent",
    description="Multi-agent financial document intelligence pipeline.",
    version="0.1.0",
    lifespan=lifespan,
)


# ── Background task ───────────────────────────────────────────────────────────


def _run_pipeline(run_id: str, file_name: str, raw_text: str) -> None:
    """
    Runs LangGraph pipeline in background.
    Called by FastAPI BackgroundTasks — non blocking.
    Stores result in run_store when complete.
    """
    logger.info(f"[{run_id}] Pipeline starting for {file_name}")

    run_store[run_id]["status"] = "processing"

    try:
        initial_state = {
            "run_id": run_id,
            "file_name": file_name,
            "raw_text": raw_text,
        }

        result = graph.invoke(initial_state)

        run_store[run_id] = {
            "status": "completed",
            "run_id": run_id,
            "file_name": result.get("file_name"),
            "vendor": result.get("extraction", {}).get("vendor_name"),
            "invoice": result.get("extraction", {}).get("invoice_number"),
            "total": result.get("extraction", {}).get("total_amount"),
            "risk_score": result.get("risk_score"),
            "recommendation": result.get("recommendation", "").upper(),
            "final_status": result.get("final_status", "").upper(),
            "routed_to": result.get("routed_to"),
            "report": result.get("investigation_report", "")[:1000],
        }

        logger.info(
            f"[{run_id}] Completed — "
            f"recommendation={run_store[run_id]['recommendation']} "
            f"risk={run_store[run_id]['risk_score']}"
        )

    except Exception as e:
        logger.error(f"[{run_id}] Pipeline failed: {e}")
        run_store[run_id] = {
            "status": "failed",
            "run_id": run_id,
            "error": str(e),
        }


# ── Endpoints ─────────────────────────────────────────────────────────────────


@app.get("/health")
def health():
    """
    Health check endpoint.
    Used by Azure Container Apps readiness probe.
    Used by GitHub Actions post-deploy smoke test.
    """
    return {"status": "ok", "pipeline": "ready" if graph else "not ready"}


@app.post("/invoke")
async def invoke(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(default=None),
    blob_path: str = Form(default=None),
):
    """
    Starts the investigation pipeline.

    Accepts either:
      file      — PDF uploaded directly
      blob_path — path to PDF already in Azure Blob
                  e.g. "raw-invoices/2024/11/invoice.pdf"

    Returns run_id immediately.
    Pipeline runs in background.
    Poll /status/{run_id} to check progress.
    """
    if not file and not blob_path:
        raise HTTPException(
            status_code=400,
            detail="Provide either a PDF file or a blob_path.",
        )

    run_id = str(uuid.uuid4())
    run_store[run_id] = {"status": "queued", "run_id": run_id}

    # extract raw_text from PDF
    try:
        if file:
            raw_text, file_name = await _extract_from_upload(file)
        else:
            raw_text, file_name = _extract_from_blob(blob_path)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"PDF extraction failed: {e}")

    # start pipeline in background
    background_tasks.add_task(_run_pipeline, run_id, file_name, raw_text)

    return JSONResponse(
        status_code=202,
        content={
            "run_id": run_id,
            "status": "queued",
            "message": "Pipeline started. Poll /status/{run_id} for updates.",
        },
    )


@app.get("/status/{run_id}")
def status(run_id: str):
    """
    Returns current status of a pipeline run.
    Possible values: queued / processing / completed / failed
    """
    if run_id not in run_store:
        raise HTTPException(status_code=404, detail="run_id not found.")

    return {
        "run_id": run_id,
        "status": run_store[run_id]["status"],
    }


@app.get("/result/{run_id}")
def result(run_id: str):
    """
    Returns full investigation result.
    Call this after /status returns completed.
    """
    if run_id not in run_store:
        raise HTTPException(status_code=404, detail="run_id not found.")

    record = run_store[run_id]

    if record["status"] == "processing":
        return JSONResponse(
            status_code=202,
            content={"run_id": run_id, "status": "processing"},
        )

    if record["status"] == "failed":
        raise HTTPException(
            status_code=500,
            detail=record.get("error", "Pipeline failed."),
        )

    return record


# ── PDF extraction helpers ────────────────────────────────────────────────────


async def _extract_from_upload(file: UploadFile) -> tuple[str, str]:
    """
    Extracts raw text from uploaded PDF file.
    Saves to temp file, extracts with pdfplumber, cleans up.
    """
    contents = await file.read()

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = Path(tmp.name)

    try:
        raw_text = _pdf_to_text(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    return raw_text, file.filename or "uploaded.pdf"


def _extract_from_blob(blob_path: str) -> tuple[str, str]:
    """
    Downloads PDF from Azure Blob and extracts raw text.
    blob_path format: "raw-invoices/2024/11/invoice.pdf"
    Container is first segment, rest is blob name.
    """
    from tools.metadata_lookup import _get_blob_client

    parts = blob_path.split("/", 1)
    container_name = parts[0]
    blob_name = parts[1] if len(parts) > 1 else blob_path

    client = _get_blob_client()
    blob = client.get_container_client(container_name).get_blob_client(blob_name)
    pdf_bytes = blob.download_blob().readall()

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = Path(tmp.name)

    try:
        raw_text = _pdf_to_text(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    file_name = blob_path.split("/")[-1]
    return raw_text, file_name


def _pdf_to_text(path: Path) -> str:
    """
    Extracts all text from PDF using pdfplumber.
    Same logic as ingestion_processor.py — single source of truth.
    """
    text = ""
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text.strip()


# ## What Each Section Does
# ```
# lifespan()
#   builds LangGraph graph once at startup
#   reused across all requests
#   no rebuild overhead per request

# run_store {}
#   in memory dict
#   keyed by run_id
#   stores status and result
#    I will swap to Azure Blob

# _run_pipeline()
#   background task
#   runs graph.invoke()
#   writes result to run_store
#   never raises — stores error in run_store

# POST /invoke
#   accepts PDF upload or blob path
#   extracts raw_text
#   starts _run_pipeline as background task
#   returns 202 Accepted + run_id immediately

# GET /status/{run_id}
#   returns queued/processing/completed/failed

# GET /result/{run_id}
#   returns full result when completed
#   202 if still processing
#   500 if failed

# GET /health
#   Container Apps readiness probe
#   GitHub Actions smoke test
