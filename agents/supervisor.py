# agents/supervisor.py
#
# Defines the StateGraph for the FinSight Agent pipeline.
# Registers all nodes, edges, and conditional routing.
# Single place where the entire agent flow is assembled.
#
# Flow:
#   START -> extraction -> investigation -> routing -> END
#
# Run: uv run python run_pipeline.py

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import TypedDict

from dotenv import load_dotenv
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from agents.extraction_agent import run_extraction
from agents.investigation_agent import run_investigation
from tools.metadata_lookup import _get_blob_client

load_dotenv()
logger = logging.getLogger("finsight.supervisor")


# ── State ─────────────────────────────────────────────────────────────────────


class SupervisorState(TypedDict):
    # input — set once by run_pipeline.py, never modified
    run_id: str
    file_name: str
    raw_text: str

    # extraction node
    extraction: dict
    extraction_status: str
    extraction_error: str

    # investigation node
    findings: dict
    risk_score: float
    recommendation: str
    investigation_status: str
    investigation_report: str

    # routing nodes
    final_status: str
    routed_to: str


# ── Routing functions ─────────────────────────────────────────────────────────


def extraction_router(state: SupervisorState) -> str:
    """
    Reads extraction_status.
    Routes to investigation if successful, failed node otherwise.
    """
    if state.get("extraction_status") == "success":
        return "investigation_node"
    return "failed_node"


def supervisor_router(state: SupervisorState) -> str:
    """
    Reads recommendation from investigation agent.
    Routes to appropriate terminal node.
    """
    routes = {
        "approve": "approved_node",
        "review": "review_node",
        "reject": "failed_node",
        "escalate": "escalate_node",
    }
    recommendation = state.get("recommendation", "escalate")
    return routes.get(recommendation, "escalate_node")


# ── Terminal nodes ────────────────────────────────────────────────────────────


def approved_node(state: SupervisorState) -> dict:
    """
    Invoice approved.
    Writes record to processed/ container in Azure Blob.
    """
    _write_to_blob(state, container="processed")
    logger.info(f"[{state['run_id']}] APPROVED — {state['file_name']}")
    return {
        "final_status": "approved",
        "routed_to": "processed",
    }


def review_node(state: SupervisorState) -> dict:
    """
    Invoice needs human review.
    interrupt() pauses the graph and waits for human input.
    Graph resumes when human provides decision via run_pipeline.py
    This is LangGraph human-in-the-loop pattern.
    """
    logger.info(f"[{state['run_id']}] REVIEW — waiting for human input")

    # interrupt pauses graph here
    # human receives the investigation report
    # human returns "approve" or "reject"
    human_decision = interrupt(
        {
            "message": "Invoice requires human review",
            "report": state.get("investigation_report", ""),
            "score": state.get("risk_score", 0),
            "file": state.get("file_name", ""),
        }
    )

    # graph resumes here after human input
    if human_decision == "approve":
        _write_to_blob(state, container="processed")
        return {"final_status": "approved_after_review", "routed_to": "processed"}

    _write_to_blob(state, container="failed")
    return {"final_status": "rejected_after_review", "routed_to": "failed"}


def failed_node(state: SupervisorState) -> dict:
    """
    Invoice rejected.
    Writes record to failed/ container in Azure Blob.
    """
    _write_to_blob(state, container="failed")
    logger.info(f"[{state['run_id']}] REJECTED — {state['file_name']}")
    return {
        "final_status": "rejected",
        "routed_to": "failed",
    }


def escalate_node(state: SupervisorState) -> dict:
    """
    Invoice escalated — serious fraud risk.
    Writes record to failed/ container.
    Logs escalation for legal team notification.
    Month 3: replace logger with actual notification service.
    """
    _write_to_blob(state, container="failed")
    logger.warning(
        f"[{state['run_id']}] ESCALATED — {state['file_name']} "
        f"risk={state.get('risk_score')} "
        f"reasons={state.get('findings', {})}"
    )
    return {
        "final_status": "escalated",
        "routed_to": "escalate_queue",
    }


# ── Azure Blob writer ─────────────────────────────────────────────────────────


def _write_to_blob(state: SupervisorState, container: str) -> None:
    """
    Writes final invoice record to Azure Blob.
    This is the persistent memory for future threat detection.
    invoice_history tool reads from processed/ container.

    Record written as JSON to:
      processed/history/{vendor_id}/{run_id}.json
      failed/history/{vendor_id}/{run_id}.json
    """
    try:
        extraction = state.get("extraction") or {}
        vendor_id = _get_vendor_id(extraction)
        run_id = state.get("run_id", str(uuid.uuid4()))

        record = {
            "run_id": run_id,
            "file_name": state.get("file_name"),
            "vendor_id": vendor_id,
            "invoice_number": extraction.get("invoice_number"),
            "total_amount": extraction.get("total_amount"),
            "invoice_date": extraction.get("invoice_date"),
            "status": state.get("recommendation"),
            "risk_score": state.get("risk_score"),
            "flags": state.get("findings", {}).get("flags", []),
            "report": state.get("investigation_report", "")[:500],
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

        blob_path = f"history/{vendor_id}/{run_id}.json"
        client = _get_blob_client()
        container_client = client.get_container_client(container)
        container_client.get_blob_client(blob_path).upload_blob(
            json.dumps(record, indent=2),
            overwrite=True,
        )
        logger.info(f"Written to {container}/{blob_path}")

    except Exception as e:
        logger.error(f"Failed to write to blob: {e}")


def _get_vendor_id(extraction: dict) -> str:
    """
    Gets vendor_id from extraction for blob path.
    Falls back to "unknown" if not found.
    """
    from tools.metadata_lookup import get_vendor

    gstin = extraction.get("gstin")
    vendor = get_vendor(gstin) if gstin else None
    return vendor["vendor_id"] if vendor else "unknown"


# ── Graph assembly ────────────────────────────────────────────────────────────


def build_graph():
    """
    Assembles and compiles the StateGraph.
    Called once by run_pipeline.py.
    Returns compiled runnable graph.
    """
    graph = StateGraph(SupervisorState)

    # register nodes
    graph.add_node("extraction_node", run_extraction)
    graph.add_node("investigation_node", run_investigation)
    graph.add_node("approved_node", approved_node)
    graph.add_node("review_node", review_node)
    graph.add_node("failed_node", failed_node)
    graph.add_node("escalate_node", escalate_node)

    # entry point
    graph.add_edge(START, "extraction_node")

    # extraction router
    graph.add_conditional_edges(
        "extraction_node",
        extraction_router,
        {
            "investigation_node": "investigation_node",
            "failed_node": "failed_node",
        },
    )

    # investigation router if extraction was successful and called investigation node ,it goes to supervisor
    graph.add_conditional_edges(
        "investigation_node",
        supervisor_router,
        {
            "approved_node": "approved_node",
            "review_node": "review_node",
            "failed_node": "failed_node",
            "escalate_node": "escalate_node",
        },
    )

    # all terminal nodes go to END
    graph.add_edge("approved_node", END)
    graph.add_edge("review_node", END)
    graph.add_edge("failed_node", END)
    graph.add_edge("escalate_node", END)

    return graph.compile()


# ## What Each Section Does
# ```
# SupervisorState
#   TypedDict — every field typed and documented
#   single source of truth for the entire pipeline
#   every node reads from here, writes back here

# extraction_router
#   reads extraction_status
#   one conditional — success or failed
#   no logic, just routing

# supervisor_router
#   reads recommendation string
#   maps to node name
#   unknown recommendation -> escalate (safe default)

# approved/failed/escalate nodes
#   pure Python functions
#   write to Azure Blob
#   return final_status and routed_to

# review_node
#   interrupt() pauses graph
#   human reads report
#   returns approve or reject
#   graph resumes from where it stopped

# _write_to_blob
#   writes JSON record to Azure
#   path includes vendor_id for history lookup
#   invoice_history tool reads from here later

# build_graph()
#   single function that assembles everything
#   called once by run_pipeline.py
#   returns compiled graph ready to invoke
