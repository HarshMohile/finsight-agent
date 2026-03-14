# agents/investigation_agent.py
#
# Investigation agent — Stage 2 of the pipeline.
# ReAct loop — LLM reasons and decides which tools to call.
#
# Pattern: ReAct (Reason + Act)
#   LLM reads extraction output
#   decides which tools to call based on what it finds
#   reads tool result
#   decides next action
#   ends with structured JSON block for clean parsing
#
# Called by: supervisor as second node in LangGraph
# Reads:  state["extraction"], state["raw_text"]
# Writes: state["findings"], state["investigation_status"]
#         state["risk_score"], state["recommendation"]

import json
import logging
import re
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage
from langchain_groq import ChatGroq
from langgraph.prebuilt import create_react_agent

from tools.date_checker import as_langchain_tool as date_tool
from tools.field_verifier import as_langchain_tool as field_tool
from tools.invoice_history import as_langchain_tool as history_tool
from tools.math_checker import as_langchain_tool as math_tool
from tools.metadata_lookup import (
    as_langchain_rate_tool as rate_tool,
)
from tools.metadata_lookup import (
    as_langchain_tool as metadata_tool,
)
from tools.risk_scorer import as_langchain_tool as risk_tool

load_dotenv()
logger = logging.getLogger("finsight.investigation_agent")
PROMPT_PATH = Path(__file__).parent / "prompts" / "investigation.txt"


def _load_prompt() -> str:
    return PROMPT_PATH.read_text(encoding="utf-8")


def _build_tools() -> list:
    """
    Assembles all investigation tools.
    Add new tools here — agent picks them up automatically.
    """
    return [
        metadata_tool(),
        math_tool(),
        date_tool(),
        field_tool(),
        rate_tool(),
        history_tool(),
        risk_tool(),
    ]


def _build_agent():
    """
    Builds ReAct agent with Groq LLM and all investigation tools.
    temperature=0 — deterministic reasoning.
    """
    llm = ChatGroq(
        model="moonshotai/kimi-k2-instruct",
        temperature=0,
        max_tokens=2048,
    )
    return create_react_agent(llm, _build_tools())


def _normalise_recommendation(raw: str) -> str:
    """
    Normalises LLM recommendation to one of four valid values.
    LLM sometimes returns "manual_review", "flag", "needs_review" etc.
    We own the routing decision — not the LLM.
    """
    raw = raw.lower().strip()

    if raw in ("approve", "approved", "pass"):
        return "approve"

    if raw in ("review", "manual_review", "needs_review", "human_review", "flag", "flagged"):
        return "review"

    if raw in ("reject", "rejected", "deny", "denied", "fail"):
        return "reject"

    if raw in ("escalate", "escalated", "urgent", "critical"):
        return "escalate"

    # unknown value — fail safe
    logger.warning(f"Unknown recommendation '{raw}' — defaulting to escalate")
    return "escalate"


def _parse_agent_output(final_message: str) -> dict:
    """
    Parses structured JSON block from end of agent final message.
    Handles both fenced and unfenced JSON.
    Falls back to safe defaults if parsing fails.
    """
    # try fenced json block first
    match = re.search(
        r"```json\s*(\{.*?\})\s*```",
        final_message,
        re.DOTALL,
    )

    # fallback — try last JSON object in message
    if not match:
        match = re.search(
            r'(\{[^{}]*"risk_score"[^{}]*"recommendation"[^{}]*\})',
            final_message,
            re.DOTALL,
        )

    if match:
        try:
            parsed = json.loads(match.group(1))
            return {
                "risk_score": float(parsed.get("risk_score", 1.0)),
                "recommendation": _normalise_recommendation(
                    parsed.get("recommendation", "escalate")
                ),
                "findings": {"reasons": parsed.get("reasons", [])},
            }
        except json.JSONDecodeError:
            pass

    # last resort — keyword match in full message
    for word in ["approve", "review", "reject", "escalate"]:
        if word in final_message.lower():
            logger.warning(f"Falling back to keyword match: {word}")
            return {
                "risk_score": 0.2 if word == "approve" else 0.7,
                "recommendation": _normalise_recommendation(word),
                "findings": {},
            }

    logger.warning("Could not parse structured output — using safe defaults")
    return {
        "risk_score": 1.0,
        "recommendation": "escalate",
        "findings": {},
    }


def run_investigation(state: dict) -> dict:
    """
    Investigation agent entry point.
    Called by supervisor as second node in LangGraph.

    Reads:
      state["extraction"]  — structured fields from extraction agent
      state["raw_text"]    — original PDF text for field verification

    Writes:
      state["findings"]              — reasons from risk scorer
      state["risk_score"]            — 0.0 to 1.0
      state["recommendation"]        — approve/review/reject/escalate
      state["investigation_status"]  — "success" | "failed"
      state["investigation_report"]  — full LLM reasoning narrative
    """
    extraction = state.get("extraction")
    raw_text = state.get("raw_text", "")

    if not extraction:
        logger.warning("No extraction in state — skipping investigation")
        return {
            "findings": {},
            "risk_score": 1.0,
            "recommendation": "escalate",
            "investigation_status": "failed",
            "investigation_report": "Extraction missing — cannot investigate",
        }

    prompt_template = _load_prompt()
    prompt = prompt_template.replace("{extraction}", json.dumps(extraction, indent=2)).replace(
        "{raw_text}", raw_text[:1500]
    )

    try:
        agent = _build_agent()
        messages = [HumanMessage(content=prompt)]
        result = agent.invoke({"messages": messages})

        final_message = result["messages"][-1].content  # fetch last message (its always AI)
        logger.info(f"Final message: {final_message}")
        logger.info(f"Investigation complete. " f"Report length: {len(final_message)} chars")

        parsed = _parse_agent_output(final_message)

        return {
            "findings": parsed["findings"],
            "risk_score": parsed["risk_score"],
            "recommendation": parsed["recommendation"],
            "investigation_status": "success",
            "investigation_report": final_message,
        }

    except Exception as e:
        logger.error(f"Investigation failed: {e}")
        return {
            "findings": {},
            "risk_score": 1.0,
            "recommendation": "escalate",
            "investigation_status": "failed",
            "investigation_report": f"Investigation error: {str(e)}",
        }
