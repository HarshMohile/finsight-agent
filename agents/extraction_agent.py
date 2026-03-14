# agents/extraction_agent.py
#
# Extraction agent — Stage 1 of the investigation pipeline.
# Reads raw invoice text from state.
# Calls LLM with extraction prompt.
# Writes structured fields back to state.
#
# No tools. Single LLM call. Fast and deterministic in structure.
# LLM is justified here for unstructured invoice formats that
# rule-based parsers cannot handle reliably.

import json
import logging
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_groq import ChatGroq

logger = logging.getLogger("finsight.extraction_agent")


load_dotenv()

PROMPT_PATH = Path(__file__).parent / "prompts" / "extraction.txt"


def _load_prompt() -> str:
    """
    Loads extraction prompt from file at runtime.
    Prompt lives outside code — change prompt without touching agent.
    """
    return PROMPT_PATH.read_text(encoding="utf-8")


def _build_llm() -> ChatGroq:
    """
    Builds Groq LLM client.
    Single place to swap model or provider.
    Month 3: swap to Azure OpenAI by changing this function only.
    """
    return ChatGroq(
        model="llama-3.3-70b-versatile",
        temperature=0,  # deterministic extraction
        max_tokens=2048,
    )


def run_extraction(state: dict) -> dict:
    """
    Extraction agent entry point.
    Called by supervisor as first node in LangGraph.

    Reads:  state["raw_text"]
    Writes: state["extraction"] — structured invoice fields
            state["extraction_status"] — "success" | "failed"
            state["extraction_error"] — error message if failed

    Returns updated state slice.
    Supervisor merges this back into full state.
    """
    raw_text = state.get("raw_text", "")

    if not raw_text:
        logger.warning("raw_text is empty — cannot extract")
        return {
            "extraction": None,
            "extraction_status": "failed",
            "extraction_error": "raw_text is empty",
        }

    prompt_template = _load_prompt()
    prompt = prompt_template.replace("{raw_text}", raw_text)
    llm = _build_llm()

    try:
        messages = [
            SystemMessage(content="You are a financial document extraction specialist."),
            HumanMessage(content=prompt),
        ]

        response = llm.invoke(messages)
        raw_content = response.content.strip()

        # strip markdown fences if LLM adds them despite instructions
        if raw_content.startswith("```"):
            raw_content = raw_content.split("```")[1]
            if raw_content.startswith("json"):
                raw_content = raw_content[4:]
            raw_content = raw_content.strip()

        extraction = json.loads(raw_content)

        logger.info(
            f"Extraction successful: "
            f"vendor={extraction.get('vendor_name')} "
            f"invoice={extraction.get('invoice_number')}"
        )

        return {
            "extraction": extraction,
            "extraction_status": "success",
            "extraction_error": None,
        }

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse failed: {e}")
        return {
            "extraction": None,
            "extraction_status": "failed",
            "extraction_error": f"JSON parse error: {str(e)}",
        }

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return {
            "extraction": None,
            "extraction_status": "failed",
            "extraction_error": str(e),
        }


## What Each Block Does

# _load_prompt()
#   reads extraction.txt from disk at runtime
#   change prompt without redeploying code

# _build_llm()
#   temperature=0 means deterministic
#   extraction should not be creative
#   same invoice = same output every time

# run_extraction(state)
#   reads raw_text from LangGraph state
#   fills prompt template
#   calls LLM
#   strips markdown fences if LLM ignores instructions
#   parses JSON
#   writes back to state
#   never raises — always returns status

# import pandas as pd
# from agents.extraction_agent import run_extraction

# df = pd.read_parquet('data/parquet/raw_invoices.parquet')
# row = df.iloc[0]

# state = {'raw_text': row['raw_text']}
# result = run_extraction(state)

# print('Status:', result['extraction_status'])
# print('Vendor:', result['extraction'].get('vendor_name'))
# print('Invoice:', result['extraction'].get('invoice_number'))
# print('Total:', result['extraction'].get('total_amount'))
# "
