# run_pipeline.py
#
# Single command entry point for the FinSight Agent pipeline.
# Reads Parquet, builds initial state, invokes compiled graph.
#
# Run: uv run python run_pipeline.py

import logging
import uuid

import pandas as pd

from agents.supervisor import build_graph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("finsight.pipeline")


def main():
    logger.info("Loading invoices from Parquet")
    df = pd.read_parquet("data/parquet/raw_invoices.parquet")
    row = df.iloc[0]

    initial_state = {
        "run_id": str(uuid.uuid4()),
        "file_name": row.get("file_name", "unknown"),
        "raw_text": row["raw_text"],
    }

    logger.info(f"Running pipeline for: {initial_state['file_name']}")

    graph = build_graph()
    result = graph.invoke(initial_state)

    print("\n" + "=" * 50)
    print("  FINSIGHT AGENT — RESULT")
    print("=" * 50)
    print(f"  File:           {result.get('file_name')}")
    print(f"  Vendor:         {result.get('extraction', {}).get('vendor_name')}")
    print(f"  Invoice:        {result.get('extraction', {}).get('invoice_number')}")
    print(f"  Risk Score:     {result.get('risk_score')}")
    print(f"  Recommendation: {result.get('recommendation', '').upper()}")
    print(f"  Final Status:   {result.get('final_status', '').upper()}")
    print(f"  Routed To:      {result.get('routed_to')}")
    print("=" * 50)
    print("\nInvestigation Report:")
    print(result.get("investigation_report", "")[:1000])


if __name__ == "__main__":
    main()
