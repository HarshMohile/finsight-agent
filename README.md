# finsight-agent

So welcome to Agentic Project for learing purposes and development .
Below are the  key skills

Tech: uv · LangGraph · Groq · LangSmith · FastAPI · pre-commit · ruff





 <----------------------data_ingestion---------------------------->
extract_text_from_pdf(file_path)
Opens a PDF with pdfplumber. Reads each page. Returns one string with all the text. Handles the case where a page has no text without crashing.

build_ingestion_record(file_path, raw_text)
Takes the file path and extracted text. Returns a typed Pydantic object — the IngestionRecord. This is what eventually becomes a Parquet row.

process_pdf_folder(folder_path)
Loops over every PDF in a folder. Calls the two functions above for each file. Returns a list of IngestionRecord objects. Writes them to Parquet. Skips files that fail without stopping the whole batch.



USER RUNS
uv run python -m data_ingestion.ingestion_processor
        |
        v
if __name__ == "__main__"
  sets input_folder  = data/raw_pdfs/
  sets output_parquet = data/parquet/raw_invoices.parquet
        |
        v
process_pdf_folder(input_folder, output_parquet)
  |
  | glob("*.pdf") — finds all PDFs in folder
  |
  | if no PDFs found — logs warning, returns early
  |
  | for each PDF file found
  |       |
  |       v
  |   build_ingestion_record(file_path)
  |     |
  |     | try
  |     |     |
  |     |     v
  |     | extract_text_from_pdf(file_path)
  |     |   opens PDF with pdfplumber
  |     |   loops each page
  |     |   page.extract_text()
  |     |   joins pages with "\n\n"
  |     |   returns (raw_text, page_count)
  |     |     |
  |     |     v
  |     | builds IngestionRecord(
  |     |   file_name, file_path,
  |     |   raw_text, page_count,
  |     |   file_size_kb,
  |     |   status = PENDING if text found
  |     |            FAILED  if no text
  |     | )
  |     |
  |     | except — any crash
  |     |   returns IngestionRecord(
  |     |     status = FAILED
  |     |     error  = exception message
  |     |   )
  |     |
  |     v
  |   returns IngestionRecord
  |
  | collects all records into a list
  |
  v
converts list to DataFrame
  pd.DataFrame([r.model_dump() for r in records])
  casts status and ingested_at to string (Parquet limitation)
        |
        v
df.to_parquet(output_parquet)
writes file to data/parquet/raw_invoices.parquet
        |
        v
returns summary dict
  { total, success, failed, output }
        |
        v
__main__ prints summary
prints DataFrame preview









<------------ Full flow of the LLM guardrailing the invoices ---------------------->




raw_text enters supervisor
        |
        v
extraction_agent
  LLM extracts structured data
  writes extraction_output
  writes extraction_status
        |
        v
supervisor_route() called
  extraction_status == "failed"? ──────────────────> route_to_failed
  else                                                      |
        |                                                   v
        v                                          log to LangSmith
validation_agent                                   email SME
  math_checker_tool                                 write to failed queue
  date_checker_tool                                END
  duplicate_checker_tool
  writes validation_output
  writes math_check
  writes date_check
  writes duplicate_check
  writes validation_status
        |
        v
supervisor_route() called
  validation_status == "fail"
  AND math_check == "fail"? ───────────────────────> route_to_failed
  else
        |
        v
hallucination_guard
  field_verifier_tool on every field
  writes hallucination_report
  writes hallucinated_fields
  writes guard_status
        |
        v
supervisor_route() called
  computes overall_confidence
  applies penalties per issue
        |
        |── overall_confidence >= 0.85
        |   AND no reasons ──────────────────────> route_to_approved
        |                                          clean_output_for_client()
        |                                          write processed Parquet
        |                                          END
        |
        |── overall_confidence >= 0.65 ──────────> route_to_review
        |                                          ReviewQueueItem
        |                                          write to review JSON
        |                                          END
        |
        └── overall_confidence < 0.65 ───────────> route_to_failed
                                                   log to LangSmith
                                                   email SME
                                                   END





##### why metadata_loopkup is needeed ?

Invoice arrives with:
  vendor_name: "Oroboros Solutions Pvt Ltd"
  gstin:       "29AABCT1332L1ZU"
  total:       2,36,000 INR
  rate:        1,500/hr for data engineering
        |
        v
metadata_lookup.get_vendor("29AABCT1332L1ZU")
  connects to Azure Blob
  downloads vendors.json
  searches for matching GSTIN
  returns vendor record
        |
        v
metadata_lookup.get_contract("VND-001")
  downloads contracts.json
  finds SOW-2024-003
  returns contract with agreed_rates
        |
        v
verification_agent now knows:
  contracted rate for data_engineering = 1,200
  invoice charged                      = 1,500
  difference                           = 300/hr * 80hrs = 24,000 overbilling
