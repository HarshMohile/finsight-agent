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




START
  |
  v
extraction_agent
  reads raw_text from state
  returns extracted fields
  writes to state
  |
  v
investigation_agent (ReAct loop)
  reads extracted fields from state
  decides which tools to call
  calls tools, reads results
  calls risk_scorer last
  writes all findings to state
  |
  v
supervisor
  reads risk score + recommendation
  routes to one of four outcomes:
    approve  -> write to processed/
    review   -> write to review_queue/
    reject   -> write to failed/
    escalate -> write to failed/ + notify
  |
  v
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
