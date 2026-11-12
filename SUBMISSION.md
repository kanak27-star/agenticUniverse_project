# Submission

## Candidate

- **Name:** Kanak Khaitan
- **Assignment chosen:** A (Data)
- **Time spent (honest estimate):** 8hours

## How to run

Run the following commands from the project root using Python 3.14 or a compatible Python version.

### 1. Create and activate a virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
python -m pip install -r requirements.txt
```

### 3. Generate the synthetic input data

```powershell
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
```

### 4. Run the pipeline

```powershell
python solution/run_pipeline.py
```

### 5. Run the tests

```powershell
python -m pytest solution/tests -v
```

### 6. Validate the generated reports

```powershell
python solution/validate_reports.py
```

The pipeline writes its SQLite database to `solution/pipeline.db` and generates the five report files in `reports/`. Generated data, reports, the local database, and encryption keys are excluded from version control.

## Design

The implementation is organized into separate components:

- **Lead ingestion (`lead_ingestion.py`):** Reads workflow-specific CSV files, normalizes headers and values, validates required fields, records rejected rows, and merges duplicate leads.
- **Event ingestion (`event_ingestion.py`):** Reads interaction events, validates and deduplicates event IDs, links events to leads, handles call-level disposition overrides, and identifies orphan and unknown-disposition events.
- **Security (`security.py`):** Encrypts sensitive lead fields and generates deterministic HMAC-based phone lookup hashes. Raw phone numbers are not used as database lookup keys.
- **Database (`database.py`):** Persists leads, events, processed-file records, rejections, and quality counters in SQLite.
- **Reporting (`reporting.py`):** Generates the five required CSV/JSON reports from the ingested data.
- **Runner (`run_pipeline.py`):** Coordinates ingestion and report generation.

### Database design

SQLite is used as the persistent storage layer.

The main tables are:

- `leads`: Stores workflow ID, phone lookup hash, original batch ID, encrypted/serialized lead fields, and timestamps. The workflow ID and phone hash form the lead identity.
- `events`: Stores event IDs, event metadata, timestamps, phone lookup hashes, payloads, and source information. Event ID is unique.
- `processed_files`: Tracks source files and checksums to prevent unchanged files from being ingested again.
- `rejections`: Stores rejected input row details and rejection reasons without including raw PII.
- `quality_counters` and `quality_groups`: Persist ingestion quality metrics.

The database keeps leads and events across executions. This supports repeatable ingestion and allows previously orphaned events to be linked when the corresponding lead becomes available.

## Decisions & assumptions

| Decision                                                 | Why                                                                                                     | Alternative considered                    |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ----------------------------------------- |
| SQLite for persistence                                   | Simple local relational storage with no separate database server required                               | PostgreSQL                                |
| Workflow ID plus HMAC phone hash as lead identity        | Supports matching without storing raw phone numbers as lookup keys                                      | Plain-text phone number                   |
| Encrypt sensitive lead fields                            | Protects PII stored at rest                                                                             | Store normalized PII directly             |
| Preserve the first batch ID for a lead                   | Keeps the original acquisition batch associated with the lead                                           | Replace batch ID on every duplicate       |
| Last non-null value wins when merging lead records       | Allows newer valid information to update existing lead fields without overwriting them with null values | Keep only the first record                |
| Event ID as the event deduplication key                  | Prevents the same interaction event from being stored more than once                                    | Deduplicate using timestamps and payloads |
| Interpret timestamps without timezone information as IST | Provides a consistent timezone for reporting when the source timestamp has no offset                    | Treat naive timestamps as UTC             |
| Track processed files using checksums                    | Prevents unchanged files from being counted again on subsequent runs                                    | Reprocess every file on every run         |

## Failure modes considered

- **Invalid lead rows:** Required-field and validation failures are recorded in the rejection report with reasons.
- **Duplicate leads:** Matching leads are merged while retaining the original batch ID.
- **Duplicate events:** Repeated event IDs are dropped.
- **Orphan events:** Events without a matching lead are identified. Previously stored events can be linked when the corresponding lead becomes available.
- **Unknown dispositions:** Events with dispositions not defined for the workflow are counted as data-quality issues.
- **Malformed events:** Invalid event records are excluded and counted.
- **Repeated execution:** Unchanged input files are tracked so that repeated pipeline runs do not ingest them again.
- **Sensitive information:** Lead PII is encrypted, phone matching uses HMAC hashes, and rejection reports avoid raw PII.

### Known limitations

- The current runner processes all available drop directories together; a dedicated command for selecting and ingesting one drop at a time is not yet implemented.
- Replacing an already processed input file with different contents at the same path is not fully handled as a source-file replacement.
- The implementation is designed for the supplied assignment dataset and local SQLite execution, not a distributed production deployment.
- Recovery from a process crash during a partially completed file ingestion could be improved through stronger transaction boundaries.

## Testing

The following checks were performed:

- Complete test suite: **19 tests passed**.
- Pipeline execution: successfully generated all five reports.
- Idempotency check: ran the pipeline twice and confirmed that the reported counts remained unchanged.
- Report validator: all nine validation checks passed.

The tests cover database persistence, lead ingestion, phone encryption, timestamp parsing, event deduplication, orphan-event relinking, and report generation.

## Results

Using the supplied synthetic dataset generated with seed `42` and small scale:

- Lead rows processed: **2,057**
- Lead rows rejected: **108**
- Unique leads loaded: **1,892**
- Duplicate lead rows merged: **57**
- Events processed: **8,745**
- Linked events: **8,020**
- Duplicate events dropped: **237**
- Orphan events: **462**
- Malformed events: **8**
- Unknown-disposition events: **18**

The pipeline generated:

1. `batch_summary.csv`
2. `daily_overview.csv`
3. `lead_report.csv`
4. `rejections.csv`
5. `data_quality.json`

All report validation checks passed.

## If I had another day

1. Add a dedicated drop-by-drop CLI workflow and test incremental ingestion against all-at-once ingestion.
2. Improve transaction handling and recovery for interrupted file ingestion.
3. Add tests for changed input files, additional malformed records, and more edge cases.
4. Benchmark the pipeline on the larger supplied dataset.
5. Improve operational logging and configuration for production use.
