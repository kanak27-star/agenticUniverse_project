# AI Usage Disclosure

## Tools used

- ChatGPT (OpenAI).

## What I used them for

- Understanding the assignment requirements and breaking the implementation into manageable steps.
- Discussing the lead ingestion, event ingestion, persistence, reporting, and data-security design.
- Drafting and refining Python implementation code for the ingestion pipeline, SQLite database layer, reporting, and command-line runner.
- Debugging implementation issues and understanding test failures.
- Reviewing report validation results and checking idempotent execution.

## What I wrote or decided myself

- I reviewed the assignment requirements and decided to implement Assignment A.
- I worked through the implementation and integration process, ran the commands locally, and reviewed the generated output.
- I chose SQLite for persistent storage and used separate database tables for leads, events, processed files, rejections, and data-quality counters.
- I reviewed the handling of duplicate leads, duplicate events, orphan events, disposition overrides, and report generation.
- I executed the test suite and report validator and checked their results before preparing the submission.

## Where the AI was wrong or unhelpful

- An initial pipeline-runner approach processed all available drop directories in one execution. This did not by itself demonstrate the separate drop-by-drop execution workflow described in the assignment. I identified this limitation while reviewing the assignment requirements. The current runner processes all available drops together; separate drop selection remains an improvement to implement.
- I also reviewed generated report values against the report validation requirements instead of assuming that generated files were correct simply because the program completed.

## How I verified AI-generated code

- Ran the complete test suite using `python -m pytest solution/tests -v`.
- All 19 tests passed.
- Ran `python solution/run_pipeline.py` and confirmed that all five reports were generated.
- Ran the pipeline a second time and checked that the reported counts remained unchanged.
- Ran `python solution/validate_reports.py`.
- All nine report validation checks passed.
- Reviewed the terminal output and generated report files locally.
