
import argparse
from pathlib import Path

from src.database import PipelineDatabase
from src.event_ingestion import EventIngestion
from src.lead_ingestion import LeadIngestion
from src.reporting import ReportingPipeline
from src.security import SecurityManager


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_pipeline(
    data_dir=None,
    database_path=None,
    output_dir=None,
):
    """Run persistent lead ingestion, event ingestion and reporting."""

    data_dir = Path(data_dir) if data_dir else PROJECT_ROOT / "data"
    database_path = (
        Path(database_path)
        if database_path
        else PROJECT_ROOT / "solution" / "pipeline.db"
    )
    output_dir = (
        Path(output_dir)
        if output_dir
        else PROJECT_ROOT / "reports"
    )

    schemas_dir = PROJECT_ROOT / "assignment-a-data" / "schemas"
    dispositions_path = schemas_dir / "dispositions.csv"

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    if not schemas_dir.exists():
        raise FileNotFoundError(f"Schema directory not found: {schemas_dir}")

    if not dispositions_path.exists():
        raise FileNotFoundError(
            f"Dispositions file not found: {dispositions_path}"
        )

    print("Initializing database and security...")
    database = PipelineDatabase(database_path=database_path)
    security = SecurityManager()

    print("\nIngesting leads...")
    lead_ingestion = LeadIngestion(
        data_dir=data_dir,
        schema_dir=schemas_dir,
        security=security,
        database=database,
    )
    lead_result = lead_ingestion.ingest()

    print(f"Leads loaded: {len(lead_result['leads'])}")
    print(f"Lead rows processed: {lead_result['lead_rows_total']}")
    print(f"Lead rows rejected: {lead_result['lead_rows_rejected']}")

    print("\nIngesting events...")
    event_ingestion = EventIngestion(
        data_dir=data_dir,
        leads=lead_result,
        security=security,
        dispositions_path=dispositions_path,
        database=database,
    )
    event_result = event_ingestion.ingest()

    print(f"Linked events: {len(event_result['events'])}")
    print(f"Events processed: {event_result['events_total']}")
    print(f"Duplicate events dropped: {event_result['duplicate_events_dropped']}")
    print(f"Orphan events: {event_result['orphan_events']}")

    print("\nGenerating reports...")
    reporter = ReportingPipeline(
        leads=lead_result,
        events=event_result,
        lead_quality=lead_result["quality"],
        event_quality=event_result["quality"],
        rejections=lead_result["rejections"],
        security=security,
        schemas_dir=schemas_dir,
        dispositions_path=dispositions_path,
        output_dir=output_dir,
    )

    report_paths = reporter.generate()

    print("\nAll reports generated successfully:")
    for report_name, report_path in report_paths.items():
        print(f"  {report_name}: {report_path}")

    print(f"\nDatabase: {database_path}")
    print(f"Reports directory: {output_dir}")

    return report_paths


def main():
    parser = argparse.ArgumentParser(
        description="Run the Agentic Universe interaction ledger pipeline."
    )

    parser.add_argument(
        "--data-dir",
        default=None,
        help="Directory containing drop_* folders (default: ./data).",
    )
    parser.add_argument(
        "--database",
        default=None,
        help="SQLite database path (default: ./solution/pipeline.db).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Report output directory (default: ./reports).",
    )

    args = parser.parse_args()

    run_pipeline(
        data_dir=args.data_dir,
        database_path=args.database,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
