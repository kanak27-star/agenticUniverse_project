
from pathlib import Path

from solution.src.lead_ingestion import LeadIngestion
from solution.src.security import SecurityManager


def test_lead_ingestion_runs():
    project_root = Path(__file__).resolve().parents[2]

    ingestion = LeadIngestion(
        data_dir=project_root / "data",
        schema_dir=project_root / "assignment-a-data" / "schemas",
        security=SecurityManager(),
    )

    result = ingestion.ingest()

    assert len(result["leads"]) > 0
    assert result["quality"]["lead_rows_total"] > 0
    assert result["quality"]["lead_rows_rejected"] > 0


def test_lead_phone_is_encrypted():
    project_root = Path(__file__).resolve().parents[2]

    security = SecurityManager()

    ingestion = LeadIngestion(
        data_dir=project_root / "data",
        schema_dir=project_root / "assignment-a-data" / "schemas",
        security=security,
    )

    result = ingestion.ingest()

    lead = result["leads"][0]

    encrypted_phone = lead["fields"]["phone"]

    assert encrypted_phone is not None
    assert not encrypted_phone.startswith("+91")
    assert security.decrypt(encrypted_phone).startswith("+91")
