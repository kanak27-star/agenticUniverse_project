
import json

from solution.src.database import PipelineDatabase
from solution.src.event_ingestion import EventIngestion
from solution.src.security import SecurityManager


def create_event_file(data_dir, drop_name, event):
    events_dir = data_dir / drop_name / "events"
    events_dir.mkdir(parents=True, exist_ok=True)

    event_file = events_dir / "events.jsonl"
    event_file.write_text(
        json.dumps(event) + "\n",
        encoding="utf-8",
    )

    return event_file


def test_event_persists_across_ingestion_runs(tmp_path):
    data_dir = tmp_path / "data"
    database_path = tmp_path / "pipeline.db"

    security = SecurityManager()
    phone = "+919876543210"

    lead = {
        "workflow_id": "motor_renewal",
        "phone_hash": security.phone_lookup_hash(phone),
        "batch_id": "batch_001",
        "fields": {
            "phone": security.encrypt(phone),
        },
    }

    event = {
        "event_id": "event_001",
        "event_type": "call.initiated",
        "workflow_id": "motor_renewal",
        "call_id": "call_001",
        "occurred_at": "2026-10-01T10:00:00+05:30",
        "phone": phone,
        "payload": {},
    }

    create_event_file(data_dir, "drop_1", event)

    database = PipelineDatabase(database_path)

    first_result = EventIngestion(
        data_dir=data_dir,
        leads=[lead],
        security=security,
        database=database,
    ).ingest()

    assert len(first_result["events"]) == 1
    assert first_result["events"][0]["event_id"] == "event_001"

    reopened_database = PipelineDatabase(database_path)

    second_result = EventIngestion(
        data_dir=data_dir,
        leads=[lead],
        security=security,
        database=reopened_database,
    ).ingest()

    assert len(second_result["events"]) == 1
    assert second_result["events"][0]["event_id"] == "event_001"
    assert second_result["events_total"] == 1
    assert second_result["duplicate_events_dropped"] == 0


def test_previously_orphaned_event_links_when_lead_arrives(tmp_path):
    data_dir = tmp_path / "data"
    database = PipelineDatabase(tmp_path / "pipeline.db")

    security = SecurityManager()
    phone = "+919876543210"

    event = {
        "event_id": "orphan_event_001",
        "event_type": "call.initiated",
        "workflow_id": "motor_renewal",
        "call_id": "call_001",
        "occurred_at": "2026-10-01T10:00:00+05:30",
        "phone": phone,
        "payload": {},
    }

    create_event_file(data_dir, "drop_1", event)

    first_result = EventIngestion(
        data_dir=data_dir,
        leads=[],
        security=security,
        database=database,
    ).ingest()

    assert len(first_result["events"]) == 0
    assert first_result["orphan_events"] == 1

    lead = {
        "workflow_id": "motor_renewal",
        "phone_hash": security.phone_lookup_hash(phone),
        "batch_id": "batch_002",
        "fields": {
            "phone": security.encrypt(phone),
        },
    }

    second_result = EventIngestion(
        data_dir=data_dir,
        leads=[lead],
        security=security,
        database=database,
    ).ingest()

    assert len(second_result["events"]) == 1
    assert second_result["events"][0]["event_id"] == "orphan_event_001"
    assert second_result["orphan_events"] == 0


def test_duplicate_event_across_different_drops_is_counted(tmp_path):
    data_dir = tmp_path / "data"
    database = PipelineDatabase(tmp_path / "pipeline.db")

    security = SecurityManager()
    phone = "+919876543210"

    lead = {
        "workflow_id": "motor_renewal",
        "phone_hash": security.phone_lookup_hash(phone),
        "batch_id": "batch_001",
        "fields": {
            "phone": security.encrypt(phone),
        },
    }

    event = {
        "event_id": "duplicate_event_001",
        "event_type": "call.initiated",
        "workflow_id": "motor_renewal",
        "call_id": "call_001",
        "occurred_at": "2026-10-01T10:00:00+05:30",
        "phone": phone,
        "payload": {},
    }

    create_event_file(data_dir, "drop_1", event)
    create_event_file(data_dir, "drop_2", event)

    result = EventIngestion(
        data_dir=data_dir,
        leads=[lead],
        security=security,
        database=database,
    ).ingest()

    assert len(result["events"]) == 1
    assert result["events_total"] == 2
    assert result["duplicate_events_dropped"] == 1
