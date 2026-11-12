
from solution.src.database import PipelineDatabase


def test_database_saves_and_retrieves_leads(tmp_path):
    database = PipelineDatabase(tmp_path / "test.db")

    lead = {
        "workflow_id": "motor_renewal",
        "phone_hash": "hashed_phone_123",
        "batch_id": "batch_001",
        "fields": {
            "phone": "encrypted_phone_value",
            "policy_number": "POL123",
        },
    }

    database.upsert_lead(lead)

    leads = database.get_leads()

    assert len(leads) == 1
    assert leads[0]["workflow_id"] == "motor_renewal"
    assert leads[0]["phone_hash"] == "hashed_phone_123"
    assert leads[0]["fields"]["policy_number"] == "POL123"


def test_database_does_not_replace_original_batch(tmp_path):
    database = PipelineDatabase(tmp_path / "test.db")

    first_lead = {
        "workflow_id": "motor_renewal",
        "phone_hash": "hashed_phone_123",
        "batch_id": "batch_001",
        "fields": {"policy_number": "POL123"},
    }

    duplicate_lead = {
        "workflow_id": "motor_renewal",
        "phone_hash": "hashed_phone_123",
        "batch_id": "batch_002",
        "fields": {"policy_number": "POL456"},
    }

    database.upsert_lead(first_lead)
    database.upsert_lead(duplicate_lead)

    leads = database.get_leads()

    assert len(leads) == 1
    assert leads[0]["batch_id"] == "batch_001"
    assert leads[0]["fields"]["policy_number"] == "POL456"


def test_database_deduplicates_events(tmp_path):
    database = PipelineDatabase(tmp_path / "test.db")

    event = {
        "event_id": "evt001",
        "event_type": "call.initiated",
        "workflow_id": "motor_renewal",
        "call_id": "call001",
        "occurred_at": "2026-10-01T10:00:00+05:30",
        "phone_hash": "hashed_phone_123",
        "payload": {},
        "source_file": "events_0001.jsonl",
        "source_line": 1,
    }

    assert database.insert_event(event) is True
    assert database.insert_event(event) is False

    events = database.get_events()

    assert len(events) == 1
    assert events[0]["event_id"] == "evt001"
    assert events[0]["phone_hash"] == "hashed_phone_123"
    assert "phone" not in events[0]


def test_database_tracks_processed_files(tmp_path):
    database = PipelineDatabase(tmp_path / "test.db")

    file_path = "data/drop_1/events/events_0001.jsonl"

    assert database.is_file_processed(file_path, "checksum1") is False

    database.mark_file_processed(file_path, "checksum1", "events")

    assert database.is_file_processed(file_path, "checksum1") is True
    assert database.is_file_processed(file_path, "checksum2") is False


def test_database_persists_quality_counters(tmp_path):
    database_path = tmp_path / "test.db"

    database = PipelineDatabase(database_path)
    database.increment_counter("lead_rows_total", 10)
    database.increment_group("rejections_by_reason", "invalid_phone", 2)

    reopened_database = PipelineDatabase(database_path)

    assert reopened_database.get_quality_counters() == {
        "lead_rows_total": 10
    }

    assert reopened_database.get_quality_groups(
        "rejections_by_reason"
    ) == {
        "invalid_phone": 2
    }
