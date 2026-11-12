
import json
from datetime import datetime, timedelta, timezone

from solution.src.event_ingestion import EventIngestion, parse_timestamp
from solution.src.security import SecurityManager


IST = timezone(timedelta(hours=5, minutes=30))


def create_test_lead(security):
    phone = "+919876543210"

    return {
        "workflow_id": "motor_renewal",
        "batch_id": "batch_001",
        "phone_hash": security.phone_lookup_hash(phone),
        "fields": {
            "phone": security.encrypt(phone),
            "customer_name": security.encrypt("Test Customer"),
            "policy_number": "POL123",
        },
    }


def write_dispositions(path):
    path.write_text(
        "workflow_id,code,label,is_terminal,is_success\n"
        "motor_renewal,renewed,Policy renewed,true,true\n"
        "motor_renewal,callback_requested,Callback requested,false,false\n",
        encoding="utf-8",
    )


def write_events(data_dir, events):
    events_dir = data_dir / "drop_1" / "events"
    events_dir.mkdir(parents=True)

    lines = []

    for event in events:
        if event == "MALFORMED_JSON":
            lines.append("{invalid json")
        else:
            lines.append(json.dumps(event))

    event_file = events_dir / "events_0001.jsonl"
    event_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return event_file


def test_parse_timestamp_converts_utc_to_ist():
    result = parse_timestamp("2026-10-01T05:00:00Z")

    assert result == datetime(2026, 10, 1, 10, 30, tzinfo=IST)


def test_parse_timestamp_treats_naive_timestamp_as_ist():
    result = parse_timestamp("2026-10-01T09:15:55")

    assert result == datetime(2026, 10, 1, 9, 15, 55, tzinfo=IST)


def test_parse_timestamp_handles_invalid_values():
    assert parse_timestamp(None) is None
    assert parse_timestamp("") is None
    assert parse_timestamp("not-a-timestamp") is None


def test_event_ingestion_deduplicates_links_overrides_and_counts_quality(
    tmp_path,
):
    security = SecurityManager(tmp_path / "master.key")
    lead = create_test_lead(security)

    data_dir = tmp_path / "data"
    dispositions_path = tmp_path / "dispositions.csv"

    write_dispositions(dispositions_path)

    events = [
        # Override intentionally appears before the call event.
        {
            "event_id": "evt_override",
            "event_type": "disposition.override",
            "workflow_id": "motor_renewal",
            "call_id": "call_001",
            "occurred_at": "2026-10-01T12:00:00Z",
            "payload": {
                "disposition_code": "renewed",
                "overridden_by": "supervisor",
                "reason": "Correction",
            },
        },
        {
            "event_id": "evt_call",
            "event_type": "call.initiated",
            "workflow_id": "motor_renewal",
            "call_id": "call_001",
            "occurred_at": "2026-10-01T10:00:00Z",
            "phone": "+91 98765 43210",
            "payload": {},
        },
        {
            "event_id": "evt_disposition",
            "event_type": "disposition.set",
            "workflow_id": "motor_renewal",
            "call_id": "call_001",
            "occurred_at": "2026-10-01T11:00:00Z",
            "phone": "9876543210",
            "payload": {
                "disposition_code": "callback_requested",
            },
        },
        # Duplicate event ID: this record must be dropped.
        {
            "event_id": "evt_call",
            "event_type": "call.ended",
            "workflow_id": "motor_renewal",
            "call_id": "call_001",
            "occurred_at": "2026-10-01T10:30:00Z",
            "phone": "+919876543210",
            "payload": {},
        },
        "MALFORMED_JSON",
        # This phone does not belong to any ingested lead.
        {
            "event_id": "evt_orphan",
            "event_type": "call.ended",
            "workflow_id": "motor_renewal",
            "call_id": "call_999",
            "occurred_at": "2026-10-01T13:00:00Z",
            "phone": "+919999999999",
            "payload": {},
        },
        # Unknown disposition code: this event must be ignored.
        {
            "event_id": "evt_unknown",
            "event_type": "disposition.set",
            "workflow_id": "motor_renewal",
            "call_id": "call_001",
            "occurred_at": "2026-10-01T14:00:00Z",
            "phone": "+919876543210",
            "payload": {
                "disposition_code": "unknown_code",
            },
        },
    ]

    write_events(data_dir, events)

    ingestion = EventIngestion(
        data_dir=data_dir,
        leads=[lead],
        security=security,
        dispositions_path=dispositions_path,
    )

    result = ingestion.ingest()

    # Seven non-empty lines were read.
    assert result["events_total"] == 7

    # One malformed JSON line.
    assert result["malformed_events"] == 1

    # The repeated event ID was discarded.
    assert result["duplicate_events_dropped"] == 1

    # One event could not be linked to a lead.
    assert result["orphan_events"] == 1

    # One linked disposition used an unknown code.
    assert result["unknown_disposition_events"] == 1

    # Only the override, call initiation, and valid disposition remain.
    assert len(result["events"]) == 3

    event_ids = {event["event_id"] for event in result["events"]}

    assert event_ids == {
        "evt_override",
        "evt_call",
        "evt_disposition",
    }

    # The override was linked even though it appeared before the call event.
    override = next(
        event
        for event in result["events"]
        if event["event_id"] == "evt_override"
    )

    assert override["lead_key"] == (
        lead["workflow_id"],
        lead["phone_hash"],
    )

    # Event records do not store raw phone numbers.
    for event in result["events"]:
        assert "phone" not in event

    # Quality counters are also available inside the quality dictionary.
    assert result["quality"]["events_total"] == 7
    assert result["quality"]["orphan_events"] == 1
