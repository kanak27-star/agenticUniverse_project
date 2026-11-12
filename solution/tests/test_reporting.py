
import csv
import json
from datetime import datetime, timedelta, timezone

from solution.src.reporting import ReportingPipeline
from solution.src.security import SecurityManager


IST = timezone(timedelta(hours=5, minutes=30))


def create_lead(security, phone, reference, batch="batch_001"):
    return {
        "workflow_id": "motor_renewal",
        "batch_id": batch,
        "phone_hash": security.phone_lookup_hash(phone),
        "fields": {
            "phone": security.encrypt(phone),
            "customer_name": security.encrypt("Test Customer"),
            "policy_number": reference,
        },
    }


def create_event(event_id, event_type, call_id, lead, timestamp, code=None):
    payload = {}

    if code is not None:
        payload["disposition_code"] = code

    return {
        "event_id": event_id,
        "event_type": event_type,
        "workflow_id": lead["workflow_id"],
        "call_id": call_id,
        "occurred_at": datetime.fromisoformat(timestamp),
        "payload": payload,
        "lead_key": (
            lead["workflow_id"],
            lead["phone_hash"],
        ),
    }


def create_reporting_files(tmp_path):
    schemas_dir = tmp_path / "schemas"
    schemas_dir.mkdir()

    schema = {
        "workflow_id": "motor_renewal",
        "fields": [
            {
                "name": "customer_name",
                "type": "string",
                "pii": True,
            },
            {
                "name": "phone",
                "type": "phone",
                "pii": True,
            },
            {
                "name": "policy_number",
                "type": "string",
                "reference": True,
            },
        ],
    }

    (schemas_dir / "motor_renewal.json").write_text(
        json.dumps(schema),
        encoding="utf-8",
    )

    dispositions_path = schemas_dir / "dispositions.csv"
    dispositions_path.write_text(
        "workflow_id,code,label,is_terminal,is_success\n"
        "motor_renewal,renewed,Policy renewed,true,true\n"
        "motor_renewal,callback_requested,Callback requested,false,false\n",
        encoding="utf-8",
    )

    return schemas_dir, dispositions_path


def read_csv(path):
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def test_reporting_generates_all_five_reports(tmp_path):
    security = SecurityManager(tmp_path / "master.key")

    lead_success = create_lead(
        security,
        "+919876543210",
        "POL001",
    )
    lead_failed = create_lead(
        security,
        "+919876543211",
        "POL002",
    )
    lead_no_calls = create_lead(
        security,
        "+919876543212",
        "POL003",
    )
    lead_dialed = create_lead(
        security,
        "+919876543213",
        "POL004",
    )

    events = [
        create_event(
            "evt001",
            "call.initiated",
            "call001",
            lead_success,
            "2026-10-01T10:00:00+05:30",
        ),
        create_event(
            "evt002",
            "call.connected",
            "call001",
            lead_success,
            "2026-10-01T10:00:10+05:30",
        ),
        create_event(
            "evt003",
            "disposition.set",
            "call001",
            lead_success,
            "2026-10-01T10:05:00+05:30",
            "renewed",
        ),
        create_event(
            "evt004",
            "call.initiated",
            "call002",
            lead_failed,
            "2026-10-01T11:00:00+05:30",
        ),
        create_event(
            "evt005",
            "call.telephony_failed",
            "call002",
            lead_failed,
            "2026-10-01T11:00:05+05:30",
        ),
        create_event(
            "evt006",
            "call.initiated",
            "call004",
            lead_dialed,
            "2026-10-02T09:00:00+05:30",
        ),
    ]

    leads = [
        lead_success,
        lead_failed,
        lead_no_calls,
        lead_dialed,
    ]

    lead_quality = {
        "lead_rows_total": 5,
        "lead_rows_rejected": 1,
        "rejections_by_reason": {
            "invalid_phone": 1,
        },
        "duplicate_lead_rows_merged": 0,
        "field_warnings": {},
    }

    event_quality = {
        "events_total": 6,
        "malformed_events": 0,
        "duplicate_events_dropped": 0,
        "orphan_events": 0,
        "unknown_disposition_events": 0,
    }

    rejections = [
        {
            "workflow_id": "motor_renewal",
            "batch_id": "batch_001",
            "row_number": 5,
            "reason": "invalid_phone",
        }
    ]

    schemas_dir, dispositions_path = create_reporting_files(tmp_path)

    output_dir = tmp_path / "reports"

    pipeline = ReportingPipeline(
        leads=leads,
        events=events,
        lead_quality=lead_quality,
        event_quality=event_quality,
        rejections=rejections,
        security=security,
        schemas_dir=schemas_dir,
        dispositions_path=dispositions_path,
        output_dir=output_dir,
    )

    paths = pipeline.generate()

    assert len(paths) == 5

    for path in paths.values():
        assert path.exists()

    batch_rows = read_csv(paths["batch_summary"])

    assert len(batch_rows) == 1

    batch = batch_rows[0]

    assert batch["workflow_id"] == "motor_renewal"
    assert batch["batch_id"] == "batch_001"
    assert batch["total_leads"] == "4"
    assert batch["yet_to_reach_out"] == "1"
    assert batch["in_progress_telephony_failed"] == "1"
    assert batch["in_progress_dialed"] == "1"
    assert batch["completed"] == "1"

    daily_rows = read_csv(paths["daily_overview"])

    assert len(daily_rows) == 2

    first_day = daily_rows[0]

    assert first_day["date"] == "2026-10-01"
    assert first_day["total_calls"] == "2"
    assert first_day["connected_calls"] == "1"
    assert first_day["connectivity_rate"] == "0.5000"
    assert first_day["success_leads"] == "1"

    lead_rows = read_csv(paths["lead_report"])

    assert len(lead_rows) == 4

    success_row = next(
        row for row in lead_rows
        if row["external_ref"] == "POL001"
    )

    assert success_row["phone_masked"] == "+91******3210"
    assert success_row["bucket"] == "completed"
    assert success_row["total_calls"] == "1"
    assert success_row["connected_calls"] == "1"
    assert success_row["current_disposition"] == "renewed"
    assert success_row["ever_success"] == "true"
    assert success_row["last_call_at"] == "2026-10-01T10:00:00+05:30"

    lead_report_text = paths["lead_report"].read_text(
        encoding="utf-8"
    )

    assert "9876543210" not in lead_report_text

    rejection_rows = read_csv(paths["rejections"])

    assert rejection_rows == [
        {
            "workflow_id": "motor_renewal",
            "batch_id": "batch_001",
            "row_number": "5",
            "reason": "invalid_phone",
        }
    ]

    with paths["data_quality"].open(
        "r",
        encoding="utf-8",
    ) as file:
        quality = json.load(file)

    assert quality["lead_rows_total"] == 5
    assert quality["lead_rows_rejected"] == 1
    assert quality["events_total"] == 6
    assert quality["orphan_events"] == 0
