
import csv
import json
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from .event_ingestion import parse_timestamp


class ReportingPipeline:
    """Generate the five required CSV and JSON reports."""

    def __init__(
        self,
        leads,
        events,
        lead_quality=None,
        event_quality=None,
        rejections=None,
        security=None,
        schemas_dir=None,
        dispositions_path=None,
        output_dir=None,
    ):
        self.project_root = Path(__file__).resolve().parents[2]

        self.leads = self._extract_list(leads, "leads")
        self.events = self._extract_list(events, "events")

        self.lead_quality = self._extract_quality(lead_quality, leads)
        self.event_quality = self._extract_quality(event_quality, events)

        self.rejections = self._extract_list(rejections, "rejections")
        self.security = security

        self.schemas_dir = (
            Path(schemas_dir)
            if schemas_dir is not None
            else self.project_root / "assignment-a-data" / "schemas"
        )

        self.dispositions_path = (
            Path(dispositions_path)
            if dispositions_path is not None
            else self.schemas_dir / "dispositions.csv"
        )

        self.output_dir = (
            Path(output_dir)
            if output_dir is not None
            else self.project_root / "reports"
        )

        self.schemas = self._load_schemas()
        self.dispositions = self._load_dispositions()

    @staticmethod
    def _extract_list(value, key):
        if value is None:
            return []

        if isinstance(value, dict):
            return list(value.get(key, []))

        return list(value)

    @staticmethod
    def _extract_quality(explicit_quality, source):
        if explicit_quality is not None:
            if isinstance(explicit_quality, dict) and "quality" in explicit_quality:
                return dict(explicit_quality["quality"])
            return dict(explicit_quality)

        if isinstance(source, dict):
            return dict(source.get("quality", {}))

        return {}

    def _load_schemas(self):
        schemas = {}

        if not self.schemas_dir.exists():
            raise FileNotFoundError(
                f"Schema directory not found: {self.schemas_dir}"
            )

        for schema_file in sorted(self.schemas_dir.glob("*.json")):
            with schema_file.open("r", encoding="utf-8") as file:
                schema = json.load(file)

            workflow_id = schema.get("workflow_id", schema_file.stem)
            raw_fields = schema.get("fields", [])

            if isinstance(raw_fields, list):
                fields = {
                    item["name"]: item
                    for item in raw_fields
                    if item.get("name")
                }
            elif isinstance(raw_fields, dict):
                fields = raw_fields
            else:
                raise ValueError(
                    f"Invalid fields format in schema: {schema_file}"
                )

            schemas[workflow_id] = fields

        return schemas

    def _load_dispositions(self):
        if not self.dispositions_path.exists():
            raise FileNotFoundError(
                f"Dispositions file not found: {self.dispositions_path}"
            )

        dispositions = {}

        with self.dispositions_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                workflow_id = row["workflow_id"].strip()
                code = row["code"].strip()

                dispositions.setdefault(workflow_id, {})[code] = {
                    "label": row.get("label", "").strip(),
                    "is_terminal": (
                        row.get("is_terminal", "").strip().lower() == "true"
                    ),
                    "is_success": (
                        row.get("is_success", "").strip().lower() == "true"
                    ),
                }

        return dispositions

    def _get_reference_field(self, workflow_id):
        fields = self.schemas.get(workflow_id, {})

        for field_name, config in fields.items():
            if config.get("reference", False):
                return field_name

        return None

    def _get_external_ref(self, lead):
        workflow_id = lead["workflow_id"]
        reference_field = self._get_reference_field(workflow_id)

        if reference_field is None:
            return ""

        value = lead.get("fields", {}).get(reference_field)

        if value is None:
            return ""

        return str(value)

    def _decrypt_phone(self, lead):
        encrypted_phone = lead.get("fields", {}).get("phone")

        if encrypted_phone is None:
            encrypted_phone = lead.get("phone_encrypted")

        if encrypted_phone is None or self.security is None:
            return None

        try:
            return self.security.decrypt(encrypted_phone)
        except Exception:
            return None

    def _mask_phone(self, lead):
        phone = self._decrypt_phone(lead)

        if not phone:
            return ""

        digits = "".join(character for character in phone if character.isdigit())

        if len(digits) < 4:
            return ""

        return f"+91******{digits[-4:]}"

    @staticmethod
    def _event_time(event):
        value = event.get("occurred_at")

        if isinstance(value, datetime):
            return value

        if isinstance(value, str):
            return parse_timestamp(value)

        return None

    @staticmethod
    def _event_call_key(event):
        return (
            event.get("workflow_id"),
            event.get("call_id"),
            tuple(event.get("lead_key") or ()),
        )

    def _build_calls(self):
        """Build calls only when a linked call.initiated event exists."""
        grouped_events = defaultdict(list)

        for event in self.events:
            if event.get("lead_key") is None:
                continue

            call_id = event.get("call_id")

            if not call_id:
                continue

            grouped_events[self._event_call_key(event)].append(event)

        calls_by_lead = defaultdict(list)
        calls_by_date = defaultdict(list)

        for call_key, call_events in grouped_events.items():
            initiated_events = [
                event
                for event in call_events
                if event.get("event_type") == "call.initiated"
            ]

            if not initiated_events:
                continue

            initiated_events.sort(
                key=lambda event: (
                    self._event_time(event) is None,
                    self._event_time(event) or datetime.min,
                )
            )

            initiated = initiated_events[0]
            initiated_at = self._event_time(initiated)

            if initiated_at is None:
                continue

            lead_key = tuple(initiated.get("lead_key") or ())
            workflow_id = initiated.get("workflow_id")

            connected = any(
                event.get("event_type") == "call.connected"
                for event in call_events
            )

            telephony_failed = any(
                event.get("event_type") == "call.telephony_failed"
                for event in call_events
            )

            overrides = []
            dispositions_set = []

            for event in call_events:
                event_type = event.get("event_type")

                if event_type == "disposition.override":
                    overrides.append(event)
                elif event_type == "disposition.set":
                    dispositions_set.append(event)

            selected_disposition_event = None

            if overrides:
                selected_disposition_event = self._latest_event(overrides)
            elif dispositions_set:
                selected_disposition_event = self._latest_event(
                    dispositions_set
                )

            disposition_code = None

            if selected_disposition_event is not None:
                disposition_code = (
                    selected_disposition_event.get("payload", {}).get(
                        "disposition_code"
                    )
                )

            disposition_info = self.dispositions.get(
                workflow_id, {}
            ).get(disposition_code)

            call = {
                "workflow_id": workflow_id,
                "lead_key": lead_key,
                "call_id": initiated.get("call_id"),
                "initiated_at": initiated_at,
                "call_date": initiated_at.date().isoformat(),
                "connected": connected,
                "telephony_failed": telephony_failed,
                "disposition_code": disposition_code,
                "disposition_info": disposition_info,
            }

            calls_by_lead[lead_key].append(call)
            calls_by_date[(workflow_id, call["call_date"])].append(call)

        for calls in calls_by_lead.values():
            calls.sort(key=lambda call: call["initiated_at"])

        return calls_by_lead, calls_by_date

    def _latest_event(self, events):
        """Return the event with the latest occurred_at timestamp."""
        valid_events = [
            event for event in events
            if self._event_time(event) is not None
        ]

        if not valid_events:
            return None

        return max(valid_events, key=self._event_time)

    def _get_bucket(self, calls):
        if not calls:
            return "yet_to_reach_out"

        if any(
            call["disposition_info"]
            and call["disposition_info"]["is_terminal"]
            for call in calls
        ):
            return "completed"

        if all(call["telephony_failed"] for call in calls):
            return "in_progress_telephony_failed"

        return "in_progress_dialed"

    def _build_batch_summary(self, calls_by_lead):
        batches = defaultdict(
            lambda: {
                "total_leads": 0,
                "yet_to_reach_out": 0,
                "in_progress_telephony_failed": 0,
                "in_progress_dialed": 0,
                "completed": 0,
            }
        )

        for lead in self.leads:
            workflow_id = lead["workflow_id"]
            batch_id = lead["batch_id"]
            lead_key = (
                workflow_id,
                lead.get("phone_hash"),
            )

            bucket = self._get_bucket(calls_by_lead.get(lead_key, []))

            summary = batches[(workflow_id, batch_id)]
            summary["total_leads"] += 1
            summary[bucket] += 1

        rows = []

        for (workflow_id, batch_id), values in batches.items():
            rows.append({
                "workflow_id": workflow_id,
                "batch_id": batch_id,
                **values,
            })

        return sorted(
            rows,
            key=lambda row: (row["workflow_id"], row["batch_id"]),
        )

    def _build_daily_overview(self, calls_by_date):
        rows = []

        for (workflow_id, call_date), calls in sorted(calls_by_date.items()):
            total_calls = len(calls)
            connected_calls = sum(
                1 for call in calls if call["connected"]
            )

            success_leads = {
                call["lead_key"]
                for call in calls
                if (
                    call["disposition_info"]
                    and call["disposition_info"]["is_success"]
                )
            }

            connectivity_rate = (
                connected_calls / total_calls
                if total_calls
                else 0.0
            )

            rows.append({
                "workflow_id": workflow_id,
                "date": call_date,
                "total_calls": total_calls,
                "connected_calls": connected_calls,
                "connectivity_rate": f"{connectivity_rate:.4f}",
                "success_leads": len(success_leads),
            })

        return rows

    def _build_lead_report(self, calls_by_lead):
        rows = []

        for lead in self.leads:
            workflow_id = lead["workflow_id"]
            lead_key = (
                workflow_id,
                lead.get("phone_hash"),
            )

            calls = calls_by_lead.get(lead_key, [])
            calls = sorted(
                calls,
                key=lambda call: call["initiated_at"],
            )

            total_calls = len(calls)
            connected_calls = sum(
                1 for call in calls if call["connected"]
            )

            last_call_at = (
                calls[-1]["initiated_at"].isoformat()
                if calls
                else ""
            )

            current_disposition = ""

            for call in reversed(calls):
                if call["disposition_code"]:
                    current_disposition = call["disposition_code"]
                    break

            ever_success = any(
                call["disposition_info"]
                and call["disposition_info"]["is_success"]
                for call in calls
            )

            rows.append({
                "workflow_id": workflow_id,
                "external_ref": self._get_external_ref(lead),
                "batch_id": lead["batch_id"],
                "phone_masked": self._mask_phone(lead),
                "bucket": self._get_bucket(calls),
                "total_calls": total_calls,
                "connected_calls": connected_calls,
                "last_call_at": last_call_at,
                "current_disposition": current_disposition,
                "ever_success": str(ever_success).lower(),
            })

        return sorted(
            rows,
            key=lambda row: (
                row["workflow_id"],
                row["external_ref"],
            ),
        )

    def _build_data_quality(self):
        quality = dict(self.lead_quality)
        quality.update(self.event_quality)

        required_keys = (
            "lead_rows_total",
            "lead_rows_rejected",
            "rejections_by_reason",
            "duplicate_lead_rows_merged",
            "field_warnings",
            "events_total",
            "malformed_events",
            "duplicate_events_dropped",
            "orphan_events",
            "unknown_disposition_events",
        )

        for key in required_keys:
            if key not in quality:
                quality[key] = {} if key in (
                    "rejections_by_reason",
                    "field_warnings",
                ) else 0

        return quality

    @staticmethod
    def _write_csv(path, fieldnames, rows):
        with path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames,
            )
            writer.writeheader()
            writer.writerows(rows)

    def generate(self):
        """Generate all five reports and return their output paths."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        calls_by_lead, calls_by_date = self._build_calls()

        batch_rows = self._build_batch_summary(calls_by_lead)
        daily_rows = self._build_daily_overview(calls_by_date)
        lead_rows = self._build_lead_report(calls_by_lead)

        batch_path = self.output_dir / "batch_summary.csv"
        daily_path = self.output_dir / "daily_overview.csv"
        lead_path = self.output_dir / "lead_report.csv"
        rejection_path = self.output_dir / "rejections.csv"
        quality_path = self.output_dir / "data_quality.json"

        self._write_csv(
            batch_path,
            [
                "workflow_id",
                "batch_id",
                "total_leads",
                "yet_to_reach_out",
                "in_progress_telephony_failed",
                "in_progress_dialed",
                "completed",
            ],
            batch_rows,
        )

        self._write_csv(
            daily_path,
            [
                "workflow_id",
                "date",
                "total_calls",
                "connected_calls",
                "connectivity_rate",
                "success_leads",
            ],
            daily_rows,
        )

        self._write_csv(
            lead_path,
            [
                "workflow_id",
                "external_ref",
                "batch_id",
                "phone_masked",
                "bucket",
                "total_calls",
                "connected_calls",
                "last_call_at",
                "current_disposition",
                "ever_success",
            ],
            lead_rows,
        )

        rejection_rows = sorted(
            self.rejections,
            key=lambda row: (
                row["workflow_id"],
                row["batch_id"],
                row["row_number"],
            ),
        )

        self._write_csv(
            rejection_path,
            [
                "workflow_id",
                "batch_id",
                "row_number",
                "reason",
            ],
            rejection_rows,
        )

        with quality_path.open("w", encoding="utf-8") as file:
            json.dump(
                self._build_data_quality(),
                file,
                indent=2,
                ensure_ascii=False,
            )

        return {
            "batch_summary": batch_path,
            "daily_overview": daily_path,
            "lead_report": lead_path,
            "rejections": rejection_path,
            "data_quality": quality_path,
        }
