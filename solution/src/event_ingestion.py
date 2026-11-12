
import csv
import hashlib
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .lead_ingestion import normalize_phone


IST = timezone(timedelta(hours=5, minutes=30))


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse UTC, offset-aware, and naive timestamps; naive means IST."""
    if not value or not isinstance(value, str):
        return None

    value = value.strip()

    try:
        if value.endswith("Z"):
            parsed = datetime.fromisoformat(value[:-1] + "+00:00")
        else:
            parsed = datetime.fromisoformat(value)

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=IST)

        return parsed.astimezone(IST)

    except ValueError:
        return None


def parse_bool(value: str) -> bool:
    return str(value).strip().lower() == "true"


class EventIngestion:
    """Read, deduplicate, link, and validate call events."""

    def __init__(
        self,
        data_dir: str | Path | None = None,
        leads=None,
        security=None,
        dispositions_path: str | Path | None = None,
        disposition_path: str | Path | None = None,
        database=None,
    ):
        self.project_root = Path(__file__).resolve().parents[2]

        self.data_dir = (
            Path(data_dir)
            if data_dir is not None
            else self.project_root / "data"
        )

        selected_dispositions_path = (
            dispositions_path or disposition_path
        )

        self.dispositions_path = (
            Path(selected_dispositions_path)
            if selected_dispositions_path is not None
            else self.project_root
            / "assignment-a-data"
            / "schemas"
            / "dispositions.csv"
        )

        self.leads = self._extract_leads(leads)
        self.security = security
        self.database = database

        self.dispositions = self._load_dispositions()

        self.events = []
        self.seen_event_ids = set()

        self.events_total = 0
        self.malformed_events = 0
        self.duplicate_events_dropped = 0
        self.orphan_events = 0
        self.unknown_disposition_events = 0

        self._lead_lookup = self._build_lead_lookup()

        if self.database is not None:
            self._load_database_counters()

    @staticmethod
    def _extract_leads(leads):
        """Accept either a lead list or the complete lead-ingestion result."""
        if leads is None:
            return []

        if isinstance(leads, dict):
            return leads.get("leads", [])

        return list(leads)

    def _load_database_counters(self):
        counters = self.database.get_quality_counters()

        self.events_total = counters.get("events_total", 0)
        self.malformed_events = counters.get("malformed_events", 0)
        self.duplicate_events_dropped = counters.get(
            "duplicate_events_dropped",
            0,
        )

    def _increment_counter(self, name):
        if name == "events_total":
            self.events_total += 1
        elif name == "malformed_events":
            self.malformed_events += 1
        elif name == "duplicate_events_dropped":
            self.duplicate_events_dropped += 1

        if self.database is not None:
            self.database.increment_counter(name)

    def _load_dispositions(self) -> dict:
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
                    "is_terminal": parse_bool(row.get("is_terminal", "")),
                    "is_success": parse_bool(row.get("is_success", "")),
                }

        return dispositions

    def _build_lead_lookup(self) -> dict:
        """
        Build a lookup from (workflow_id, phone_hash) to the lead.

        In-memory decryption is used only when a lead has no stored hash.
        Raw phone numbers are never copied into event records.
        """
        lookup = {}

        for lead in self.leads:
            workflow_id = lead.get("workflow_id")
            phone_hash = lead.get("phone_hash")

            if phone_hash:
                lookup[(workflow_id, phone_hash)] = lead
                continue

            fields = lead.get("fields", {})
            encrypted_phone = fields.get("phone")

            if encrypted_phone is None:
                encrypted_phone = lead.get("phone_encrypted")

            if not encrypted_phone or self.security is None:
                continue

            try:
                canonical_phone = self.security.decrypt(encrypted_phone)

                if not canonical_phone:
                    continue

                phone_hash = self.security.phone_lookup_hash(
                    canonical_phone
                )

                lookup[(workflow_id, phone_hash)] = lead

            except Exception:
                continue

        return lookup

    def _find_lead_by_phone(self, workflow_id, phone):
        if not phone or self.security is None:
            return None

        canonical_phone = normalize_phone(str(phone))

        if canonical_phone is None:
            return None

        phone_hash = self.security.phone_lookup_hash(canonical_phone)

        return self._lead_lookup.get((workflow_id, phone_hash))

    @staticmethod
    def _lead_key(lead):
        return (
            lead.get("workflow_id"),
            lead.get("phone_hash"),
        )

    @staticmethod
    def _calculate_checksum(file_path: Path) -> str:
        sha256 = hashlib.sha256()

        with file_path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                sha256.update(chunk)

        return sha256.hexdigest()

    def _read_event_files(self):
        """Yield non-empty JSONL lines in drop, file, and line order."""
        if not self.data_dir.exists():
            raise FileNotFoundError(
                f"Data directory not found: {self.data_dir}"
            )

        drop_dirs = sorted(
            path
            for path in self.data_dir.glob("drop_*")
            if path.is_dir()
        )

        for drop_dir in drop_dirs:
            events_dir = drop_dir / "events"

            if not events_dir.exists():
                continue

            for event_file in sorted(events_dir.glob("*.jsonl")):
                yield event_file

    def _process_event_file(self, event_file: Path):
        checksum = self._calculate_checksum(event_file)
        file_identity = str(event_file.resolve())

        if self.database is not None:
            if self.database.is_file_processed(file_identity, checksum):
                return

        with event_file.open(
            "r",
            encoding="utf-8-sig",
        ) as file:
            for line_number, line in enumerate(file, start=1):
                if not line.strip():
                    continue

                self._increment_counter("events_total")

                try:
                    event = json.loads(line)
                except (json.JSONDecodeError, TypeError):
                    self._increment_counter("malformed_events")
                    continue

                if not isinstance(event, dict) or not event.get("event_id"):
                    self._increment_counter("malformed_events")
                    continue

                event_id = str(event["event_id"])

                if self.database is not None:
                    if event_id in self.seen_event_ids:
                        self._increment_counter(
                            "duplicate_events_dropped"
                        )
                        continue
                elif event_id in self.seen_event_ids:
                    self._increment_counter(
                        "duplicate_events_dropped"
                    )
                    continue

                self.seen_event_ids.add(event_id)

                event_type = event.get("event_type") or "unknown"
                workflow_id = event.get("workflow_id") or "unknown"
                call_id = event.get("call_id")
                payload = event.get("payload") or {}

                if not isinstance(payload, dict):
                    payload = {}

                occurred_at_raw = event.get("occurred_at")
                occurred_at = parse_timestamp(occurred_at_raw)

                phone_hash = None

                if event_type != "disposition.override":
                    phone = event.get("phone")

                    if phone and self.security is not None:
                        canonical_phone = normalize_phone(str(phone))

                        if canonical_phone is not None:
                            phone_hash = self.security.phone_lookup_hash(
                                canonical_phone
                            )

                parsed = {
                    "event_id": event_id,
                    "event_type": event_type,
                    "workflow_id": workflow_id,
                    "call_id": call_id,
                    "occurred_at": occurred_at,
                    "occurred_at_raw": occurred_at_raw,
                    "payload": payload,
                    "source_file": event_file.name,
                    "source_line": line_number,
                    "phone_hash": phone_hash,
                    "lead_key": None,
                }

                if phone_hash:
                    lead = self._lead_lookup.get(
                        (workflow_id, phone_hash)
                    )

                    if lead is not None:
                        parsed["lead_key"] = self._lead_key(lead)

                if self.database is not None:
                    stored_event = {
                        **parsed,
                        "occurred_at": (
                            occurred_at.isoformat()
                            if occurred_at is not None
                            else None
                        ),
                    }

                    inserted = self.database.insert_event(stored_event)

                    if not inserted:
                        self._increment_counter(
                            "duplicate_events_dropped"
                        )

        if self.database is not None:
            self.database.mark_file_processed(
                file_identity,
                checksum,
                "events",
            )

    def _load_persisted_events(self):
        """Load all unique events, including events that were once orphaned."""
        parsed_events = []

        for stored in self.database.get_events():
            occurred_at_raw = stored.get("occurred_at")
            occurred_at = parse_timestamp(occurred_at_raw)

            workflow_id = stored.get("workflow_id")
            phone_hash = stored.get("phone_hash")

            lead = None

            if phone_hash:
                lead = self._lead_lookup.get(
                    (workflow_id, phone_hash)
                )

            parsed_events.append(
                {
                    "event_id": stored["event_id"],
                    "event_type": stored["event_type"],
                    "workflow_id": workflow_id,
                    "call_id": stored.get("call_id"),
                    "occurred_at": occurred_at,
                    "occurred_at_raw": occurred_at_raw,
                    "payload": stored.get("payload", {}),
                    "source_file": stored.get("source_file"),
                    "source_line": stored.get("source_line"),
                    "phone_hash": phone_hash,
                    "lead_key": (
                        self._lead_key(lead)
                        if lead is not None
                        else None
                    ),
                }
            )

        return parsed_events

    def _read_and_deduplicate(self):
        """Parse event files and discard malformed or duplicate records."""
        if self.database is not None:
            for event_file in self._read_event_files():
                self._process_event_file(event_file)

            return self._load_persisted_events()

        parsed_events = []

        for event_file in self._read_event_files():
            with event_file.open(
                "r",
                encoding="utf-8-sig",
            ) as file:
                for line_number, line in enumerate(file, start=1):
                    if not line.strip():
                        continue

                    self._increment_counter("events_total")

                    try:
                        event = json.loads(line)
                    except (json.JSONDecodeError, TypeError):
                        self._increment_counter("malformed_events")
                        continue

                    if not isinstance(event, dict) or not event.get("event_id"):
                        self._increment_counter("malformed_events")
                        continue

                    event_id = str(event["event_id"])

                    if event_id in self.seen_event_ids:
                        self._increment_counter(
                            "duplicate_events_dropped"
                        )
                        continue

                    self.seen_event_ids.add(event_id)

                    event_type = event.get("event_type")
                    workflow_id = event.get("workflow_id")
                    call_id = event.get("call_id")
                    payload = event.get("payload") or {}

                    if not isinstance(payload, dict):
                        payload = {}

                    parsed = {
                        "event_id": event_id,
                        "event_type": event_type,
                        "workflow_id": workflow_id,
                        "call_id": call_id,
                        "occurred_at": parse_timestamp(
                            event.get("occurred_at")
                        ),
                        "occurred_at_raw": event.get("occurred_at"),
                        "payload": payload,
                        "source_file": event_file.name,
                        "source_line": line_number,
                        "phone_hash": None,
                        "lead_key": None,
                    }

                    if event_type != "disposition.override":
                        lead = self._find_lead_by_phone(
                            workflow_id,
                            event.get("phone"),
                        )

                        if lead is not None:
                            parsed["lead_key"] = self._lead_key(lead)

                    parsed_events.append(parsed)

        return parsed_events

    def _link_overrides(self, events):
        """
        Link overrides using the call_id of other events.

        This is done after reading all events so an override can appear
        before the event that establishes its call-to-lead relationship.
        """
        call_to_lead = {}

        for event in events:
            if event["event_type"] == "disposition.override":
                continue

            if event["lead_key"] is None:
                continue

            call_id = event.get("call_id")

            if call_id:
                key = (event["workflow_id"], call_id)
                call_to_lead.setdefault(key, event["lead_key"])

        for event in events:
            if event["event_type"] != "disposition.override":
                continue

            call_id = event.get("call_id")

            if not call_id:
                continue

            key = (event["workflow_id"], call_id)
            event["lead_key"] = call_to_lead.get(key)

    def _filter_events(self, events):
        """Count final orphans and discard unknown disposition codes."""
        linked_events = []

        self.orphan_events = 0
        self.unknown_disposition_events = 0

        for event in events:
            if event["lead_key"] is None:
                self.orphan_events += 1
                continue

            event_type = event["event_type"]

            if (
                isinstance(event_type, str)
                and event_type.startswith("disposition.")
            ):
                payload = event["payload"]
                code = payload.get("disposition_code")
                workflow_id = event["workflow_id"]

                known_codes = self.dispositions.get(workflow_id, {})

                if code not in known_codes:
                    self.unknown_disposition_events += 1
                    continue

            linked_events.append(event)

        return linked_events

    def ingest(self) -> dict:
        """Ingest event drops and return linked events and quality data."""
        parsed_events = self._read_and_deduplicate()

        self._link_overrides(parsed_events)

        self.events = self._filter_events(parsed_events)

        quality = {
            "events_total": self.events_total,
            "malformed_events": self.malformed_events,
            "duplicate_events_dropped": self.duplicate_events_dropped,
            "orphan_events": self.orphan_events,
            "unknown_disposition_events": self.unknown_disposition_events,
        }

        return {
            "events": self.events,
            "quality": quality,
            **quality,
        }
