
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

from .security import SecurityManager


def normalize_header(value: str) -> str:
    value = value.strip().lower()
    return re.sub(r"[_\s]+", " ", value)


def normalize_phone(value: str) -> str | None:
    digits = re.sub(r"\D", "", value)

    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]

    if len(digits) != 10 or digits[0] not in "6789":
        return None

    return f"+91{digits}"


def normalize_date(value: str) -> str | None:
    value = value.strip()

    formats = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d-%b-%Y",
    )

    for date_format in formats:
        try:
            return datetime.strptime(value, date_format).date().isoformat()
        except ValueError:
            continue

    return None


def normalize_decimal(value: str) -> str | None:
    value = value.strip()
    value = re.sub(
        r"^(₹|rs\.?|inr)\s*",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = value.replace(",", "").strip()

    try:
        amount = Decimal(value)

        if not amount.is_finite() or amount < 0:
            return None

        amount = amount.quantize(
            Decimal("0.01"),
            rounding=ROUND_HALF_UP,
        )
        return str(amount)

    except (InvalidOperation, ValueError):
        return None


def normalize_enum(value: str, allowed_values: list[str]) -> str | None:
    normalized = value.strip().lower()
    allowed = [item.lower() for item in allowed_values]

    if normalized in allowed:
        return normalized

    return None


def build_header_mapping(headers: list[str], schema_fields: dict) -> dict:
    aliases = {}

    for field_name, field_config in schema_fields.items():
        aliases[normalize_header(field_name)] = field_name

        for alias in field_config.get("aliases", []):
            aliases[normalize_header(alias)] = field_name

    mapping = {}

    for index, header in enumerate(headers):
        normalized_header = normalize_header(header)

        if normalized_header in aliases:
            mapping[index] = aliases[normalized_header]

    return mapping


def is_blank_row(row: list[str]) -> bool:
    return all(not (cell or "").strip() for cell in row)


class LeadIngestion:
    """Ingest, validate, encrypt, merge and persist lead CSV records."""

    def __init__(
        self,
        data_dir: str | Path | None = None,
        schema_dir: str | Path | None = None,
        security: SecurityManager | None = None,
        schemas_dir: str | Path | None = None,
        security_manager: SecurityManager | None = None,
        database=None,
    ):
        self.project_root = Path(__file__).resolve().parents[2]

        self.data_dir = (
            Path(data_dir)
            if data_dir is not None
            else self.project_root / "data"
        )

        selected_schema_dir = schema_dir or schemas_dir

        self.schemas_dir = (
            Path(selected_schema_dir)
            if selected_schema_dir is not None
            else self.project_root / "assignment-a-data" / "schemas"
        )

        self.security = security or security_manager or SecurityManager()
        self.database = database

        self.schemas = self._load_schemas()

        self.leads = {}

        self.lead_rows_total = 0
        self.lead_rows_rejected = 0
        self.rejections_by_reason = Counter()
        self.duplicate_lead_rows_merged = 0
        self.field_warnings = Counter()
        self.rejections = []

        if self.database is not None:
            self._load_database_state()

    def _load_schemas(self) -> dict:
        schemas = {}

        if not self.schemas_dir.exists():
            raise FileNotFoundError(
                f"Schema directory not found: {self.schemas_dir}"
            )

        for schema_file in sorted(self.schemas_dir.glob("*.json")):
            with schema_file.open("r", encoding="utf-8") as file:
                schema = json.load(file)

            workflow_id = schema.get(
                "workflow_id",
                schema_file.stem,
            )

            raw_fields = schema.get("fields", [])

            if isinstance(raw_fields, list):
                schema_fields = {}

                for field_config in raw_fields:
                    field_name = field_config.get("name")

                    if not field_name:
                        raise ValueError(
                            f"Field without a name in schema: {schema_file}"
                        )

                    schema_fields[field_name] = field_config

            elif isinstance(raw_fields, dict):
                schema_fields = raw_fields

            else:
                raise ValueError(
                    f"Invalid fields format in schema: {schema_file}"
                )

            schema["fields"] = schema_fields
            schemas[workflow_id] = schema

        return schemas

    def _load_database_state(self):
        for lead in self.database.get_leads():
            identity = (
                lead["workflow_id"],
                lead["phone_hash"],
            )
            self.leads[identity] = lead

        counters = self.database.get_quality_counters()

        self.lead_rows_total = counters.get("lead_rows_total", 0)
        self.lead_rows_rejected = counters.get("lead_rows_rejected", 0)
        self.duplicate_lead_rows_merged = counters.get(
            "duplicate_lead_rows_merged",
            0,
        )

        self.rejections_by_reason.update(
            self.database.get_quality_groups("rejections_by_reason")
        )

        self.field_warnings.update(
            self.database.get_quality_groups("field_warnings")
        )

        self.rejections = self.database.get_rejections()

    def _increment_counter(self, name: str, amount: int = 1):
        if name == "lead_rows_total":
            self.lead_rows_total += amount

        elif name == "lead_rows_rejected":
            self.lead_rows_rejected += amount

        elif name == "duplicate_lead_rows_merged":
            self.duplicate_lead_rows_merged += amount

        if self.database is not None:
            self.database.increment_counter(name, amount)

    def _increment_group(self, group_name: str, group_key: str):
        if group_name == "rejections_by_reason":
            self.rejections_by_reason[group_key] += 1

        elif group_name == "field_warnings":
            self.field_warnings[group_key] += 1

        if self.database is not None:
            self.database.increment_group(group_name, group_key)

    def _validate_row(
        self,
        row: list[str],
        header_mapping: dict,
        schema_fields: dict,
    ):
        normalized_values = {}

        for index, field_name in header_mapping.items():
            if index < len(row):
                normalized_values[field_name] = (row[index] or "").strip()

        validated = {}
        warnings = []

        for field_name, field_config in schema_fields.items():
            raw_value = normalized_values.get(field_name, "")

            if not raw_value:
                if field_config.get("required", False):
                    return None, f"missing_required:{field_name}", []

                validated[field_name] = None
                continue

            value, error = self._validate_field(raw_value, field_config)

            if error:
                if field_config.get("required", False):
                    return None, error, []

                validated[field_name] = None
                warnings.append(field_name)
            else:
                validated[field_name] = value

        return validated, None, warnings

    @staticmethod
    def _validate_field(value: str, field_config: dict):
        field_type = field_config.get("type", "string")

        if field_type == "string":
            return value.strip(), None

        if field_type == "phone":
            normalized = normalize_phone(value)
            if normalized is None:
                return None, "invalid_phone"
            return normalized, None

        if field_type == "date":
            normalized = normalize_date(value)
            if normalized is None:
                return None, "invalid_date"
            return normalized, None

        if field_type == "decimal":
            normalized = normalize_decimal(value)
            if normalized is None:
                return None, "invalid_decimal"
            return normalized, None

        if field_type == "enum":
            normalized = normalize_enum(
                value,
                field_config.get("values", []),
            )
            if normalized is None:
                return None, "invalid_enum"
            return normalized, None

        return value.strip(), None

    def _apply_defaults(self, fields: dict, schema_fields: dict) -> dict:
        for field_name, field_config in schema_fields.items():
            if fields.get(field_name) is None:
                if "default" in field_config:
                    default_value = field_config["default"]

                    if field_config.get("pii", False):
                        default_value = self.security.encrypt(
                            str(default_value)
                        )

                    fields[field_name] = default_value

        return fields

    def _merge_lead(
        self,
        workflow_id: str,
        batch_id: str,
        validated: dict,
        schema_fields: dict,
    ):
        canonical_phone = validated.get("phone")
        phone_hash = self.security.phone_lookup_hash(canonical_phone)

        identity = (workflow_id, phone_hash)

        if identity not in self.leads:
            fields = {}

            for field_name, field_config in schema_fields.items():
                value = validated.get(field_name)

                if value is not None and field_config.get("pii", False):
                    value = self.security.encrypt(str(value))

                fields[field_name] = value

            self._apply_defaults(fields, schema_fields)

            lead = {
                "workflow_id": workflow_id,
                "batch_id": batch_id,
                "phone_hash": phone_hash,
                "fields": fields,
            }

            self.leads[identity] = lead

        else:
            lead = self.leads[identity]
            self._increment_counter("duplicate_lead_rows_merged")

            for field_name, field_config in schema_fields.items():
                value = validated.get(field_name)

                if value is None:
                    continue

                if field_config.get("pii", False):
                    value = self.security.encrypt(str(value))

                lead["fields"][field_name] = value

        if self.database is not None:
            self.database.upsert_lead(lead)

    @staticmethod
    def _calculate_checksum(file_path: Path) -> str:
        sha256 = hashlib.sha256()

        with file_path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                sha256.update(chunk)

        return sha256.hexdigest()

    def _process_csv(
        self,
        csv_path: Path,
        workflow_id: str,
        schema_fields: dict,
    ):
        checksum = self._calculate_checksum(csv_path)
        file_identity = str(csv_path.resolve())

        if self.database is not None:
            if self.database.is_file_processed(file_identity, checksum):
                return

        batch_id = csv_path.stem

        with csv_path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:
            reader = csv.reader(file)

            try:
                headers = next(reader)
            except StopIteration:
                if self.database is not None:
                    self.database.mark_file_processed(
                        file_identity,
                        checksum,
                        "leads",
                    )
                return

            header_mapping = build_header_mapping(
                headers,
                schema_fields,
            )

            row_number = 0

            for row in reader:
                if is_blank_row(row):
                    continue

                row_number += 1
                self._increment_counter("lead_rows_total")

                validated, error, warnings = self._validate_row(
                    row,
                    header_mapping,
                    schema_fields,
                )

                if error:
                    self._increment_counter("lead_rows_rejected")
                    self._increment_group("rejections_by_reason", error)

                    rejection = {
                        "workflow_id": workflow_id,
                        "batch_id": batch_id,
                        "row_number": row_number,
                        "reason": error,
                    }

                    self.rejections.append(rejection)

                    if self.database is not None:
                        self.database.save_rejection(
                            rejection,
                            file_identity,
                        )

                    continue

                for warning in warnings:
                    self._increment_group("field_warnings", warning)

                self._merge_lead(
                    workflow_id,
                    batch_id,
                    validated,
                    schema_fields,
                )

        if self.database is not None:
            self.database.mark_file_processed(
                file_identity,
                checksum,
                "leads",
            )

    def ingest(self) -> dict:
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
            leads_dir = drop_dir / "leads"

            if not leads_dir.exists():
                continue

            for workflow_dir in sorted(
                path for path in leads_dir.iterdir() if path.is_dir()
            ):
                workflow_id = workflow_dir.name

                if workflow_id not in self.schemas:
                    continue

                schema_fields = self.schemas[workflow_id].get(
                    "fields",
                    {},
                )

                for csv_path in sorted(workflow_dir.glob("*.csv")):
                    self._process_csv(
                        csv_path,
                        workflow_id,
                        schema_fields,
                    )

        quality = {
            "lead_rows_total": self.lead_rows_total,
            "lead_rows_rejected": self.lead_rows_rejected,
            "rejections_by_reason": dict(self.rejections_by_reason),
            "duplicate_lead_rows_merged": self.duplicate_lead_rows_merged,
            "field_warnings": dict(self.field_warnings),
        }

        return {
            "leads": list(self.leads.values()),
            "quality": quality,
            "lead_rows_total": self.lead_rows_total,
            "lead_rows_rejected": self.lead_rows_rejected,
            "rejections_by_reason": dict(self.rejections_by_reason),
            "duplicate_lead_rows_merged": self.duplicate_lead_rows_merged,
            "field_warnings": dict(self.field_warnings),
            "rejections": self.rejections,
        }
