
import csv
import json
import re
from pathlib import Path


REPORT_DIR = Path(__file__).resolve().parents[1] / "reports"


def read_csv(filename):
    path = REPORT_DIR / filename

    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def check_sorted(rows, keys, filename):
    def sort_key(row):
        return tuple(
            int(row[key]) if key == "row_number" else row[key]
            for key in keys
        )

    values = [sort_key(row) for row in rows]

    assert values == sorted(values), (
        f"{filename} is not sorted by {keys}"
    )

    print(f"PASS: {filename} sorting")


def main():
    batch_rows = read_csv("batch_summary.csv")
    daily_rows = read_csv("daily_overview.csv")
    lead_rows = read_csv("lead_report.csv")
    rejection_rows = read_csv("rejections.csv")

    with (REPORT_DIR / "data_quality.json").open(
        "r",
        encoding="utf-8",
    ) as file:
        quality = json.load(file)

    assert len(batch_rows) == 6
    assert len(daily_rows) == 33
    assert len(lead_rows) == 1892
    assert len(rejection_rows) == 108

    print("PASS: report row counts")

    check_sorted(
        batch_rows,
        ["workflow_id", "batch_id"],
        "batch_summary.csv",
    )
    check_sorted(
        daily_rows,
        ["workflow_id", "date"],
        "daily_overview.csv",
    )
    check_sorted(
        lead_rows,
        ["workflow_id", "external_ref"],
        "lead_report.csv",
    )
    check_sorted(
        rejection_rows,
        ["workflow_id", "batch_id", "row_number"],
        "rejections.csv",
    )

    bucket_columns = [
        "yet_to_reach_out",
        "in_progress_telephony_failed",
        "in_progress_dialed",
        "completed",
    ]

    for row in batch_rows:
        bucket_total = sum(
            int(row[column])
            for column in bucket_columns
        )
        assert bucket_total == int(row["total_leads"])

    assert sum(
        int(row["total_leads"])
        for row in batch_rows
    ) == len(lead_rows)

    print("PASS: batch totals and bucket totals")

    for row in daily_rows:
        total = int(row["total_calls"])
        connected = int(row["connected_calls"])
        rate = float(row["connectivity_rate"])

        assert connected <= total
        assert 0.0 <= rate <= 1.0

        expected_rate = connected / total if total else 0.0

        assert row["connectivity_rate"] == f"{expected_rate:.4f}"
        assert re.fullmatch(
            r"\d+\.\d{4}",
            row["connectivity_rate"],
        )

    print("PASS: daily connectivity rates")

    for row in lead_rows:
        phone = row["phone_masked"]

        if phone:
            assert re.fullmatch(r"\+91\*{6}\d{4}", phone), phone

    lead_text = (
        REPORT_DIR / "lead_report.csv"
    ).read_text(encoding="utf-8")

    assert not re.search(r"\+91\d{10}", lead_text)

    rejection_text = (
        REPORT_DIR / "rejections.csv"
    ).read_text(encoding="utf-8")

    # Rejection reports may contain field names in rejection reasons,
    # but must not contain actual phone numbers.
    assert not re.search(r"\+91\d{10}", rejection_text)
    assert not re.search(r"\b\d{10}\b", rejection_text)

    print("PASS: phone masking and rejection report privacy")

    assert (
        quality["lead_rows_total"]
        - quality["lead_rows_rejected"]
        - quality["duplicate_lead_rows_merged"]
        == len(lead_rows)
    )

    assert (
        quality["events_total"]
        - quality["malformed_events"]
        - quality["duplicate_events_dropped"]
        - quality["orphan_events"]
        - quality["unknown_disposition_events"]
        == 8020
    )

    assert sum(
        quality["rejections_by_reason"].values()
    ) == quality["lead_rows_rejected"]

    print("PASS: data quality reconciliation")
    print("\nAll report validation checks passed.")


if __name__ == "__main__":
    main()
