"""Check Airflow pipeline artifacts with fast, non-Spark metadata assertions."""
import json
from pathlib import Path

def main():
    full = json.loads(Path("outputs/airflow/phase2/quality_report_spark.json").read_text())
    incremental = json.loads(Path(
        "outputs/airflow/phase3/snapshots/2026-11-20-demo/manifest.json").read_text())
    assert (full["raw_rows"], full["accepted_rows"], full["rejected_rows"]) == (100100, 99375, 625)
    assert incremental["final_rows"] == 99865
    assert incremental["replay_is_idempotent"]
    print("Phase 2 + Phase 3 reconciliation checks passed")

if __name__ == "__main__":
    main()
