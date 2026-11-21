"""Run Phase 3 from Airflow or command line."""
import argparse
import json
from pathlib import Path
from src.spark_pipeline import spark_session
from src.incremental_pipeline import run_demo

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", default="outputs/airflow/phase3")
    args = parser.parse_args()
    spark = spark_session()
    try:
        report, snapshot = run_demo(spark, Path(args.project_root), Path(args.output_dir))
        print(json.dumps(report, indent=2))
        assert snapshot.count() == 99865
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
