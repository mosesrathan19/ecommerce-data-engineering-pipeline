"""Phase 4 Airflow 3 DAG: orchestrate Phase 2 and Phase 3.

This is a manually triggered demonstration. The synthetic CDC batch in Phase 3
has a fixed ID; an actual daily workflow needs a new incoming batch and durable
storage before enabling a time-based schedule.
"""
from datetime import datetime, timezone
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

@dag(
    dag_id="ecommerce_etl_phases_2_and_3",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "data-engineering", "pyspark"],
)
def ecommerce_etl():
    phase2 = BashOperator(
        task_id="phase2_full_refresh",
        bash_command=(
            "cd /opt/project && python -m src.spark_pipeline "
            "--raw-dir data/raw --output-dir outputs/airflow/phase2"
        ),
        retries=2,
        retry_delay=__import__("datetime").timedelta(minutes=2),
    )
    phase3 = BashOperator(
        task_id="phase3_incremental_cdc",
        bash_command=(
            "cd /opt/project && python -m src.phase3_cli "
            "--output-dir outputs/airflow/phase3"
        ),
        retries=2,
        retry_delay=__import__("datetime").timedelta(minutes=2),
    )
    verify = BashOperator(
        task_id="verify_outputs",
        bash_command="cd /opt/project && python -m src.verify_airflow",
    )
    phase2 >> phase3 >> verify

ecommerce_etl()
