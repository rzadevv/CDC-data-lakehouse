"""Airflow orchestration DAG for the local CDC lakehouse demo."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

CONNECT_URL = "http://connect:8083"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
PROJECT_DIR = "/opt/airflow/project"

with DAG(
    dag_id="cdc_lakehouse_platform",
    start_date=datetime(2026, 4, 30),
    schedule=None,
    catchup=False,
    tags=["cdc", "lakehouse", "portfolio"],
) as dag:
    start = EmptyOperator(task_id="start")

    wait_for_connect = BashOperator(
        task_id="wait_for_connect",
        bash_command=f"for i in {{1..30}}; do curl -sf {CONNECT_URL}/connectors && exit 0; sleep 5; done; exit 1",
    )

    wait_for_schema_registry = BashOperator(
        task_id="wait_for_schema_registry",
        bash_command=(
            f"for i in {{1..30}}; do curl -sf {SCHEMA_REGISTRY_URL}/subjects && exit 0; "
            "sleep 5; done; exit 1"
        ),
    )

    register_json_connector = BashOperator(
        task_id="register_json_connector",
        bash_command=(
            f"curl -sf -X POST -H 'Content-Type: application/json' "
            f"--data @{PROJECT_DIR}/debezium-connector.json {CONNECT_URL}/connectors "
            "|| curl -sf http://connect:8083/connectors/postgres-connector-json/status"
        ),
    )

    show_connector_status = BashOperator(
        task_id="show_connector_status",
        bash_command=f"curl -sf {CONNECT_URL}/connectors/postgres-connector-json/status",
    )

    show_schema_subjects = BashOperator(
        task_id="show_schema_subjects",
        bash_command=f"curl -sf {SCHEMA_REGISTRY_URL}/subjects",
    )

    end = EmptyOperator(task_id="end")

    start >> [wait_for_connect, wait_for_schema_registry]
    [wait_for_connect, wait_for_schema_registry] >> register_json_connector
    register_json_connector >> [show_connector_status, show_schema_subjects] >> end
