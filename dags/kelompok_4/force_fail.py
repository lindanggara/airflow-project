"""
DAG: force_fail_for_alert_demo

Tujuan:
- Memicu DAG-DAG lain dengan conf {"force_fail_for_alert": True}
  supaya DAG tersebut sengaja melempar error (kalau mereka punya
  hook pengecek flag ini).

DAG yang dipicu:
  - logging_pipeline_to_postgres
  - intro_etl_workflow
  - advanced_weather_etl
  - master_weather_collection
  - daily_weather_summary
  - hourly_weather_analysis
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

TARGET_DAGS = [
    "logging_pipeline_to_postgres",
    "intro_etl_workflow",
    "advanced_weather_etl",
    "master_weather_collection",
    "daily_weather_summary",
    "hourly_weather_analysis",
]

default_args = {
    "owner": "monitoring_team",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="force_fail_for_alert_demo",
    default_args=default_args,
    schedule=None,        # hanya manual trigger
    catchup=False,
    tags=["monitoring", "alert_demo", "force_fail"],
) as dag:

    trigger_logging = TriggerDagRunOperator(
        task_id="trigger_logging_pipeline_to_postgres_fail",
        trigger_dag_id="logging_pipeline_to_postgres",
        conf={"force_fail_for_alert": True},
        wait_for_completion=False,
    )

    trigger_intro = TriggerDagRunOperator(
        task_id="trigger_intro_etl_workflow_fail",
        trigger_dag_id="intro_etl_workflow",
        conf={"force_fail_for_alert": True},
        wait_for_completion=False,
    )

    trigger_advanced = TriggerDagRunOperator(
        task_id="trigger_advanced_weather_etl_fail",
        trigger_dag_id="advanced_weather_etl",
        conf={"force_fail_for_alert": True},
        wait_for_completion=False,
    )

    trigger_master = TriggerDagRunOperator(
        task_id="trigger_master_weather_collection_fail",
        trigger_dag_id="master_weather_collection",
        conf={"force_fail_for_alert": True},
        wait_for_completion=False,
    )

    trigger_daily = TriggerDagRunOperator(
        task_id="trigger_daily_weather_summary_fail",
        trigger_dag_id="daily_weather_summary",
        conf={"force_fail_for_alert": True},
        wait_for_completion=False,
    )

    trigger_hourly = TriggerDagRunOperator(
        task_id="trigger_hourly_weather_analysis_fail",
        trigger_dag_id="hourly_weather_analysis",
        conf={"force_fail_for_alert": True},
        wait_for_completion=False,
    )

    # Boleh paralel semua
    (
        trigger_logging
        >> [trigger_intro, trigger_advanced, trigger_master, trigger_daily, trigger_hourly]
    )
