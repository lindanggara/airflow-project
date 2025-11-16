"""
DAG: logging_pipeline_to_postgres

Versi MAIN:
- Menyimpan log ke tabel PostgreSQL: pipeline_logs_main
- Kolom:
    id          : SERIAL PK
    log_time    : timestamp with time zone
    tag         : INFO / WARNING / ERROR
    dag         : nama DAG (dag_id)
    task        : nama task (task_id)
    run_id      : run_id
    state       : state task (success / failed / up_for_retry / dst)
    description : TRY, START, END, DURATION, dll (tanpa DAG/TASK/RUN_ID/STATE)
"""

from datetime import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator

POSTGRES_CONN_ID = "postgres_default"

MONITORED_DAGS = [
    "intro_etl_workflow",
    "advanced_weather_etl",
    "master_weather_collection",
    "hourly_weather_analysis",
    "daily_weather_summary",
    "weather_alert_system",
    "email_on_failure_demo",
]

LOOKBACK_HOURS = 1
LOG_TABLE_NAME = "pipeline_logs_main"


def collect_and_store_task_logs(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = True
    cur = conn.cursor()

    # 1. Buat tabel (kalau belum ada)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE_NAME} (
            id          SERIAL PRIMARY KEY,
            log_time    TIMESTAMPTZ NOT NULL,
            tag         TEXT NOT NULL,
            dag         TEXT NOT NULL,
            task        TEXT NOT NULL,
            run_id      TEXT NOT NULL,
            state       TEXT NOT NULL,
            description TEXT
        );
    """)

    # 2. Ambil task_instance (Airflow 3.x – pakai start_date sebagai anchor)
    select_sql = """
        SELECT
            dag_id,
            task_id,
            run_id,
            state,
            try_number,
            start_date,
            end_date,
            EXTRACT(EPOCH FROM (end_date - start_date)) AS duration_sec
        FROM task_instance
        WHERE dag_id = ANY(%s)
          AND start_date >= NOW() AT TIME ZONE 'utc' - INTERVAL %s
        ORDER BY start_date DESC;
    """

    cur.execute(select_sql, (MONITORED_DAGS, f"{LOOKBACK_HOURS} hours"))
    rows = cur.fetchall()

    if not rows:
        # Tidak usah tulis apa-apa ke DB kalau memang tidak ada activity
        print("ℹ️ Tidak ada task_instance dalam window waktu.")
        return "no_task_instances"

    insert_sql = f"""
        INSERT INTO {LOG_TABLE_NAME} (
            log_time, tag, dag, task, run_id, state, description
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    failed_found = False
    count = 0

    for (
        dag_id,
        task_id,
        run_id,
        state,
        try_number,
        start_date,
        end_date,
        duration_sec,
    ) in rows:

        # Waktu log: ambil dari start_date kalau ada, kalau tidak pakai NOW
        log_time = start_date or datetime.utcnow()

        # Mapping state -> tag
        state_lower = (state or "").lower()
        if state_lower == "failed":
            tag = "ERROR"
            failed_found = True
        elif state_lower in ("up_for_retry", "up_for_reschedule"):
            tag = "WARNING"
        else:
            tag = "INFO"

        # Sisanya dimasukkan ke description (TRY, START, END, DURATION, dll)
        desc_parts = [f"TRY={try_number}"]
        if start_date:
            desc_parts.append(f"START={start_date}")
        if end_date:
            desc_parts.append(f"END={end_date}")
        if duration_sec is not None:
            desc_parts.append(f"DURATION={duration_sec:.2f}s")

        description = " | ".join(desc_parts) if desc_parts else None

        cur.execute(
            insert_sql,
            (log_time, tag, dag_id, task_id, run_id, state, description),
        )
        count += 1

    print(f"✅ Disimpan {count} baris ke {LOG_TABLE_NAME}.")

    if failed_found:
        # Supaya DAG monitoring jadi merah kalau ada task failed
        raise AirflowException(
            "Ada task FAILED pada DAG yang dimonitor (cek tabel pipeline_logs_main)."
        )

    return f"inserted_{count}_rows"


default_args = {
    "owner": "monitoring_team",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="logging_pipeline_to_postgres",
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
    tags=["monitoring", "postgres", "logging"],
) as dag:

    collect_and_store = PythonOperator(
        task_id="collect_and_store_task_logs",
        python_callable=collect_and_store_task_logs,
    )
