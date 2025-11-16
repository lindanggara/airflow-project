from datetime import datetime
from textwrap import dedent

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.email import send_email

# -------------------------------------------------------------------
# KONFIGURASI
# -------------------------------------------------------------------

# Daftar DAG yang dipantau
MONITORED_DAGS = [
    "logging_pipeline_to_postgres",
    "intro_etl_workflow",
    "advanced_weather_etl",
    "master_weather_collection",
    "daily_weather_summary",
    "hourly_weather_analysis",
]

# Connection ID ke metastore Postgres Airflow
POSTGRES_CONN_ID = "postgres_default"

# Kirim alert kalau ada task gagal dalam X menit terakhir
LOOKBACK_MINUTES = 10

# Email tujuan alert
ALERT_EMAILS = ["itsmeee.intan99@gmail.com"]

# Base URL Airflow Webserver untuk membuat link ke log
# Buat Variable bernama AIRFLOW_BASE_URL di UI (Admin -> Variables),
# contoh: http://localhost:8083
DEFAULT_BASE_URL = "http://localhost:8083"


# -------------------------------------------------------------------
# FUNGSI UTAMA: cek task gagal dan kirim email
# -------------------------------------------------------------------
def check_failed_tasks_and_send_email():
    """
    Mengecek tabel task_instance untuk DAG-DAG yang dipantau.
    Jika ada task FAILED dalam LOOKBACK_MINUTES terakhir, kirim email.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Siapkan placeholder untuk klausa IN (%s, %s, ...)
    dag_placeholders = ", ".join(["%s"] * len(MONITORED_DAGS))

    query = f"""
        SELECT
            dag_id,
            task_id,
            run_id,
            state,
            try_number,
            start_date,
            end_date
        FROM task_instance
        WHERE dag_id IN ({dag_placeholders})
          AND state = 'failed'
          AND start_date >= NOW() AT TIME ZONE 'utc' - INTERVAL %s
        ORDER BY start_date DESC;
    """

    params = [*MONITORED_DAGS, f"{LOOKBACK_MINUTES} minutes"]
    cur.execute(query, params)
    rows = cur.fetchall()

    if not rows:
        # Tidak ada kegagalan baru, tidak perlu kirim email
        cur.close()
        conn.close()
        return "no_failed_tasks_found"

    # Ambil base URL Airflow dari Variable, kalau tidak ada pakai default
    base_url = Variable.get("AIRFLOW_BASE_URL", DEFAULT_BASE_URL)

    # Susun isi email (HTML)
    li_items = []
    for dag_id, task_id, run_id, state, try_number, start_date, end_date in rows:
        log_url = (
            f"{base_url}/dags/{dag_id}/grid"
            f"?task_id={task_id}&run_id={run_id}&tab=log"
        )

        li_items.append(
            f"<li>"
            f"<b>DAG:</b> {dag_id} &nbsp;|&nbsp; "
            f"<b>Task:</b> {task_id} &nbsp;|&nbsp; "
            f"<b>Run ID:</b> {run_id} &nbsp;|&nbsp; "
            f"<b>Try:</b> {try_number} &nbsp;|&nbsp; "
            f"<b>Start:</b> {start_date} &nbsp;|&nbsp; "
            f"<b>End:</b> {end_date} &nbsp;|&nbsp; "
            f'<a href="{log_url}">Lihat log</a>'
            f"</li>"
        )

    html_content = dedent(
        f"""
        <p>Halo,</p>

        <p>Dalam <b>{LOOKBACK_MINUTES} menit</b> terakhir ditemukan task
        dengan status <b>FAILED</b> pada DAG-DAG berikut:</p>

        <ul>
        {''.join(li_items)}
        </ul>

        <p>Silakan klik link <b>"Lihat log"</b> untuk melihat detail error
        lengkap di Airflow Web UI.</p>

        <p>Salam,<br/>
        Airflow Alert DAG</p>
        """
    )

    subject = "[Airflow Alert] Task FAILED pada DAG ETL / Logging"

    # Kirim email
    send_email(to=ALERT_EMAILS, subject=subject, html_content=html_content)

    cur.close()
    conn.close()

    return f"sent_alert_for_{len(rows)}_failed_tasks"


# -------------------------------------------------------------------
# DEFINISI DAG
# -------------------------------------------------------------------
default_args = {
    "owner": "monitoring",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

with DAG(
    dag_id="multi_dag_failure_alert",
    description=(
        "DAG alert yang mengecek task FAILED pada beberapa DAG ETL "
        "dan mengirim email ringkasan error."
    ),
    default_args=default_args,
    schedule="*/5 * * * *",  # jalan tiap 5 menit
    catchup=False,
    tags=["monitoring", "alert", "email"],
) as dag:

    check_and_alert = PythonOperator(
        task_id="check_failed_tasks_and_send_email",
        python_callable=check_failed_tasks_and_send_email,
    )
