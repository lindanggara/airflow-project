from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator 

# ------------------ DEFAULT ARGS ------------------ #

default_args = {
    "owner": "intan",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,  # biar langsung gagal, mudah mengetes email
    "retry_delay": timedelta(minutes=1),

    # === Konfigurasi email notifikasi ===
    # daftar email penerima (TUJUAN / TO)
    "email": ["itsmeee.intan99@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}

with DAG(
    dag_id="email_on_failure_demo",
    default_args=default_args,
    schedule=None,  # hanya manual trigger
    catchup=False,
    tags=["notification", "email", "demo"],
) as dag:

    # Task dummy yang sukses
    success_task = BashOperator(
        task_id="success_task",
        bash_command="echo 'Task ini sukses berjalan.'",
    )

    # Task yang sengaja dibuat gagal untuk menguji email_on_failure
    failing_task = BashOperator(
        task_id="failing_task",
        bash_command="echo 'Task ini akan gagal'; exit 1",
    )

    success_task >> failing_task
