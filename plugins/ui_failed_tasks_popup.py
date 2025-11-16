"""
Plugin Airflow: Popup Failed Tasks + Tombol Retry/Stop (satu file)

Fitur:
- Halaman baru: /ui-failed-tasks
- Baca metadata DB Airflow (PostgreSQL) -> tabel task_instance
- Cari task FAILED dalam LOOKBACK_HOURS terakhir
- Tampilkan popup peringatan di halaman dengan tombol:
    - Retry  -> clear DAG run via REST API (api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/clear)
    - Stop   -> sembunyikan alert di sesi browser

Cara pakai:
- Simpan file ini di folder: <AIRFLOW_HOME>/plugins/ui_failed_tasks_popup.py
- Sesuaikan METADB_CONN_ID dengan connection ID Postgres metadata Airflow-mu
- Restart webserver Airflow
- Buka http://localhost:8080/ui-failed-tasks

Catatan:
- Ini kerangka kerja dasar, untuk dev / lokal.
- Untuk production: perhatikan CSRF, auth, dan permission REST API.
"""

from typing import List, Dict, Any

from flask import Blueprint, render_template_string, jsonify
from airflow.plugins_manager import AirflowPlugin
# from airflow.www.app import csrf
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ================== KONFIGURASI ==================

# Connection ID ke metadata DB Airflow (PostgreSQL)
# Sesuaikan dengan yang kamu pakai: "postgres_default", "airflow_db", dsb.
METADB_CONN_ID = "postgres_default"

# Cek task gagal dalam berapa jam terakhir
LOOKBACK_HOURS = 1

# Kalau hanya ingin monitor DAG tertentu, isi list ini.
# Kalau mau semua DAG -> set ke None.
MONITORED_DAGS = [
    "intro_etl_workflow",
    "advanced_weather_etl",
    "master_weather_collection",
    "hourly_weather_analysis",
    "daily_weather_summary",
    "weather_alert_system",
]


# ================== FUNGSI UTIL ==================

def _get_failed_tasks() -> List[Dict[str, Any]]:
    """
    Baca metadata DB Airflow (tabel task_instance) dan ambil task FAILED
    dalam LOOKBACK_HOURS terakhir.
    Jika MONITORED_DAGS tidak None, filter hanya DAG tersebut.
    """
    hook = PostgresHook(postgres_conn_id=METADB_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    lookback_interval = f"{LOOKBACK_HOURS} hours"

    if MONITORED_DAGS:
        sql = """
        SELECT
            dag_id,
            task_id,
            run_id,
            execution_date,
            state,
            try_number
        FROM task_instance
        WHERE state = 'failed'
          AND dag_id = ANY(%s)
          AND execution_date >= NOW() AT TIME ZONE 'utc' - INTERVAL %s
        ORDER BY execution_date DESC
        LIMIT 20;
        """
        cur.execute(sql, (MONITORED_DAGS, lookback_interval))
    else:
        sql = """
        SELECT
            dag_id,
            task_id,
            run_id,
            execution_date,
            state,
            try_number
        FROM task_instance
        WHERE state = 'failed'
          AND execution_date >= NOW() AT TIME ZONE 'utc' - INTERVAL %s
        ORDER BY execution_date DESC
        LIMIT 20;
        """
        cur.execute(sql, (lookback_interval,))

    rows = cur.fetchall()
    failed: List[Dict[str, Any]] = []

    for dag_id, task_id, run_id, execution_date, state, try_number in rows:
        failed.append(
            {
                "dag_id": dag_id,
                "task_id": task_id,
                "run_id": run_id,
                "execution_date": execution_date.isoformat() if execution_date else None,
                "state": state,
                "try_number": try_number,
            }
        )

    cur.close()
    conn.close()
    return failed


# ================== BLUEPRINT FLASK ==================

failed_tasks_blueprint = Blueprint(
    "failed_tasks_popup",
    __name__,
    url_prefix="/ui-failed-tasks",
)


@failed_tasks_blueprint.route("/", methods=["GET"])
def failed_tasks_main_page():
    """
    Halaman HTML utama /ui-failed-tasks.
    - Menampilkan info singkat
    - Meng-include JS yang polling ke /ui-failed-tasks/api
    - Jika ada task FAILED -> munculkan popup.
    """
    html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8" />
    <title>Airflow Failed Tasks Monitor</title>
    <style>
        body {
            font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            margin: 0;
            padding: 0;
        }
        .page-container {
            padding: 16px;
        }
        .alert-info {
            background: #edf2ff;
            border-left: 4px solid #4c6fff;
            padding: 12px 16px;
            margin-bottom: 16px;
            border-radius: 4px;
        }
        .popup-overlay {
            position: fixed;
            inset: 0;
            background: rgba(15, 23, 42, 0.65);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 9999;
        }
        .popup-content {
            background: #ffffff;
            border-radius: 12px;
            padding: 20px 24px;
            max-width: 720px;
            width: 90%;
            box-shadow: 0 25px 60px rgba(15, 23, 42, 0.35);
        }
        .popup-header {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 12px;
        }
        .popup-title {
            font-size: 18px;
            font-weight: 700;
            color: #111827;
        }
        .popup-badge {
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            padding: 2px 8px;
            border-radius: 999px;
            background: #fee2e2;
            color: #b91c1c;
        }
        .popup-body {
            max-height: 360px;
            overflow-y: auto;
            margin-bottom: 16px;
        }
        .popup-footer {
            display: flex;
            justify-content: flex-end;
            gap: 8px;
        }
        .task-card {
            border-radius: 8px;
            border: 1px solid #e5e7eb;
            padding: 8px 10px;
            margin-bottom: 8px;
            background: #f9fafb;
        }
        .task-title {
            font-size: 13px;
            font-weight: 600;
            color: #111827;
        }
        .task-meta {
            font-size: 12px;
            color: #6b7280;
        }
        .task-actions {
            margin-top: 6px;
            display: flex;
            gap: 8px;
        }
        .btn {
            font-size: 12px;
            font-weight: 500;
            padding: 4px 10px;
            border-radius: 999px;
            border: none;
            cursor: pointer;
        }
        .btn-retry {
            background: #16a34a;
            color: white;
        }
        .btn-stop {
            background: #e5e7eb;
            color: #374151;
        }
        .btn-close {
            background: transparent;
            color: #6b7280;
        }
        .muted { color: #6b7280; font-size: 12px; }
    </style>
</head>
<body>
    <div class="page-container">
        <h2>Failed Tasks Monitor</h2>
        <div class="alert-info">
            Halaman ini akan menampilkan <strong>popup peringatan</strong> jika ada task
            dari DAG yang dipantau berstatus <code>failed</code> dalam {{ lookback_hours }} jam terakhir.
            Biarkan halaman ini terbuka di browser untuk memantau secara realtime.
        </div>
        <p class="muted">
            Tips: buka tab ini di samping Grid View Airflow. Ketika ada error, popup akan muncul dan
            kamu bisa pilih <strong>Retry</strong> (ulang proses) atau <strong>Stop</strong> (tutup alert).
        </p>
    </div>

    <div id="popup-overlay" class="popup-overlay">
        <div class="popup-content">
            <div class="popup-header">
                <span class="popup-badge">Warning</span>
                <div class="popup-title">Ada proses yang gagal</div>
            </div>
            <div class="popup-body">
                <div id="popup-tasks-container">
                    <!-- diisi via JavaScript -->
                </div>
            </div>
            <div class="popup-footer">
                <button id="btn-close-all" class="btn btn-close">Tutup</button>
            </div>
        </div>
    </div>

    <script>
        const LOOKBACK_HOURS = {{ lookback_hours | tojson }};
        const API_URL = "{{ api_url }}";  // /ui-failed-tasks/api

        const popupOverlay = document.getElementById("popup-overlay");
        const popupTasksContainer = document.getElementById("popup-tasks-container");
        const btnCloseAll = document.getElementById("btn-close-all");

        let lastShownAlertsKey = null;

        function buildAlertKey(alerts) {
            return alerts.map(a => a.dag_id + ":" + a.run_id + ":" + a.task_id).join("|");
        }

        function renderPopup(alerts) {
            popupTasksContainer.innerHTML = "";

            alerts.forEach((a) => {
                const card = document.createElement("div");
                card.className = "task-card";

                const title = document.createElement("div");
                title.className = "task-title";
                title.textContent = `${a.dag_id} :: ${a.task_id}`;

                const meta = document.createElement("div");
                meta.className = "task-meta";
                meta.textContent =
                    `Run: ${a.run_id} · Try: ${a.try_number} · Exec: ${a.execution_date ?? "-"}`;

                const actions = document.createElement("div");
                actions.className = "task-actions";

                const btnRetry = document.createElement("button");
                btnRetry.className = "btn btn-retry";
                btnRetry.textContent = "Retry (Clear DAG Run)";
                btnRetry.onclick = () => retryDagRun(a);

                const btnStop = document.createElement("button");
                btnStop.className = "btn btn-stop";
                btnStop.textContent = "Stop Alert Ini";
                btnStop.onclick = () => {
                    card.style.display = "none";
                    const visibleCards = popupTasksContainer.querySelectorAll(".task-card:not([style*='display: none'])");
                    if (visibleCards.length === 0) {
                        popupOverlay.style.display = "none";
                    }
                };

                actions.appendChild(btnRetry);
                actions.appendChild(btnStop);

                card.appendChild(title);
                card.appendChild(meta);
                card.appendChild(actions);
                popupTasksContainer.appendChild(card);
            });

            popupOverlay.style.display = "flex";
        }

        async function retryDagRun(alert) {
            const ok = window.confirm(
                `Ulangi seluruh DAG Run?\\n\\nDAG: ${alert.dag_id}\\nRun ID: ${alert.run_id}`
            );
            if (!ok) return;

            try {
                const url = `/api/v1/dags/${encodeURIComponent(alert.dag_id)}/dagRuns/${encodeURIComponent(alert.run_id)}/clear`;
                const res = await fetch(url, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json",
                        "Accept": "application/json"
                    },
                    body: JSON.stringify({
                        dry_run: false,
                        reset_dag_runs: true
                    })
                });

                if (!res.ok) {
                    const text = await res.text();
                    alert(`Gagal meng-clear DAG Run.\\nStatus: ${res.status}\\nBody: ${text}`);
                    return;
                }

                alert(`Permintaan clear DAG Run sudah dikirim.\\nCek Grid View DAG di UI Airflow.`);
            } catch (e) {
                console.error("Error retryDagRun:", e);
                alert(`Terjadi error saat memanggil REST API: ${e}`);
            }
        }

        async function pollFailedTasks() {
            try {
                const res = await fetch(API_URL, {
                    method: "GET",
                    headers: { "Accept": "application/json" }
                });
                if (!res.ok) {
                    console.warn("Gagal fetch failed tasks:", res.status);
                    return;
                }
                const data = await res.json();
                const alerts = data.alerts || [];

                if (alerts.length === 0) return;

                const key = buildAlertKey(alerts);
                if (key === lastShownAlertsKey) return;

                lastShownAlertsKey = key;
                renderPopup(alerts);
            } catch (e) {
                console.error("Error polling failed tasks:", e);
            }
        }

        btnCloseAll.addEventListener("click", () => {
            popupOverlay.style.display = "none";
        });

        // Polling setiap 20 detik
        pollFailedTasks();
        setInterval(pollFailedTasks, 20000);
    </script>
</body>
</html>
    """
    return render_template_string(
        html,
        lookback_hours=LOOKBACK_HOURS,
        api_url="/ui-failed-tasks/api",
    )


@failed_tasks_blueprint.route("/api", methods=["GET"])
# @csrf.exempt
def failed_tasks_api():
    """
    Endpoint JSON yang mengembalikan daftar task FAILED.
    Dipanggil oleh JavaScript di /ui-failed-tasks/.
    """
    alerts = _get_failed_tasks()
    return jsonify({"alerts": alerts})


# ================== REGISTRASI PLUGIN ==================


class FailedTasksPopupPlugin(AirflowPlugin):
    name = "failed_tasks_popup_plugin"
    flask_blueprints = [failed_tasks_blueprint]
