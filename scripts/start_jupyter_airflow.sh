#!/usr/bin/env bash

# Export PATH to ensure conda and local binaries are found
export PATH="/opt/conda/bin:/home/jovyan/.local/bin:$PATH"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# 1. Check and install dependencies if airflow is missing (e.g. after image rebuild)
if ! command -v airflow &> /dev/null; then
    echo "[Init] Airflow not found. Installing project dependencies from requirements.txt..."
    pip install --no-cache-dir -r /home/jovyan/work/requirements.txt
fi

# 2. Clean up stale Airflow pid lock files from previous runs
echo "[Init] Cleaning stale Airflow PID files..."
rm -f /home/jovyan/airflow/airflow-webserver.pid /home/jovyan/airflow/airflow-scheduler.pid /home/jovyan/airflow/airflow-webserver-monitor.pid

# 3. Ensure Airflow directories and DAGs symlink exist
mkdir -p /home/jovyan/airflow
if [ ! -L /home/jovyan/airflow/dags ]; then
    echo "[Init] Linking DAGs directory..."
    ln -sf /home/jovyan/work/05_orchestration /home/jovyan/airflow/dags
fi

# 4. Ensure Delta Lake volume symlink exists
if [ ! -L /app/delta_lake ]; then
    mkdir -p /app 2>/dev/null || true
    ln -sf /home/jovyan/work/data/delta_lake /app/delta_lake 2>/dev/null || true
fi

# 5. Initialize Airflow DB and default admin if missing
if [ ! -f /home/jovyan/airflow/airflow.db ]; then
    echo "[Init] Initializing Airflow database and admin user..."
    airflow db init
    airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
fi

# 6. Start Airflow Webserver and Scheduler in background
echo "[Init] Starting Airflow Webserver on port 8080 (host: 8082)..."
airflow webserver -p 8080 > /home/jovyan/airflow/webserver.log 2>&1 &

echo "[Init] Starting Airflow Scheduler..."
airflow scheduler > /home/jovyan/airflow/scheduler.log 2>&1 &

# 7. Start Real-time Event Generator (stream_producer.py) in background
echo "[Init] Starting Real-time Event Producer (stream_producer.py)..."
pkill -f "stream_producer.py" || true
python -u /home/jovyan/work/03_streaming/stream_producer.py > /home/jovyan/stream_producer.log 2>&1 &

# 8. Start JupyterLab as the main foreground container process
echo "[Init] Starting JupyterLab..."
exec tini -g -- start-notebook.py
