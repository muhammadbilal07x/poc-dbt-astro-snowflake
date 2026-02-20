# Trigger
from __future__ import annotations

import time
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

DBT_CLOUD_API_BASE = "https://cloud.getdbt.com/api/v2"

def trigger_dbt_cloud_job():
    account_id = Variable.get("dbt_account_id")
    job_id = Variable.get("dbt_job_id")
    token = Variable.get("dbt_api_token")

    url = f"{DBT_CLOUD_API_BASE}/accounts/{account_id}/jobs/{job_id}/run/"
    headers = {"Authorization": f"Token {token}"}

    resp = requests.post(url, headers=headers, timeout=30)
    resp.raise_for_status()

    run_id = resp.json()["data"]["id"]
    return run_id  # will be pushed to XCom automatically in Airflow 2+ when using TaskFlow, but not here

def wait_for_run_completion(ti):
    account_id = Variable.get("dbt_account_id")
    token = Variable.get("dbt_api_token")

    run_id = ti.xcom_pull(task_ids="trigger_dbt_job")
    headers = {"Authorization": f"Token {token}"}

    deadline = time.time() + (30 * 60)

    while time.time() < deadline:
        url = f"{DBT_CLOUD_API_BASE}/accounts/{account_id}/runs/{run_id}/"
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        data = resp.json()["data"]
        status = data["status"]

        if status == 10:
            return
        if status in (20, 30):
            raise RuntimeError(f"dbt Cloud run failed/cancelled. run_id={run_id}, status={status}")

        time.sleep(20)

    raise TimeoutError(f"Timed out waiting for dbt Cloud run. run_id={run_id}")

default_args = {"owner": "bilal", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dbt_cloud_sales_inventory_poc",
    start_date=datetime(2026, 2, 17),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["poc", "dbt", "snowflake"],
) as dag:

    trigger = PythonOperator(
        task_id="trigger_dbt_job",
        python_callable=trigger_dbt_cloud_job,
    )

    wait = PythonOperator(
        task_id="wait_for_dbt_completion",
        python_callable=wait_for_run_completion,
    )

    trigger >> wait
