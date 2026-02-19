from __future__ import annotations
# trigger deploy
# redeploy trigger
import time
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


DBT_CLOUD_API_BASE = "https://cloud.getdbt.com/api/v2"


def trigger_dbt_cloud_job(**context):
    account_id = context["params"]["account_id"]
    job_id = context["params"]["job_id"]
    token = context["params"]["token"]

    url = f"{DBT_CLOUD_API_BASE}/accounts/{account_id}/jobs/{job_id}/run/"
    headers = {"Authorization": f"Token {token}"}

    resp = requests.post(url, headers=headers, timeout=30)
    resp.raise_for_status()

    run_id = resp.json()["data"]["id"]
    # push to XCom
    context["ti"].xcom_push(key="dbt_run_id", value=run_id)


def wait_for_run_completion(**context):
    account_id = context["params"]["account_id"]
    token = context["params"]["token"]

    run_id = context["ti"].xcom_pull(key="dbt_run_id", task_ids="trigger_dbt_job")
    headers = {"Authorization": f"Token {token}"}

    # Poll for up to ~30 minutes (adjust if needed)
    deadline = time.time() + (30 * 60)

    while time.time() < deadline:
        url = f"{DBT_CLOUD_API_BASE}/accounts/{account_id}/runs/{run_id}/"
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()

        data = resp.json()["data"]
        status = data["status"]  # int codes in dbt Cloud

        # Common status meanings (simplified):
        # 1=Queued, 2=Starting, 3=Running, 10=Success, 20=Error, 30=Cancelled
        if status == 10:
            return
        if status in (20, 30):
            raise RuntimeError(f"dbt Cloud run failed/cancelled. run_id={run_id}, status={status}")

        time.sleep(20)

    raise TimeoutError(f"Timed out waiting for dbt Cloud run. run_id={run_id}")


default_args = {
    "owner": "bilal",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

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
        params={
            "account_id": "{{ var.value.dbt_account_id }}",
            "job_id": "{{ var.value.dbt_job_id }}",
            "token": "{{ var.value.dbt_api_token }}",
        },
    )

    wait = PythonOperator(
        task_id="wait_for_dbt_completion",
        python_callable=wait_for_run_completion,
        params={
            "account_id": "{{ var.value.dbt_account_id }}",
            "token": "{{ var.value.dbt_api_token }}",
        },
    )

    trigger >> wait
