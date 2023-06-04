from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


def _extract(ti):
    print("Extracting data from source")
    ti.xcom_push(key="partner", value="partner_1")
    return {"partner": "partner_2", "value": 42, "path": "/tmp/"}


def _process(ti):
    # Default return
    returned = ti.xcom_pull(task_ids="extract", key="return_value")
    # Same as following:
    returned = ti.xcom_pull(task_ids="extract")

    info = ti.xcom_pull(key="partner", task_ids="extract")
    # EXTRA: You can even specify the dag_id and include_prior_dates to get the value from a previous dag run different from the current one.
    print(
        f"Processing partner: {info} {returned['partner']} {returned['value']} {returned['path']}"
    )


with DAG(
    "dag_authoring_02",
    description="Fun with dags 02",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
) as dag:
    # To test this task, within the container run:
    # airflow tasks test dag_authoring_02 extract 2021-01-01
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    process = PythonOperator(task_id="process", python_callable=_process)

    extract >> process
