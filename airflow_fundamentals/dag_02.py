from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


default_args = {"owner": "abot", "retries": 5, "retry_delay": timedelta(minutes=5)}


def _download_data(name, ds, **kwargs):
    print("custom param: ", name)
    print(
        "Dag execution date: ", ds
    )  # ds is part of kwargs, else you can use kwargs['ds']
    print("Downloading data. Context: ", kwargs)


# Dates are in UTC
# datetime(2023,6,1): Always define date and not now() because it will be evaluated when the DAG is loaded
# @weekly, @daily, @hourly, @monthly, @yearly is availably
# timedelta is also available with timedeatla(days=1) where it triggers after 1 day
# None is also available where it will be triggered manually
with DAG(
    dag_id="dag_02",
    # start_date= days_ago(3),
    start_date=datetime(2021, 1, 1),  # Start date is required with interval
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=False,  # If you do not want to catchup. Use cli to run the DAG with backfill option airflow dags backfill simple_dag -s 2021-01-01 -e 2021-01-02
    max_active_runs=1,  # If you do not want to run multiple DAGs at the same time
    tags=["airflow_fundamentals"],  # You will learn about tags in dag authoring course
) as dag:
    download_data = PythonOperator(
        task_id="download_data",
        python_callable=_download_data,
        # Custom params
        op_kwargs={"name": "abot"},
    )
