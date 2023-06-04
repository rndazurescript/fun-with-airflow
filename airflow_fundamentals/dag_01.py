from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta


default_args = {"owner": "abot", "retries": 5, "retry_delay": timedelta(minutes=5)}

# Dates are in UTC
# datetime(2023,6,1): Always define date and not now() because it will be evaluated when the DAG is loaded
# @weekly, @daily, @hourly, @monthly, @yearly is availably
# timedelta is also available with timedeatla(days=1) where it triggers after 1 day
# None is also available where it will be triggered manually
with DAG(
    dag_id="dag_01",
    start_date=days_ago(3),
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=False,  # If you do not want to catchup. Use cli to run the DAG with backfill option airflow dags backfill simple_dag -s 2021-01-01 -e 2021-01-02
    max_active_runs=1,  # If you do not want to run multiple DAGs at the same time
    tags=["airflow_fundamentals"],  # You will learn about tags in dag authoring course
) as dag:
    # You can have start_date but shouldn't do it
    task_1 = DummyOperator(task_id="task_1", start_date=datetime(2021, 1, 2))
    # Instead of using the following on all task, I use default_args
    #    retry=5,
    #    retry_delay=timedelta(minutes=5))
    # And I can override defaults:
    task_2 = DummyOperator(task_id="task_2", retries=2)

    task_1 >> task_2
