from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

from airflow.models.baseoperator import chain

from datetime import timedelta


def on_failure_callback(context):
    ti = context["task_instance"]
    print(f"ABOT ERROR: task {ti.task_id } failed in dag { ti.dag_id } ")


def on_retry_callback(context):
    ti = context["task_instance"]
    print(f"ABOT RETRY: task {ti.task_id } failed in dag { ti.dag_id } ")


default_args = {
    "owner": "abot",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure_callback
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'email': ''
}


def _download_data(ti, **kwargs):
    with open("/tmp/test.txt", "w") as f:
        f.write("Hello World")
    ti.xcom_push(key="other_value", value=43)
    return 42  # This will be pushed to xcom as return_value


# ti is the Task Instance object
def _check_data(ti):
    # max size of xcom depends on the db used but it is not recommended to use it for large data
    # MySQL can only store 64KB
    my_value = ti.xcom_pull(task_ids="download_data", key="return_value")
    my_value_2 = ti.xcom_pull(task_ids="download_data", key="other_value")
    print("Checking data:", my_value, my_value_2)


# Dates are in UTC
# datetime(2023,6,1): Always define date and not now() because it will be evaluated when the DAG is loaded
# @weekly, @daily, @hourly, @monthly, @yearly is availably
# timedelta is also available with timedeatla(days=1) where it triggers after 1 day
# None is also available where it will be triggered manually
with DAG(
    dag_id="dag_03",
    start_date=days_ago(3),
    default_args=default_args,
    schedule_interval="*/10 * * * *",
    catchup=True,  # If you do not want to catchup. Use cli to run the DAG with backfill option airflow dags backfill simple_dag -s 2021-01-01 -e 2021-01-02
    # max_active_runs=1, # If you do not want to run multiple DAGs at the same time
    # concurrency=2, # If you do not want to run multiple DAGs at the same time
    tags=["airflow_fundamentals"],  # You will learn about tags in dag authoring course
) as dag:
    download_data = PythonOperator(
        task_id="download_data",
        python_callable=_download_data,
    )

    check_data = PythonOperator(task_id="check_data", python_callable=_check_data)

    wait_for_data = FileSensor(
        task_id="wait_for_data",
        fs_conn_id="fs_default",
        filepath="test.txt",
    )
    # 0 for ok, 1 for error
    process_data = BashOperator(
        task_id="process_data",
        bash_command="exit 1",
        on_retry_callback=on_retry_callback,
    )

    # Not used way
    # download_data.set_downstream(wait_for_data)
    # process_data.set_upstream(wait_for_data)
    # Best practice
    # download_data >> wait_for_data >> process_data
    # To run both tasks after download_data
    # download_data >> [wait_for_data , process_data]

    # Instead of >>, you can use chain
    chain(download_data, check_data, wait_for_data, process_data)
    # Or you can build complex chains where each of the right tasks will be executed after the left tasks:
    # cross_downstream([ download_data, check_data], [wait_for_data, process_data])
