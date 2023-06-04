from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta
from time import sleep


# WATCH OUT: The callbacks won't be retried if they fail.
# Prints will be shown in the logs in the folder e.g.
# /usr/local/airflow/logs/scheduler/2023-06-04/dag_authoring/dag_authoring_12_retries.py.log
# I did see the "Running SLA Checks for" but never saw an SLA miss in the logs or in the /slamiss/list/ page.
# This needs more investigation to ensure that the SLA callback is working.
# https://github.com/apache/airflow/blob/main/airflow/dag_processing/processor.py
def _task_failure_callback(context):
    from airflow.exceptions import AirflowTaskTimeout

    if context["exception"]:
        if isinstance(context["exception"], AirflowTaskTimeout):
            print("task timeout")


def _retry_callback(context):
    if context["ti"].try_number() > 2:
        print("retried more than 2 times")


def _success_callback(context):
    print("Hooray! Success!")
    print(context)


def _dag_failure_callback(context):
    print(context)


def _start_task():
    sleep(15)
    print("start task")


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    # Imagine T1 >> T2 both have SLAs. If T1 is late, T2 will be late too due to T1.
    # T1 is a blocking task for T2.
    # dag: dag object
    # task_list: String list of tasks missed the SLA since last callback.
    # blocking_task_list: All tasks that are not success when callback is called.
    # SLAs: list of SlaMiss objects for each task in task_list.
    # blocking_tis: list of task instances that are in blocking_task_list.
    print("SLA missed")
    print(task_list)
    print(blocking_task_list)
    print(slas)


@dag(
    description="Fun with dags",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    on_failure_callback=_dag_failure_callback,
    on_success_callback=_success_callback,
    sla_miss_callback=_sla_miss_callback,  # SLA is on the dag level, not on the task.
    # SLA won't be checked if manually triggered.
    default_args={"email": None},
    # Needs SMTP configured in Airflow and Email parameter set in the DAG.
)
def dag_authoring_12():
    start = PythonOperator(
        task_id="start",
        python_callable=_start_task,
        on_failure_callback=_task_failure_callback,
        on_success_callback=_success_callback,
        on_retry_callback=_retry_callback,
        retries=0,  # Default is 0 but can be configured in airflow.cfg as default_task_retries or in the default_args.
        retry_delay=timedelta(
            minutes=5
        ),  # Default is 5 minutes but can be configured in airflow.cfg as default_task_retry_delay or in the default_args.
        retry_exponential_backoff=False,  # Instead of retry_delay, you can use retry_exponential_backoff=True to use exponential backoff. For API or DB calls, it's better to use exponential backoff.
        max_retry_delay=timedelta(
            minutes=30
        ),  # The task will retry at max 30 minutes later (useful for exponential backoff that may go beyond that).
    )

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="sleep 90",
        sla=timedelta(minutes=1),  # SLA for the task
    )

    end = DummyOperator(
        task_id="end",
        sla=timedelta(
            minutes=1, seconds=10
        ),  # We want all tasks of this DAG to be completed within 70 seconds. This is why we specify the SLA at the last task.
    )

    start >> bash_task >> end


dag = dag_authoring_12()
