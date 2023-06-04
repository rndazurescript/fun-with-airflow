from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.sensors.date_time import DateTimeSensor


from datetime import datetime, timedelta


# Best practice: Always specify timeouts for your tasks and dags.
@dag(
    description="Fun with dags",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    # DAGRun Timeout is not used when manually triggering a DAGRun
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
)
def dag_authoring_11():
    start = DummyOperator(
        task_id="start",
        execution_timeout=timedelta(
            minutes=10
        ),  # This is the timeout for the task. No value by default.
    )

    # This sensor will wait until the execution date is 2021-01-01 10:00:00
    wait_for_10 = DateTimeSensor(
        task_id="wait_for_10",
        target_time="{{ execution_date.add(hours=9) }}",  # Template to be evaluated at run time (Avoid f-strings)
        poke_interval=60 * 60,
        # default poke is 60 seconds, for us it makes sense to check every hour.
        mode="reschedule",  # 'poke' is the default mode which keeps the task running until the condition is met.
        # With 'reschedule' the task is rescheduled and will be executed again at the next schedule interval.
        timeout=60 * 60 * 10,
        # 7 days is the default timeout. In our case, we shouldn't wait more than 10 hours.
        soft_fail=True,  # If the sensor fails, the task will fail. With soft_fail=True the task will not fail but will be skipped.
        exponential_backoff=True,  # If the sensor fails, the time between retries will increase exponentially. Ideal for HTTP requests to avoid flooding the server.
    )

    wait_for_10 >> start


dag = dag_authoring_11()
