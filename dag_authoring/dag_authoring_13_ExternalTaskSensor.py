from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Wait for a task in another dag to complete. For example a cleanup task for the XComs, or create cross-dag dependencies.
# Let's wait for the extract_partners task of dag_authoring_04
# When I started this, in the log I saw:
# [2023-06-04, 15:45:29 UTC] {external_task.py:231} INFO - Poking for tasks ['extract_partners'] in dag dag_authoring_04 on 2023-06-03T00:00:00+00:00 ...
# Waiting for the execution date of the external task to be 2023-06-03.
# Once I enabled the dag_authoring_04 DAG and the poking interval passed, I saw:
# [2023-06-04, 15:48:30 UTC] {external_task.py:231} INFO - Poking for tasks ['extract_partners'] in dag dag_authoring_04 on 2023-06-03T00:00:00+00:00 ...
# [2023-06-04, 15:48:30 UTC] {base.py:255} INFO - Success criteria met. Exiting.
from datetime import datetime

default_args = {"start_date": datetime(2021, 1, 1)}


@dag(
    description="Fun with dags",
    # We use the same start_date to align the execution dates
    # That's why it's complicated and probably not a good idea to use ExternalTaskSensor
    default_args=default_args,
    schedule_interval="@daily",
    tags=["dag_authoring"],
    catchup=False,
)
def dag_authoring_13():
    wait_for_extract = ExternalTaskSensor(
        task_id="wait_for_extract",
        external_dag_id="dag_authoring_04",
        external_task_id="extract_partners",
        # execution_delta=timedelta(hours=5),  # The delta, in the execution date, to wait for the external task to complete. Alternatively, use:
        # execution_date_fn=calculate_fn,  # The function that receives the execution date of the current task and the context and returns a list of desired execution dates for the external task.
        failed_states=[
            "failed",
            "skipped",
        ],  # The task status that signify failure of the external task. Defaults to [] which means sensor will run until time out (7 days)
        allowed_states=[
            "success"
        ],  # The task status that signify success of the external task.
    )

    start = DummyOperator(task_id="start")

    wait_for_extract >> start


dag = dag_authoring_13()
