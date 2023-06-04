from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator

# Let's assume you want to trigger another dag from this dag, which needs to be enabled
# (I had to manually enable it because it was stack in scheduled state)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# The logs I noticed:
# [2023-06-04, 16:09:18 UTC] {trigger_dagrun.py:160} INFO - Clearing dag_authoring_04 on 2023-06-03T00:00:00+00:00
# [2023-06-04, 16:09:18 UTC] {trigger_dagrun.py:197} INFO - Waiting for dag_authoring_04 on 2023-06-03 00:00:00+00:00 to become allowed state ['success']
# ... I went and enabled the DAG which was scheduled ...
# [2023-06-04, 16:11:18 UTC] {trigger_dagrun.py:210} INFO - dag_authoring_04 finished with allowed state success

from datetime import datetime

default_args = {"start_date": datetime(2021, 1, 1)}


@dag(
    description="Fun with dags",
    default_args=default_args,
    schedule_interval="@daily",
    tags=["dag_authoring"],
    catchup=False,
)
def dag_authoring_14():
    start = DummyOperator(task_id="start")

    # TriggerDagRunOperator is not based on sensor, so doesn't have mode.
    trigger_dag_04 = TriggerDagRunOperator(
        task_id="trigger_dag_04",
        trigger_dag_id="dag_authoring_04",
        execution_date="{{ ds }}",  # Useful for backfilling
        wait_for_completion=True,  # Wait for the dag to complete. This is the argument people were waiting to migrate from external task sensor
        poke_interval=60,  # The poke interval for the dag to complete. Meaningful value should be used.
        reset_dag_run=True,  # Best Practice: Set to True instead of default False. If you want to rerun, you need to clean the invoking dag otherwise it will throw exception.
        failed_states=[
            "failed"
        ],  # Default empty so we need to specify this to avoid waiting for timeout.
    )

    trigger_dag_04 >> start


dag = dag_authoring_14()
