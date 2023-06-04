from airflow.decorators import dag, task
from airflow.operators.subdag import SubDagOperator

from datetime import datetime, timedelta

# Created the factory in a separate file
from dag_authoring.subdag.subdag_factory import subdag_factory


@task.python(task_id="extract_partners", multiple_outputs=True)
def extract():
    print("Extracting data from source")
    return {"partner": "partner_2", "value": 42, "path": "/tmp/"}


# Use default_args to ensure same start_date for all tasks
default_args = {"start_date": datetime(2021, 1, 1)}


@dag(
    description="Fun with dags",
    default_args=default_args,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
)
def dag_authoring_04():
    # Tried to pass the output of the extract task to the subdag, but it didn't work
    # We got an error: Tried to set relationships between tasks in more than one DAG: {<DAG: dag_authoring_04.process_tasks>, <DAG: dag_authoring_04>}
    # This is why we will read the XCom value from the context
    process_tasks = SubDagOperator(
        task_id="process_tasks",
        subdag=subdag_factory("dag_authoring_04", "process_tasks", default_args),
        poke_interval=30,  # SubDag is a sensor, so you configure the poke_interval and mode='reschedule'
        # Don't use task_concurrency here, it will be ignored, use it in the tasks within the subdag
    )

    extract() >> process_tasks


dag = dag_authoring_04()
