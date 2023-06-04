# Airflow for now does not support fully dynamic tasks based on data from a previous task.
# This is how we can create dynamic tasks from already known data, like from a dictionary.
# Also check learning notes on how to generate DAGs dynamically, which may also be used for tasks.
from airflow.decorators import dag, task, task_group
from airflow.operators.dummy import DummyOperator

from datetime import datetime, timedelta

partners = {
    "partner_1": {"name": "partner_1", "path": "/tmp/partner_1/"},
    "partner_2": {"name": "partner_2", "path": "/tmp/partner_2/"},
    "partner_3": {"name": "partner_3", "path": "/tmp/partner_3/"},
}


@task.python
def process_a(partner, path):
    print(f"Processing partner: {partner} {path}")


@task.python
def process_b(partner, path):
    print(f"Processing partner: {partner} {path}")


@task.python
def process_c(partner, path):
    print(f"Processing partner: {partner} {path}")


# Based on docs, to avoid collisions with other tasks, you can add a suffix to the group_id
# add_suffix_on_collision=True
# But airflow was complaining and it seems that it is not needed as the default value is True
@task_group
def process_tasks(partner_settings):
    process_a(partner_settings["partner"], partner_settings["path"])
    process_b(partner_settings["partner"], partner_settings["path"])
    process_c(partner_settings["partner"], partner_settings["path"])


@dag(
    description="Fun with dags",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
)
def dag_authoring_07():
    start = DummyOperator(task_id="start")
    for partner, details in partners.items():
        # We move the definition here to
        # have clear names for the tasks otherwise we got extract_partner, extract_partner__1, etc
        @task.python(task_id=f"extract_{partner}", multiple_outputs=True)
        def extract(name, path):
            print("Extracting data from source")
            return {"partner": name, "value": 42, "path": path}

        # extract_task is the XCom arg
        extract_task = extract(partner, details["path"])

        # We add a dummy start to have better graph visualization
        start >> extract_task

        # And we can even set the group name (or use the __1, __2 automatic suffixes that airflow adds)
        # @task_group(group_id=f"process_{partner}")
        # def process_tasks(partner_settings):
        #     process_a(partner_settings["partner"], partner_settings["path"])
        #     process_b(partner_settings["partner"], partner_settings["path"])
        #     process_c(partner_settings["partner"], partner_settings["path"])

        process_tasks(extract_task)


dag = dag_authoring_07()
