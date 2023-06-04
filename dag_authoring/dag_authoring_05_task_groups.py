from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta


@task.python(task_id="extract_partners", multiple_outputs=True)
def extract():
    print("Extracting data from source")
    return {"partner": "partner_2", "value": 42, "path": "/tmp/"}


@task.python
def process_a(partner, path):
    print(f"Processing partner: {partner} {path}")


@task.python
def process_b(partner, path):
    print(f"Processing partner: {partner} {path}")


@task.python
def process_c(partner, path):
    print(f"Processing partner: {partner} {path}")


@dag(
    description="Fun with dags",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
)
def dag_authoring_05():
    partner_settings = extract()

    with TaskGroup(group_id="process_tasks") as process_tasks:
        process_a(partner_settings["partner"], partner_settings["path"])
        process_b(partner_settings["partner"], partner_settings["path"])
        process_c(partner_settings["partner"], partner_settings["path"])


dag = dag_authoring_05()
