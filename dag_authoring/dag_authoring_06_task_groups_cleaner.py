from airflow.decorators import dag, task

from datetime import datetime, timedelta

# Move the subgroups to a separate file
# See that file for more complex group examples
from dag_authoring.groups.process_tasks import process_tasks


@task.python(task_id="extract_partners", multiple_outputs=True)
def extract():
    print("Extracting data from source")
    return {"partner": "partner_2", "value": 42, "path": "/tmp/"}


@dag(
    description="Fun with dags",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
)
def dag_authoring_06():
    partner_settings = extract()
    process_tasks(partner_settings)


dag = dag_authoring_06()
