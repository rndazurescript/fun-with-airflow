from airflow.decorators import dag, task

from datetime import datetime, timedelta
from typing import Dict


@task.python(
    task_id="extract_partners"
)  # , multiple_outputs=True can be used to split the output into multiple XComs
def extract() -> (
    Dict[str, any]
):  # Alternatively, we specify that output is multiple XComs with a key value pair
    print("Extracting data from source")
    return {"partner": "partner_2", "value": 42, "path": "/tmp/"}


@task  # This is the same as task.python but BestPractice is to use task.python
def process(partner, path):
    print(f"Processing partner: {partner} {path}")


# Not used that often as folks prefer the `with DAG` approach
@dag(
    description="Fun with dags 03",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
)
def dag_authoring_03():
    # Instead of defining the order of the tasks with >>,
    # extract() >> process()
    # we can use the following "XCom implicit" of the TaskAPI
    # If it was just a single output
    # process(extract())
    # Now that we have multiple outputs
    partner_settings = extract()
    process(partner_settings["partner"], partner_settings["path"])
    # If you want to test from CLI
    # airflow tasks test dag_authoring_03 process 2023-06-02
    # You need to run it in the portal first so that the extract task is run and the XComs are created
    # test doesn't store the XComs (as it used to in the past due to a bug)


dag = dag_authoring_03()
