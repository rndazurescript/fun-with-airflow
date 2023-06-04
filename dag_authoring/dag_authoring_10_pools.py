from airflow.decorators import dag, task

# In the UI Admins->pools I created one `intense_tasks_pool` with 1 slot

from datetime import datetime, timedelta
from time import sleep

partners = {
    "partner_1": {
        "name": "partner_1",
        "priority": 2,  # The priority of the task in the pool higher number means higher priority
        "path": "/tmp/partner_1/",
    },
    "partner_2": {"name": "partner_2", "priority": 3, "path": "/tmp/partner_2/"},
    "partner_3": {"name": "partner_3", "priority": 1, "path": "/tmp/partner_3/"},
}


@dag(
    description="Fun with dags",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
)
def dag_authoring_10():
    for partner, details in partners.items():

        @task.python(
            task_id=f"extract_{partner}",
            multiple_outputs=True,
            pool="intense_tasks_pool",  # Tasks will be executed in that pool which allows only 1 slot
            pool_slots=1,  # This is the number of slots that the task will take and defaults to 1.
            priority_weight=details["priority"],
            # The priority of the task in the pool, higher number means higher priority
            # Obviously, priority is taken into account only if all tasks are in the same pool.
            # NOTE: If you trigger the tag manually, the priority is not taken into account.
        )
        def extract(name, path):
            print("Extracting data from source")
            sleep(10)  # Simulate a long running task
            return {"partner": name, "value": 42, "path": path}

        extract_task = extract(partner, details["path"])


dag = dag_authoring_10()
