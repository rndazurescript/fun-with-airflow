from airflow.decorators import dag, task, task_group
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

# Besides the python one, we have the SQL branch operator, the datetime branch operator, brach day of week operator, etc

# We can use ShortCircuitOperator https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/python/index.html#airflow.operators.python.ShortCircuitOperator
# To stop the execution of the dag if a condition is not met

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


@task_group
def process_tasks(partner_settings):
    process_a(partner_settings["partner"], partner_settings["path"])
    process_b(partner_settings["partner"], partner_settings["path"])
    process_c(partner_settings["partner"], partner_settings["path"])


def _load_partner_based_on_condition(execution_date):
    day = execution_date.day_of_week
    print(f"Day of the week: {day} for {execution_date}")
    if day == 1:  # Monday
        return "extract_partner_1"
    elif day == 3:  # Wednesday
        return "extract_partner_2"
    elif day == 5:  # Friday
        return "extract_partner_3"
    # We always need to return a task_id
    # We could use the ShortCircuitOperator to stop the execution of the dag if a condition is not met
    # Or schedule the dag to run only on the days we want
    # Or use this simple trick (resource wise, probably not the best option)
    return "stop"


@dag(
    description="Fun with dags",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
    max_active_runs=1,
)
def dag_authoring_08():
    start = DummyOperator(task_id="start")

    # Because some of the process_tasks are not executed,
    # the stop task will also not be executed by default and this is why we add the trigger_rule
    # Anti pattern to use the one_success for our case (Best practice: use none_failed_or_skipped)
    stop = DummyOperator(task_id="stop", trigger_rule="none_failed_or_skipped")
    # You can only use 1 rule at a time. Other options for the trigger_rule are:
    # all_success: default value
    # all_failed: alerting?
    # all_done: Don't care on the result
    # one_failed: alerting as soon as one task fails. Not waiting all of the parent tasks to finish.
    # one_success: start as soon as at least one task succeeds. Not waiting all of the parent tasks to finish.
    # one_done: One or more parents have succeeded, failed or were skipped (status: success, failed, skipped or upstream_failed). Not waiting all of the parent tasks to finish.
    # none_failed: Parents skipped or succeeded. This won't trigger if a parent task fails or upstream task fails (status: upstream_failed)
    # none_skipped: All parents succeeded. This won't trigger if any parent task is skipped (status: skipped)
    # dummy: No matter what the state of the parent tasks is, trigger this task. This is useful to trigger tasks based on external events.

    load_partner_based_on_condition = BranchPythonOperator(
        task_id="load_partner_based_on_condition",
        python_callable=_load_partner_based_on_condition,
    )

    start >> load_partner_based_on_condition
    load_partner_based_on_condition >> stop

    for partner, details in partners.items():

        @task.python(task_id=f"extract_{partner}", multiple_outputs=True)
        def extract(name, path):
            print("Extracting data from source")
            return {"partner": name, "value": 42, "path": path}

        extract_task = extract(partner, details["path"])
        # Instead of start, add the branch operator
        load_partner_based_on_condition >> extract_task
        # Because some of the process_tasks are not executed,
        # the stop task will also not be executed by default even
        # if a single task is executed, this is why we add the trigger_rule
        # in the definition of the stop task
        process_tasks(extract_task) >> stop


dag = dag_authoring_08()
