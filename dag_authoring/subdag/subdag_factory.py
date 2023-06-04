from airflow.models import DAG
from airflow.decorators import task

# We got an error: Tried to set relationships between tasks in more than one DAG: {<DAG: dag_authoring_04.process_tasks>, <DAG: dag_authoring_04>}
# This is why we will read the XCom value from the context
from airflow.operators.python import get_current_context


@task.python
def process_a():
    ti = get_current_context()["ti"]
    partner = ti.xcom_pull(
        task_ids="extract_partners", key="partner", dag_id="dag_authoring_04"
    )
    path = ti.xcom_pull(
        task_ids="extract_partners", key="path", dag_id="dag_authoring_04"
    )
    print(f"Processing partner: {partner} {path}")


@task.python
def process_b():
    ti = get_current_context()["ti"]
    partner = ti.xcom_pull(
        task_ids="extract_partners", key="partner", dag_id="dag_authoring_04"
    )
    path = ti.xcom_pull(
        task_ids="extract_partners", key="path", dag_id="dag_authoring_04"
    )
    print(f"Processing partner: {partner} {path}")


@task.python
def process_c():
    ti = get_current_context()["ti"]
    partner = ti.xcom_pull(
        task_ids="extract_partners", key="partner", dag_id="dag_authoring_04"
    )
    path = ti.xcom_pull(
        task_ids="extract_partners", key="path", dag_id="dag_authoring_04"
    )
    print(f"Processing partner: {partner} {path}")


def subdag_factory(parent_dag_id, child_dag_id, default_args):
    with DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",  # This is the name you MUST use for the subdag
        default_args=default_args,
    ) as dag:
        process_a()
        process_b()
        process_c()
    return dag
