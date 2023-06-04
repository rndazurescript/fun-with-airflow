from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta
from time import sleep
from typing import Dict


@dag(
    description="Fun with dags",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    tags=["dag_authoring"],
)
def weird_cases():
    @task
    def extract() -> Dict[str, str]:
        return {"start": "start", "end": "end"}

    bash_task = BashOperator(
        task_id="bash_task",
        bash_command="echo '{{ ti.xcom_pull(task_ids=['extract'], key='start') }}'",
    )

    @task(task_id="a")
    def transform():
        print("Transforming")

    @task_group(group_id="process_tasks")
    def process_tasks():
        @task(task_id="a")
        def p_a():
            print("Processing A")

        @task(task_id="b")
        def p_b():
            print("Processing A")

        p_a() >> p_b()

    tasks = []
    for i in range(10):
        tasks.append(transform())

    tasks >> extract() >> bash_task >> transform() >> process_tasks()


dag = weird_cases()
