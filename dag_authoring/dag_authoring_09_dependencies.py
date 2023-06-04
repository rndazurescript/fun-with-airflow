from airflow import DAG

from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import cross_downstream, chain

from datetime import datetime, timedelta


with DAG(
    "dag_authoring_09",
    description="The nice description",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["dag_authoring"],
    catchup=False,
) as dag:
    t1 = DummyOperator(task_id="task_1")
    t2 = DummyOperator(task_id="task_2")
    t3 = DummyOperator(task_id="task_3")
    t4 = DummyOperator(task_id="task_4")
    t5 = DummyOperator(task_id="task_5")
    t6 = DummyOperator(task_id="task_6")
    t7 = DummyOperator(task_id="task_7")

    # Old way
    t1.set_downstream(t2)
    t3.set_upstream(t2)

    # Newer approach with bit-shift operator
    t3 >> t4
    t5 << t4

    # Define multiple dependencies
    [t1, t2] >> t3
    # This doesn't work for list >> list
    # for that we have the cross_downstream method
    # This will put all left as predecessors of all right
    cross_downstream([t1, t2, t3], [t4, t5, t6])
    # This doesn't return anything, so no >> after this.
    # You need to do the following:
    [t4, t5, t6] >> t7

    # Chain
    t11 = DummyOperator(task_id="task_11")
    t12 = DummyOperator(task_id="task_12")
    t13 = DummyOperator(task_id="task_13")
    t14 = DummyOperator(task_id="task_14")
    t15 = DummyOperator(task_id="task_15")
    t16 = DummyOperator(task_id="task_16")
    # The 2 lists must have the same length
    chain(t11, [t12, t13], [t14, t15], t16)
    # This is equivalent to:
    # t11 >> t12 >> t14 >> t16
    # t11 >> t13 >> t15 >> t16

    # Chain with cross_downstream
    t21 = DummyOperator(task_id="task_21")
    t22 = DummyOperator(task_id="task_22")
    t23 = DummyOperator(task_id="task_23")
    t24 = DummyOperator(task_id="task_24")
    t25 = DummyOperator(task_id="task_25")
    t26 = DummyOperator(task_id="task_26")
    cross_downstream([t22, t23], [t24, t25])
    # The 2 lists must have the same length
    chain(t21, [t22, t23], [t24, t25], t26)
