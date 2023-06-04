from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup


@task.python
def process_a(partner, path):
    print(f"Processing partner: {partner} {path}")


@task.python
def process_b(partner, path):
    print(f"Processing partner: {partner} {path}")


@task.python
def process_c(partner, path):
    print(f"Processing partner: {partner} {path}")


@task.python
def check_a():
    print("checking")


@task.python
def check_b():
    print("checking")


@task.python
def check_c():
    print("checking")


@task_group  # Another way to define a task group with decorators
def sample():
    pass


def process_tasks(partner_settings):
    with TaskGroup(group_id="process_tasks") as process_tasks:
        # You can also have subgroups
        with TaskGroup(group_id="check_tasks") as check_tasks:
            check_a()
            check_b()
            check_c()
        # And define dependencies between them
        process_a(partner_settings["partner"], partner_settings["path"]) >> check_tasks
        process_b(partner_settings["partner"], partner_settings["path"]) >> check_tasks
        process_c(partner_settings["partner"], partner_settings["path"]) >> check_tasks

        check_tasks >> sample()
