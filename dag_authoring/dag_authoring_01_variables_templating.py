from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.models import Variable


# To test this task, within the container run:
# airflow tasks test dag_authoring_01 extract 2021-01-01
def _extract():
    print("Extracting data from source")
    # BestPractice: Prefix your variables with the dag_id
    # sensitive_var_conn_names configuration under the section core in airflow.cfg has the keywords to mask
    # e.g. password, secret, api_key
    # BestPractice: Fetch the variables in the task, not in the dag file to avoid loading the variables every time the dag file is parsed
    partner = Variable.get("dag_authoring_01_partner")
    # Both UI and logs will show ****
    secret = Variable.get("dag_authoring_01_service_secret")
    print(f"Partner: {partner} with secret: {secret}")
    # A better approach to fetch both variables at once is to use json instead of string
    # If will make only a single call to the database, but sensitive info may be exposed in the logs
    # Create a variable with {
    # "name": "test",
    # "secret": "the_secret"
    # }
    # Then fetch it with
    service_info = Variable.get(
        "dag_authoring_01_service_connection_secret", deserialize_json=True
    )
    print(f"Service: {service_info['name']} with secret: {service_info['secret']}")


class CustomPostgresOperator(PostgresOperator):
    # We can make additional fields templateable by adding them to template_fields
    template_fields = (
        "sql",
        "parameters",
    )


with DAG(
    "dag_authoring_01",
    description="The nice description",
    start_date=datetime(2021, 1, 1),  # This is required if you have at least one task
    schedule_interval="@daily",  # or timedelta(days=1)
    dagrun_timeout=timedelta(
        minutes=10
    ),  #  Ensuring that is less than the schedule interval, otherwise you will have overlapping dag runs
    tags=["data_science_team", "customers", "dag_authoring"],  # You can group by tags
    catchup=False,  # If you want to run only the latest dag run. Best practice is to set it to False. Or catch_by_default in airflow.cfg.
    # You can backfill through CLI even if catchup is False. airflow dags backfill -s 2021-01-01 -e 2022-01-1
    max_active_runs=1,  # If you want to run only one dag run at a time.
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=_extract)

    # I can use variables through the templating system and these will not be evaluated when
    # the dag file is parsed, but when the task is executed (e.g. instead of using
    # Variable.get("dag_authoring_01_service_connection_secret", deserialize_json=True)['name']])
    process = BashOperator(
        task_id="process",
        bash_command="echo Service name: {{ var.json.dag_authoring_01_service_connection_secret.name }}",
    )

    # You can use environment variables as well. In the docker file add
    # ENV AIRFLOW_VAR_DAG_AUTHORING_01_ENVIRONMENT_VARIABLE='{"name":"test from env", "path":"/tmp"}'
    # Note the capitalization and the prefix AIRFLOW_VAR_
    # These variables are hidden from airflow UI and CLI.
    # Additional benefit, you don't make a connection to meta database.
    verify = BashOperator(
        task_id="verify",
        bash_command="echo env name: {{ var.json.dag_authoring_01_environment_variable.path }}",
    )

    # To find if parameters are templated or not
    # https://registry.astronomer.io/providers/apache-airflow-providers-postgres/versions/5.5.0/modules/PostgresOperator
    # fetch_data = PostgresOperator(
    #     task_id="fetch_data",
    #     sql="SELECT partner_name FROM my_table WHERE created_at={{ ds }}", # ds is the execution date and will be in format 2021-01-01
    # )

    # BestPractice: Use SQL files instead of embedding the SQL in the dag file
    # By default, parameters are not templateable but we extended the class to make that field templateable
    fetch_data = CustomPostgresOperator(
        task_id="fetch_data",
        sql="sql/my_request.sql",  # will pull file and based on docs, it will be templated cause of the .sql extension.
        parameters={
            "next_ds": "{{ next_ds }}",
            "prev_ds": "{{ prev_ds }}",
            "path": "{{ var.json.dag_authoring_01_environment_variable.path  }}",
        },
    )

    # Since we don't have a connection this DAG will fail, so we don't define the order
    # extract >> fetch_data >> process >> verify
