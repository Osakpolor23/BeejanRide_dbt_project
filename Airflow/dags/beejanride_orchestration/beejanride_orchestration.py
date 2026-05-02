from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import datetime, duration
from beejanride_orchestration.callbacks import task_failure_notification
import os

# Define variables
PROJECT_DIR = "/opt/airflow/beejanride_dbt_project"
PROFILES_DIR  = "/home/airflow/.dbt"
ALERT_EMAIL   = [os.environ.get("ALERT_EMAIL")]

# Default args to be passed to my DAG
args = {
    "owner"           : "data-engineering",
    "email"           : ALERT_EMAIL,
    "email_on_retry"  : False,
    "retries"         : 3,
    "retry_delay"     : duration(seconds=15),   # delay my retry for 15 seconds
    "retry_exponential_backoff": True,          # Increase the number of wait period after after retries exponentially
    "max_retry_delay": duration(hours=2),
    "on_failure_callback": task_failure_notification,
}

# DAG definition
with DAG(
    dag_id="beejanride_orchestration",
    description="This is an End-to-end ELT Airflow orchestration Project: Airbyte → dbt transformation → BigQuery",
    tags=["beejanride", "analytics"],
    start_date=datetime(2026, 4, 30),
    end_date=datetime(2026, 5, 20),
    max_active_runs=1,      # Only one of my task is allowed to run at a particular time
    schedule="0 1 * * *",   # Automated to run at 1 am daily
    catchup=True,           # To enable backfill in case of missed dag runs
    default_args=args,
    doc_md="""
    ## BeejanRide Orchestration DAG
    This DaG is meant to run the full ELT daily at 1 am with the following steps in place:
    1. **Airbyte** syncs raw data from sources into BigQuery on GCP using the Airbyte Cloud
    2. **dbt staging** cleans and standardises raw data
    3. **dbt intermediate** applies business logic
    4. **dbt marts** builds final analytical models
    5. Each layer is tested before the next one runs
    6. A success email is sent on completion
    7. A failure email is sent if any task fails

    ### Idempotency
    - max_active_runs=1 was set to prevents concurrent runs mutating the same tables
    """,
) as dag:
    
    start = EmptyOperator(task_id="start")

    # @task(retries=2)
    # def raise_error():
    #     raise KeyError

    # 1. Airbyte ingestion
    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync",
        airbyte_conn_id="airbyte_conn",
        connection_id="c986a7c0-a35d-42e5-ac8e-16e4c04ec147",
    )

    # 2. Staging Layer
    dbt_run_staging = BashOperator(
    task_id="dbt_run_staging",
    bash_command= f"dbt run --select staging --project-dir {PROJECT_DIR}",
    trigger_rule="none_failed"
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command= f"dbt test --select staging --project-dir {PROJECT_DIR}",
        trigger_rule="none_failed"
    )

    # 3. Intermediate
    dbt_run_intermediate = BashOperator(
    task_id="dbt_run_intermediate",
    bash_command= f"dbt run --select intermediate --project-dir {PROJECT_DIR}",
    trigger_rule="none_failed"
    )

    dbt_test_intermediate = BashOperator(
        task_id="dbt_test_intermediate",
        bash_command= f"dbt test --select intermediate --project-dir {PROJECT_DIR}",
        trigger_rule="none_failed"
    )

    # 4 Marts Layer
    dbt_run_marts = BashOperator(
    task_id="dbt_run_marts",
    bash_command= f"dbt run --select marts --project-dir {PROJECT_DIR}",
    trigger_rule="none_failed"
    )

    # 5. Successfull Email Notification
    send_notification = EmailOperator(
    task_id="send_success_email",
    trigger_rule="none_failed",
    to=ALERT_EMAIL,
    subject="Notification For Successful BeejanRide Analytics Orchestration",
    html_content="""
        <h3> BeejanRide Analytics ETL Successful</h3>
        <p> Dear team, the ETL for BeejanRide Analytics has successfuly completed and the models have been persisted on Bigquery </p>
        <p> Best Regards,</p>
        <p><b> The Data Engineering Wizard </b></p>
    """,
    conn_id="smtp"
    )
 
    # Define dependencies
    (
    start 
    >> airbyte_sync 
    >> dbt_run_staging 
    >> dbt_test_staging
    >> dbt_run_intermediate
    >> dbt_test_intermediate
    >> dbt_run_marts 
    >> send_notification
    )
    
    # >> raise_error()

