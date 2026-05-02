from airflow.providers.smtp.operators.smtp import EmailOperator
import os

ALERT_EMAIL = [os.environ.get("ALERT_EMAIL")]

# Failure callback
def task_failure_notification(context):
    """Send an email whenever any task in the DAG fails."""
    task_instance = context.get("task_instance")
    dag_id        = context.get("dag").dag_id
    task_id       = task_instance.task_id
    exec_date     = context.get("execution_date")
    log_url       = task_instance.log_url

    send_failure_email = EmailOperator(
        task_id="failure_email",
        to=ALERT_EMAIL,
        subject=f"BeejanRide DAG Failed: {dag_id}",
        html_content=f"""
            <h3>Pipeline Failure Alert</h3>
            <p><b>DAG:</b> {dag_id}</p>
            <p><b>Failed Task:</b> {task_id}</p>
            <p><b>Execution Date:</b> {exec_date}</p>
            <p><b>Logs:</b> <a href="{log_url}">Click here</a></p>
            <p>Please investigate immediately.</p>
            <p><b>The Data Engineering Wizard</b></p>
        """,
        conn_id="smtp",
    )
    send_failure_email.execute(context=context)