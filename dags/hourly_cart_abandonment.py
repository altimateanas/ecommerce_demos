"""
hourly_cart_abandonment — Hourly cart abandonment detection and recovery triggers

Orchestrates dbt models for CartWave ecommerce analytics.
"""
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)


def on_failure_callback(context):
    """Log detailed failure information for debugging."""
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')

    logger.error(
        "Task FAILED — DAG: %s | Task: %s | Execution: %s | Error: %s",
        dag_id,
        task_id,
        execution_date,
        str(exception),
    )


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': false,
    'email': ['data-alerts@cartwave.demo'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': on_failure_callback,
}


with DAG(
    dag_id='hourly_cart_abandonment',
    default_args=default_args,
    description='Hourly cart abandonment detection and recovery triggers',
    schedule_interval='0 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=false,
    max_active_runs=1,
    tags=['dbt', 'ecommerce'],
) as dag:

    # -------------------------------------------------------------------------
    # dbt tasks
    # -------------------------------------------------------------------------
    dbt_project_dir = '/opt/airflow/dbt'
    dbt_target = 'dev'


    # -------------------------------------------------------------------------
    # Task dependencies
    # -------------------------------------------------------------------------
