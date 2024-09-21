from datetime import timedelta, datetime

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    default_args={
        "owner": "nqh",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    dag_id="machine_learning_pipeline",
    schedule=None,
    catchup=False,
    start_date=datetime.today(),
) as dag:
    task1 = EmptyOperator(task_id="task1", dag=dag)