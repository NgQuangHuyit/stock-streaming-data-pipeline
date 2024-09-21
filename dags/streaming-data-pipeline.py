import os
import sys
from datetime import datetime, timedelta
import logging

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from scripts.cassandra_init import cassandra_initalization
from scripts.kafka_topics import create_kafka_topic, logger

DDL_FILE_PATH = os.path.join(os.path.dirname(__file__), "scripts","ddl.cql")
SYMBOLS = ["BINANCE:BTCUSDT"]
BOOTSTRAP_SERVERS = "kafka1:19092"
TOPICS = ["stock", "btc_features"]
CASSANDRA_CLUSTER = 'cassandra'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def check_environment_variables(required_vars):
    logger = logging.getLogger("Environment Variables Check")
    not_set_vars = []
    for var in required_vars:
        if not os.getenv(var):
            not_set_vars.append(var)
            logger.warning(f"{var} is not set")
    if not_set_vars:
        return 'end_dag'
    else:
        return 'continue_dag'

with DAG(
    dag_id="streaming-data-pipeline",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    default_args={
        "owner": "nqh",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:
    environment_check = BranchPythonOperator(
        task_id="environment_check",
        python_callable=check_environment_variables,
        op_kwargs={
            "required_vars": ["FINNHUB_API_KEY",
                              "CASSANDRA_USER",
                              "CASSANDRA_PASSWORD",
                              "MINIO_ACCESS_KEY",
                              "MINIO_SECRET_KEY",
                              "MINIO_ENDPOINT"]
        }
    )

    end_dag = EmptyOperator(task_id="end_dag")

    continue_dag = EmptyOperator(task_id="continue_dag")

    cassandra_init = PythonOperator(
        task_id="cassandra_init",
        python_callable=cassandra_initalization,
        op_kwargs={
            "cluster": CASSANDRA_CLUSTER,
            "cql_file": DDL_FILE_PATH,
            "username": os.getenv("CASSANDRA_USER"),
            "password": os.getenv("CASSANDRA_PASSWORD"),}
    )

    kafka_init = PythonOperator(
        task_id="kafka_init",
        python_callable=create_kafka_topic,
        op_kwargs={
            "kafka_servers": BOOTSTRAP_SERVERS,
            "topics": TOPICS,
        }
    )

    produce_to_kafka = BashOperator(
        task_id="produce_to_kafka",
        bash_command=f"""
            python {os.path.join(os.path.dirname(__file__), "scripts", "FinnhubProducer", "FinnhubProducer.py")} \
            --api_key {os.getenv("FINNHUB_API_KEY")} \
            --topic stock \
            --symbols {",".join(SYMBOLS)} \
            --schema-path {os.path.join(os.path.dirname(__file__), "scripts", "FinnhubProducer", "schemas", "trades.avsc")} \
            --bootstrap-servers {BOOTSTRAP_SERVERS}
        """,
        retries=5,
        retry_delay=timedelta(milliseconds=500)
    )
    start_stream = SparkSubmitOperator(
        task_id="start_stream",
        application=f"{os.path.join(os.path.dirname(__file__), 'scripts', 'StreamProcessing', 'ProcessStockStream.py')}",
        name="Streaming_Job",
        conn_id="spark_default",
        verbose=False,
        deploy_mode="client",
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
                "org.apache.spark:spark-avro_2.12:3.5.2,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                 "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        application_args=[
            '--cassandra-cluster', CASSANDRA_CLUSTER,
            '--cassandra-user', os.getenv("CASSANDRA_USER"),
            '--cassandra-password', os.getenv("CASSANDRA_PASSWORD"),
            '--kafka-bootstrap-servers', BOOTSTRAP_SERVERS,
            '--minio-access-key', os.getenv("MINIO_ACCESS_KEY"),
            '--minio-secret-key', os.getenv("MINIO_SECRET_KEY"),
            '--minio-endpoint', os.getenv("MINIO_ENDPOINT"),
            '--avro-schema-path', os.path.join(os.path.dirname(__file__), "scripts", "StreamProcessing", "schemas", "trades.avsc"),
        ]
    )

    environment_check >> [end_dag, continue_dag]
    continue_dag >> [cassandra_init, kafka_init]
    kafka_init >> produce_to_kafka
    [kafka_init, cassandra_init] >> start_stream
