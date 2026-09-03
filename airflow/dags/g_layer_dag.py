from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="batch_features",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 9, 2),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    
    run_volatility = BashOperator(
        task_id="g_volatility",
        bash_command=(
            "cd ~/crypto-pipeline && PYTHONPATH=~/crypto-pipeline spark-submit "
            "--master local[1] "
            "--conf spark.driver.cores=1 "
            "--conf spark.sql.shuffle.partitions=2 "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0,"
            "io.delta:delta-spark_4.1_2.13:4.3.0,"
            "org.apache.hadoop:hadoop-aws:3.4.1 "
            "spark/gold/g_volatility.py"
        ),
        execution_timeout=timedelta(minutes=5),
    )
    