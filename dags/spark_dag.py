from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1)
    }
) as dag:

    heartbeat = BashOperator(
        task_id="heartbeat",
        bash_command='echo "Pipeline running at $(date)"'
    )

    check_producer = BashOperator(
        task_id="check_producer",
        bash_command="""
        if docker ps | grep -q test-producer; then
            echo "Producer is running"
        else
            echo "Producer is NOT running"
            exit 1
        fi
        """
    )

    check_kafka = BashOperator(
    task_id="check_kafka",
    bash_command="""
    docker exec kafka-1 kafka-topics \
    --bootstrap-server kafka-1:9092 \
    --describe \
    --topic crypto-prices
    """
)
    run_analytics = BashOperator(
        task_id="run_spark_analytics",
        bash_command="""
        docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        --executor-memory 1G \
        --total-executor-cores 1 \
        /opt/spark/jobs/analytics.py
        """
    )

    heartbeat >> check_producer >> check_kafka >> run_analytics