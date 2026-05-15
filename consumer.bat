@echo off

echo Running Spark Streaming job...

docker exec spark-master /opt/spark/bin/spark-submit ^
--master spark://spark-master:7077 ^
--executor-memory 1G ^
--total-executor-cores 1 ^
--conf spark.executor.instances=1 ^
--conf spark.executor.cores=1 ^
/opt/spark/jobs/kafka_streaming.py

if %ERRORLEVEL% NEQ 0 (
    echo Spark job failed with exit code %ERRORLEVEL%
) else (
    echo Spark job completed successfully
)

pause