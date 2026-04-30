#!/usr/bin/env bash
set -euo pipefail

CHECKPOINT=${1:-s3a://lakehouse/checkpoints/replay-$(date +%Y%m%d%H%M%S)}

docker exec -it \
  -e KAFKA_STARTING_OFFSETS=earliest \
  -e SPARK_CHECKPOINT_LOCATION="${CHECKPOINT}" \
  spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
  /app/spark_streaming.py
