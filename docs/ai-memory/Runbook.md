# Runbook

## Setup
```bash
cp .env.example .env
make up
```

## Default JSON CDC Lakehouse Path
```bash
make register
make spark-job
make stream-data
```

## Schema Registry / Avro Demo Path
Use this path when demonstrating schema subjects and source schema evolution.
Do not run the JSON and Avro connectors at the same time because they publish to the same topic names with different serialization formats.

```bash
make up
make register-avro
make stream-data
make schema-evolution
make schema-subjects
```

## Run Spark Streaming Job Manually
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
  /app/spark_streaming.py
```

## Useful Make Targets
```bash
make up
make down
make logs
make register
make register-avro
make spark-job
make stream-data
make schema-evolution
make schema-subjects
make test
```

## Validation Notes
- `python -m py_compile pipeline_config.py spark_streaming.py data_generator.py scripts/schema_evolution_demo.py` validates Python syntax.
- `make test` requires `pytest` and `pyspark` in the active Python environment.
