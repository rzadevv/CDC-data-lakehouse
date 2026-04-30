# Cloud and Kubernetes Deployment Notes

This repo is still optimized for a local Docker Compose portfolio demo. The cloud/Kubernetes path below shows how to scale the same architecture without pretending the local Compose stack is production.

## Target Architecture

- PostgreSQL: managed PostgreSQL with logical replication enabled, or self-managed PostgreSQL on Kubernetes for demos.
- Kafka: managed Kafka/MSK/Confluent Cloud, or Strimzi on Kubernetes.
- Kafka Connect/Debezium: Kafka Connect workers deployed as Kubernetes workloads.
- Schema Registry: Confluent Schema Registry or managed equivalent.
- Spark: Spark Operator on Kubernetes, EMR on EKS, Dataproc, or standalone Spark-on-K8s.
- Iceberg catalog: Iceberg REST catalog or Nessie.
- Object storage: S3/GCS/ADLS in cloud, MinIO only for local demos.
- Query engine: Trino workers using the Iceberg REST catalog.
- Orchestration: Airflow on Kubernetes or managed Airflow.
- Observability: Prometheus/Grafana plus cloud logs.

## Migration Checklist

1. Replace MinIO with cloud object storage.
2. Move secrets from `.env` into Kubernetes Secrets or a cloud secret manager.
3. Replace Docker Compose networking names with service DNS names.
4. Deploy Iceberg REST/Nessie catalog with persistent metadata storage.
5. Configure Spark and Trino to use the same catalog URI and warehouse.
6. Deploy Debezium connector config through CI/CD or a Kafka Connect operator.
7. Add Prometheus scraping for Kafka Connect, Spark, Trino, and custom exporters.
8. Use separate checkpoint paths per environment and replay run.
9. Protect destructive reset/replay commands behind explicit environment checks.

## Kubernetes Mapping

| Compose Service | Kubernetes Equivalent |
|---|---|
| `postgres` | StatefulSet or managed PostgreSQL |
| `broker`, `zookeeper` | Strimzi Kafka cluster or managed Kafka |
| `connect` | Kafka Connect Deployment/Strimzi KafkaConnect |
| `schema-registry` | Deployment + Service |
| `iceberg-rest` | Deployment + Service backed by metadata DB |
| `spark-master`, `spark-worker` | Spark Operator `SparkApplication` or Spark-on-K8s submit |
| `trino` | Trino Helm chart |
| `airflow` | Airflow Helm chart |
| `prometheus`, `grafana` | kube-prometheus-stack |

## Portfolio Talking Points

- The local stack demonstrates the full path end-to-end.
- The Kubernetes plan separates control plane, compute, object storage, and query layers.
- The same Iceberg REST catalog is shared by Spark writers and Trino readers.
- Replay uses a new checkpoint path instead of mutating existing checkpoints.
- DLQ tables preserve malformed events for inspection instead of silently dropping them.
