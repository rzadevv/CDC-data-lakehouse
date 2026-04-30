# Kubernetes Skeleton

These manifests are intentionally minimal. They document the target deployment shape for a portfolio discussion; use Helm charts/operators for real deployments.

Recommended production-grade components:

- Strimzi for Kafka and Kafka Connect
- Spark Operator for streaming jobs
- Trino Helm chart for query serving
- Airflow Helm chart for orchestration
- kube-prometheus-stack for monitoring
- External Secrets Operator for credentials

Apply order for a demo cluster:

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f iceberg-rest.yaml
kubectl apply -f trino.yaml
```
