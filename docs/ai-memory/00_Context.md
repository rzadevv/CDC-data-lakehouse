# CDC Data Lakehouse - Context

## Scope
- Real-time multi-table CDC pipeline from PostgreSQL into an Iceberg-based lakehouse.
- Primary flow: PostgreSQL WAL -> Debezium Connect -> Kafka -> Spark Structured Streaming -> Iceberg on MinIO.
- Lakehouse modeling uses Bronze raw CDC, Silver current-state tables, and Gold analytics aggregates.

## Key Constraints
- Keep repo exploration focused; use Graphify and targeted file reads first.
- Trino is present but currently limited for this setup (`iceberg.catalog.type=hadoop` compatibility issue per README).
- Default Spark ingestion path uses JSON Debezium messages with schemas enabled.
- Avro connector path is available for Schema Registry and schema evolution demos.

## Core Components
- Source DB: PostgreSQL (`docker-compose.yaml`)
- CDC capture: Debezium Connect service `connect`
- Schema management demo: Confluent Schema Registry service `schema-registry`
- Transport: Kafka broker `broker` (+ Zookeeper)
- Processing: `spark_streaming.py`
- Shared table metadata: `pipeline_config.py`
- Sink/storage: Iceberg tables in MinIO bucket `lakehouse`
- Change simulator: `data_generator.py`
- Schema evolution demo: `scripts/schema_evolution_demo.py`
- Tests: `tests/test_transformations.py`
