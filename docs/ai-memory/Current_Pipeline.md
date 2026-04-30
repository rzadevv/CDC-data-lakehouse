# Current Pipeline

## Active Flow
1. `postgres` receives row-level writes for `users`, `merchants`, `accounts`, and `financial_transactions`.
2. Debezium connector publishes CDC events to per-table Kafka topics.
3. Spark job (`spark_streaming.py`) subscribes to the configured topic pattern from `pipeline_config.py`.
4. Each micro-batch writes raw events to Bronze Iceberg tables.
5. `latest_changes()` keeps the latest event per primary key in the batch.
6. `merge_silver()` applies upserts/deletes into Silver current-state Iceberg tables.
7. `refresh_gold_tables()` rebuilds Gold aggregates from Silver.

## Lakehouse Layers
- Bronze: `bronze.<table>_cdc_events` raw Debezium event history plus Kafka metadata.
- Silver: `silver.<table>` current-state tables maintained with CDC upsert/delete semantics.
- Gold: `gold.daily_transaction_summary`, `gold.user_account_summary`.

## Main Implementation Files
- `pipeline_config.py`: CDC table metadata, topics, primary keys, schemas, and topic pattern.
- `spark_streaming.py`: config-driven Bronze/Silver/Gold Spark Structured Streaming job.
- `data_generator.py`: synthetic multi-table workload generator.
- `debezium-connector.json`: JSON connector for the default Spark ingestion path.
- `debezium-avro-connector.json`: Avro connector for Schema Registry/schema evolution demo.
- `scripts/schema_evolution_demo.py`: adds `risk_score`, inserts evolved transaction, prints registry subjects.
- `docker-compose.yaml`: local service topology including Schema Registry and custom Connect image.
- `tests/test_transformations.py`: config, Debezium parsing, and latest-change logic tests.

## Current Graphify Snapshot
- Source: `graphify-out/GRAPH_REPORT.md` (generated 2026-04-30 before the multi-table upgrade).
- Previous graph summary: 55 nodes, 61 edges, 11 communities.
- Refresh Graphify after the new implementation stabilizes.
