-- Run in Spark SQL or Trino after the Iceberg REST catalog is up.
SELECT table_name, error_reason, COUNT(*) AS bad_event_count
FROM bronze.dead_letter_events
GROUP BY table_name, error_reason
ORDER BY bad_event_count DESC;

SELECT topic, kafka_partition, kafka_offset, error_reason, value_json
FROM bronze.dead_letter_events
ORDER BY ingest_ts DESC
LIMIT 25;
