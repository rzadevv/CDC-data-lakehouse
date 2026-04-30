"""
Config-driven Spark streaming job for a CDC lakehouse.

Pipeline layers:
- Bronze: raw Debezium Kafka events, one table per source table.
- Silver: current-state Iceberg tables maintained with MERGE/DELETE.
- Gold: analytics aggregates rebuilt from Silver after each micro-batch.
"""
import logging
import os
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat_ws, current_timestamp, from_json, lit, row_number, when
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

from pipeline_config import (
    BRONZE_NAMESPACE,
    CDC_TABLES,
    GOLD_NAMESPACE,
    ICEBERG_CATALOG,
    SILVER_NAMESPACE,
    topic_pattern,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_SPARK_TYPES = {
    "int": IntegerType,
    "double": DoubleType,
    "string": StringType,
    "long": LongType,
}


def q(identifier: str) -> str:
    """Quote an identifier for Spark SQL."""
    return f"`{identifier}`"


def qualified(namespace: str, table_name: str) -> str:
    return f"{ICEBERG_CATALOG}.{namespace}.{table_name}"


def create_spark_session() -> SparkSession:
    """Set up Spark with Iceberg and MinIO."""
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "admin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "password")
    catalog_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    return (
        SparkSession.builder.appName("CDC-Lakehouse-Ingestion")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.uri", catalog_uri)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", "s3://lakehouse/warehouse")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint", minio_endpoint)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.access-key-id", minio_access_key)
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.secret-access-key", minio_secret_key)
        .config("spark.sql.defaultCatalog", ICEBERG_CATALOG)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def spark_struct(columns: List[Dict[str, str]]) -> StructType:
    """Build a Spark schema from a configured table column list."""
    fields = []
    for column in columns:
        type_factory = _SPARK_TYPES[column["spark_type"]]
        fields.append(StructField(column["name"], type_factory(), True))
    return StructType(fields)


def get_debezium_schema(table_config: Dict[str, object]) -> StructType:
    """Schema to parse Debezium JSON messages with schema-enabled JsonConverter."""
    record_schema = spark_struct(table_config["columns"])
    return StructType(
        [
            StructField(
                "payload",
                StructType(
                    [
                        StructField("before", record_schema, True),
                        StructField("after", record_schema, True),
                        StructField("op", StringType(), True),
                        StructField("ts_ms", LongType(), True),
                    ]
                ),
                True,
            )
        ]
    )


def create_lakehouse_tables(spark: SparkSession) -> None:
    """Create Bronze, Silver, and Gold namespaces/tables if missing."""
    for namespace in [BRONZE_NAMESPACE, SILVER_NAMESPACE, GOLD_NAMESPACE]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{namespace}")

    for table_name, config in CDC_TABLES.items():
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified(BRONZE_NAMESPACE, table_name + '_cdc_events')} (
                event_id STRING,
                topic STRING,
                table_name STRING,
                operation STRING,
                event_ts_ms BIGINT,
                ingest_ts TIMESTAMP,
                kafka_partition INT,
                kafka_offset BIGINT,
                key_json STRING,
                value_json STRING,
                batch_id BIGINT
            ) USING iceberg
            """
        )

        business_columns = ",\n                ".join(
            f"{q(column['name'])} {column['sql_type']}" for column in config["columns"]
        )
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {qualified(SILVER_NAMESPACE, table_name)} (
                {business_columns},
                _cdc_operation STRING,
                _cdc_event_ts_ms BIGINT,
                _ingested_at TIMESTAMP
            ) USING iceberg
            """
        )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified(BRONZE_NAMESPACE, 'dead_letter_events')} (
            event_id STRING,
            topic STRING,
            table_name STRING,
            error_reason STRING,
            ingest_ts TIMESTAMP,
            kafka_partition INT,
            kafka_offset BIGINT,
            key_json STRING,
            value_json STRING,
            batch_id BIGINT
        ) USING iceberg
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified(GOLD_NAMESPACE, 'daily_transaction_summary')} (
            transaction_date DATE,
            transaction_type STRING,
            transaction_status STRING,
            transaction_count BIGINT,
            total_amount DOUBLE,
            avg_amount DOUBLE
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified(GOLD_NAMESPACE, 'user_account_summary')} (
            user_id INT,
            full_name STRING,
            status STRING,
            account_count BIGINT,
            total_balance DOUBLE,
            transaction_count BIGINT,
            total_transaction_amount DOUBLE
        ) USING iceberg
        """
    )


def parse_table_events(batch_df: DataFrame, table_name: str, config: Dict[str, object]) -> DataFrame:
    """Filter one table topic and parse its Debezium envelope."""
    schema = get_debezium_schema(config)
    return (
        batch_df.filter(col("topic") == lit(config["topic"]))
        .select(
            col("topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("key").cast("string").alias("key_json"),
            col("value").cast("string").alias("value_json"),
        )
        .withColumn("data", from_json("value_json", schema))
        .select(
            "topic",
            "kafka_partition",
            "kafka_offset",
            "key_json",
            "value_json",
            col("data.payload.op").alias("op"),
            col("data.payload.ts_ms").alias("event_ts_ms"),
            col("data.payload.before").alias("before"),
            col("data.payload.after").alias("after"),
        )
    )


def append_bronze(parsed_df: DataFrame, table_name: str, batch_id: int) -> None:
    """Append raw CDC events to the Bronze layer."""
    bronze_df = parsed_df.select(
        concat_ws(":", col("topic"), col("kafka_partition"), col("kafka_offset")).alias("event_id"),
        col("topic"),
        lit(table_name).alias("table_name"),
        col("op").alias("operation"),
        col("event_ts_ms"),
        current_timestamp().alias("ingest_ts"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("key_json"),
        col("value_json"),
        lit(batch_id).cast("long").alias("batch_id"),
    )
    bronze_df.writeTo(qualified(BRONZE_NAMESPACE, table_name + "_cdc_events")).append()


def append_dead_letters(parsed_df: DataFrame, table_name: str, batch_id: int) -> int:
    """Persist records Spark could not parse as valid Debezium CDC events."""
    invalid_df = parsed_df.filter(col("op").isNull())
    invalid_count = invalid_df.count()
    if invalid_count == 0:
        return 0

    dlq_df = invalid_df.select(
        concat_ws(":", col("topic"), col("kafka_partition"), col("kafka_offset")).alias("event_id"),
        col("topic"),
        lit(table_name).alias("table_name"),
        lit("missing_or_invalid_debezium_payload").alias("error_reason"),
        current_timestamp().alias("ingest_ts"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("key_json"),
        col("value_json"),
        lit(batch_id).cast("long").alias("batch_id"),
    )
    dlq_df.writeTo(qualified(BRONZE_NAMESPACE, "dead_letter_events")).append()
    return invalid_count


def latest_changes(parsed_df: DataFrame, config: Dict[str, object]) -> DataFrame:
    """Keep the latest CDC event per primary key within the current micro-batch."""
    pk = config["primary_key"]
    selected_columns = [
        when(col("op") == "d", col(f"before.{column['name']}"))
        .otherwise(col(f"after.{column['name']}"))
        .alias(column["name"])
        for column in config["columns"]
    ]
    changes = parsed_df.select(col("op"), col("event_ts_ms"), *selected_columns).filter(col(pk).isNotNull())
    window = Window.partitionBy(pk).orderBy(col("event_ts_ms").desc_nulls_last())
    return changes.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")


def merge_silver(spark: SparkSession, latest_df: DataFrame, table_name: str, config: Dict[str, object]) -> None:
    """Apply upserts and deletes to the Silver current-state table."""
    pk = config["primary_key"]
    column_names = [column["name"] for column in config["columns"]]

    upserts_df = latest_df.filter(col("op").isin("c", "u", "r")).select(
        *[col(name) for name in column_names],
        col("op").alias("_cdc_operation"),
        col("event_ts_ms").alias("_cdc_event_ts_ms"),
        current_timestamp().alias("_ingested_at"),
    )
    deletes_df = latest_df.filter(col("op") == "d").select(col(pk))

    if not upserts_df.isEmpty():
        view_name = f"{table_name}_cdc_upserts"
        upserts_df.createOrReplaceGlobalTempView(view_name)
        update_assignments = ",\n                ".join(
            f"t.{q(name)} = s.{q(name)}" for name in column_names if name != pk
        )
        if update_assignments:
            update_assignments += ",\n                "
        update_assignments += (
            "t._cdc_operation = s._cdc_operation,\n"
            "                t._cdc_event_ts_ms = s._cdc_event_ts_ms,\n"
            "                t._ingested_at = s._ingested_at"
        )
        metadata_columns = ["_cdc_operation", "_cdc_event_ts_ms", "_ingested_at"]
        insert_columns = ", ".join([q(name) for name in column_names] + metadata_columns)
        insert_values = ", ".join(
            [f"s.{q(name)}" for name in column_names] + [f"s.{name}" for name in metadata_columns]
        )
        spark.sql(
            f"""
            MERGE INTO {qualified(SILVER_NAMESPACE, table_name)} t
            USING global_temp.{view_name} s
            ON t.{q(pk)} = s.{q(pk)}
            WHEN MATCHED THEN UPDATE SET
                {update_assignments}
            WHEN NOT MATCHED THEN INSERT ({insert_columns})
            VALUES ({insert_values})
            """
        )

    if not deletes_df.isEmpty():
        view_name = f"{table_name}_cdc_deletes"
        deletes_df.createOrReplaceGlobalTempView(view_name)
        spark.sql(
            f"""
            DELETE FROM {qualified(SILVER_NAMESPACE, table_name)} t
            WHERE t.{q(pk)} IN (SELECT {q(pk)} FROM global_temp.{view_name})
            """
        )


def refresh_gold_tables(spark: SparkSession) -> None:
    """Rebuild Gold aggregates from Silver current-state tables."""
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {qualified(GOLD_NAMESPACE, 'daily_transaction_summary')}
        USING iceberg AS
        SELECT
            COALESCE(to_date({q('timestamp')}), current_date()) AS transaction_date,
            transaction_type,
            transaction_status,
            COUNT(*) AS transaction_count,
            SUM(transaction_amount) AS total_amount,
            AVG(transaction_amount) AS avg_amount
        FROM {qualified(SILVER_NAMESPACE, 'financial_transactions')}
        GROUP BY COALESCE(to_date({q('timestamp')}), current_date()), transaction_type, transaction_status
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {qualified(GOLD_NAMESPACE, 'user_account_summary')}
        USING iceberg AS
        WITH account_rollup AS (
            SELECT
                user_id,
                COUNT(*) AS account_count,
                SUM(balance) AS total_balance
            FROM {qualified(SILVER_NAMESPACE, 'accounts')}
            GROUP BY user_id
        ),
        transaction_rollup AS (
            SELECT
                user_id,
                COUNT(*) AS transaction_count,
                SUM(transaction_amount) AS total_transaction_amount
            FROM {qualified(SILVER_NAMESPACE, 'financial_transactions')}
            GROUP BY user_id
        )
        SELECT
            u.user_id,
            u.full_name,
            u.status,
            COALESCE(a.account_count, 0) AS account_count,
            COALESCE(a.total_balance, 0.0D) AS total_balance,
            COALESCE(t.transaction_count, 0) AS transaction_count,
            COALESCE(t.total_transaction_amount, 0.0D) AS total_transaction_amount
        FROM {qualified(SILVER_NAMESPACE, 'users')} u
        LEFT JOIN account_rollup a ON u.user_id = a.user_id
        LEFT JOIN transaction_rollup t ON u.user_id = t.user_id
        """
    )


def process_cdc_batch(batch_df: DataFrame, batch_id: int, spark: SparkSession) -> None:
    """Process each micro-batch across every configured CDC table."""
    if batch_df.isEmpty():
        logger.info("Batch %s: empty batch, skipping", batch_id)
        return

    logger.info("Batch %s: processing multi-table CDC events", batch_id)
    processed_tables = 0

    for table_name, config in CDC_TABLES.items():
        table_events_df = parse_table_events(batch_df, table_name, config)
        if table_events_df.isEmpty():
            continue

        invalid_count = append_dead_letters(table_events_df, table_name, batch_id)
        parsed_df = table_events_df.filter(col("op").isNotNull())
        if parsed_df.isEmpty():
            logger.warning("Batch %s: %s invalid events written to DLQ for %s", batch_id, invalid_count, table_name)
            continue

        event_count = parsed_df.count()
        append_bronze(parsed_df, table_name, batch_id)
        latest_df = latest_changes(parsed_df, config)
        merge_silver(spark, latest_df, table_name, config)
        processed_tables += 1
        logger.info(
            "Batch %s: %s valid and %s invalid events processed for %s",
            batch_id,
            event_count,
            invalid_count,
            table_name,
        )

    if processed_tables:
        refresh_gold_tables(spark)
        logger.info("Batch %s: Gold tables refreshed", batch_id)


def main() -> None:
    """Start the streaming job."""
    spark = create_spark_session()
    logger.info("Spark session started")
    create_lakehouse_tables(spark)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribePattern", topic_pattern())
        .option("startingOffsets", os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
        .load()
    )

    query = (
        kafka_df.writeStream.foreachBatch(lambda df, batch_id: process_cdc_batch(df, batch_id, spark))
        .trigger(processingTime="10 seconds")
        .option(
            "checkpointLocation",
            os.getenv("SPARK_CHECKPOINT_LOCATION", "s3a://lakehouse/checkpoints/multi_table_cdc"),
        )
        .start()
    )

    logger.info("Streaming query started for topics: %s", topic_pattern())
    query.awaitTermination()


if __name__ == "__main__":
    main()
