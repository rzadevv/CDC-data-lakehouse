"""
Spark streaming job that reads CDC events from Kafka and writes to Iceberg.
Uses MERGE INTO to handle inserts, updates, and deletes.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, LongType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Set up Spark with Iceberg and MinIO."""
    return SparkSession.builder \
        .appName("CDC-Lakehouse-Ingestion") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", "s3a://lakehouse/warehouse") \
        .config("spark.sql.defaultCatalog", "my_catalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()


def create_iceberg_table_if_not_exists(spark):
    """Create target table if missing."""
    logger.info("Creating Iceberg table if not exists...")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS my_catalog.db.financial_transactions (
            transaction_id INT,
            user_id INT,
            transaction_amount DOUBLE,
            transaction_type STRING
        )
        USING iceberg
    """)
    logger.info("Iceberg table created/verified successfully")


def get_debezium_schema():
    """Schema to parse Debezium JSON messages."""
    record_schema = StructType([
        StructField("transaction_id", IntegerType()),
        StructField("user_id", IntegerType()),
        StructField("transaction_amount", DoubleType()),
        StructField("transaction_type", StringType())
    ])
    
    # Debezium wraps data in a payload with before/after states
    return StructType([
        StructField("payload", StructType([
            StructField("before", record_schema),
            StructField("after", record_schema),
            StructField("op", StringType()),  # c=create, u=update, d=delete
            StructField("ts_ms", LongType())
        ]))
    ])


def process_cdc_batch(batch_df, batch_id, spark):
    """Process each micro-batch: parse CDC events and merge into Iceberg."""
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: Empty batch, skipping")
        return
    
    logger.info(f"Batch {batch_id}: Processing {batch_df.count()} records")
    
    # Parse JSON from Kafka
    schema = get_debezium_schema()
    parsed_df = batch_df \
        .selectExpr("CAST(value AS STRING) as value") \
        .withColumn("data", from_json("value", schema)) \
        .select(
            col("data.payload.op").alias("op"),
            col("data.payload.before").alias("before"),
            col("data.payload.after").alias("after")
        )
    
    # Get inserts and updates (use 'after' values)
    upserts_df = parsed_df.filter(col("op").isin("c", "u", "r")) \
        .select(
            col("after.transaction_id").alias("transaction_id"),
            col("after.user_id").alias("user_id"),
            col("after.transaction_amount").alias("transaction_amount"),
            col("after.transaction_type").alias("transaction_type")
        ) \
        .filter(col("transaction_id").isNotNull()) \
        .dropDuplicates(["transaction_id"])
    
    # Get deletes (use 'before' values)
    deletes_df = parsed_df.filter(col("op") == "d") \
        .select(col("before.transaction_id").alias("transaction_id")) \
        .filter(col("transaction_id").isNotNull()) \
        .dropDuplicates(["transaction_id"])
    
    # Run MERGE for upserts
    if upserts_df.count() > 0:
        logger.info(f"Batch {batch_id}: Processing {upserts_df.count()} upserts")
        
        upserts_df.createOrReplaceGlobalTempView("cdc_updates")
        
        spark.sql("""
            MERGE INTO my_catalog.db.financial_transactions t
            USING global_temp.cdc_updates s
            ON t.transaction_id = s.transaction_id
            WHEN MATCHED THEN UPDATE SET
                t.user_id = s.user_id,
                t.transaction_amount = s.transaction_amount,
                t.transaction_type = s.transaction_type
            WHEN NOT MATCHED THEN INSERT
                (transaction_id, user_id, transaction_amount, transaction_type)
                VALUES (s.transaction_id, s.user_id, s.transaction_amount, s.transaction_type)
        """)
        logger.info(f"Batch {batch_id}: Upserts completed")
    
    # Run DELETE
    if deletes_df.count() > 0:
        logger.info(f"Batch {batch_id}: Processing {deletes_df.count()} deletes")
        
        deletes_df.createOrReplaceGlobalTempView("cdc_deletes")
        
        spark.sql("""
            DELETE FROM my_catalog.db.financial_transactions t
            WHERE t.transaction_id IN (SELECT transaction_id FROM global_temp.cdc_deletes)
        """)
        logger.info(f"Batch {batch_id}: Deletes completed")
    
    logger.info(f"Batch {batch_id}: Processing complete")


def main():
    """Start the streaming job."""
    spark = create_spark_session()
    logger.info("Spark Session Started Successfully")
    
    create_iceberg_table_if_not_exists(spark)
    
    # Read CDC events from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", "postgres_server.public.financial_transactions") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Process each batch with our merge logic
    query = kafka_df.writeStream \
        .foreachBatch(lambda df, id: process_cdc_batch(df, id, spark)) \
        .trigger(processingTime="10 seconds") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/financial_transactions") \
        .start()
    
    logger.info("Streaming Query Started with MERGE INTO support...")
    query.awaitTermination()


if __name__ == "__main__":
    main()