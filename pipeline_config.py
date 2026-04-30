"""Shared CDC table configuration for the lakehouse pipeline."""

CDC_SERVER_NAME = "postgres_server"
KAFKA_TOPIC_PREFIX = f"{CDC_SERVER_NAME}.public"
ICEBERG_CATALOG = "my_catalog"
BRONZE_NAMESPACE = "bronze"
SILVER_NAMESPACE = "silver"
GOLD_NAMESPACE = "gold"

# spark_type is mapped in spark_streaming.py; sql_type is used for Iceberg DDL.
CDC_TABLES = {
    "users": {
        "primary_key": "user_id",
        "topic": f"{KAFKA_TOPIC_PREFIX}.users",
        "columns": [
            {"name": "user_id", "spark_type": "int", "sql_type": "INT"},
            {"name": "full_name", "spark_type": "string", "sql_type": "STRING"},
            {"name": "email", "spark_type": "string", "sql_type": "STRING"},
            {"name": "status", "spark_type": "string", "sql_type": "STRING"},
            {"name": "created_at", "spark_type": "string", "sql_type": "STRING"},
            {"name": "updated_at", "spark_type": "string", "sql_type": "STRING"},
        ],
    },
    "merchants": {
        "primary_key": "merchant_id",
        "topic": f"{KAFKA_TOPIC_PREFIX}.merchants",
        "columns": [
            {"name": "merchant_id", "spark_type": "int", "sql_type": "INT"},
            {"name": "merchant_name", "spark_type": "string", "sql_type": "STRING"},
            {"name": "category", "spark_type": "string", "sql_type": "STRING"},
            {"name": "country", "spark_type": "string", "sql_type": "STRING"},
            {"name": "updated_at", "spark_type": "string", "sql_type": "STRING"},
        ],
    },
    "accounts": {
        "primary_key": "account_id",
        "topic": f"{KAFKA_TOPIC_PREFIX}.accounts",
        "columns": [
            {"name": "account_id", "spark_type": "int", "sql_type": "INT"},
            {"name": "user_id", "spark_type": "int", "sql_type": "INT"},
            {"name": "account_type", "spark_type": "string", "sql_type": "STRING"},
            {"name": "balance", "spark_type": "double", "sql_type": "DOUBLE"},
            {"name": "status", "spark_type": "string", "sql_type": "STRING"},
            {"name": "updated_at", "spark_type": "string", "sql_type": "STRING"},
        ],
    },
    "financial_transactions": {
        "primary_key": "transaction_id",
        "topic": f"{KAFKA_TOPIC_PREFIX}.financial_transactions",
        "columns": [
            {"name": "transaction_id", "spark_type": "int", "sql_type": "INT"},
            {"name": "account_id", "spark_type": "int", "sql_type": "INT"},
            {"name": "user_id", "spark_type": "int", "sql_type": "INT"},
            {"name": "merchant_id", "spark_type": "int", "sql_type": "INT"},
            {"name": "transaction_amount", "spark_type": "double", "sql_type": "DOUBLE"},
            {"name": "transaction_type", "spark_type": "string", "sql_type": "STRING"},
            {"name": "transaction_status", "spark_type": "string", "sql_type": "STRING"},
            {"name": "timestamp", "spark_type": "string", "sql_type": "STRING"},
            # Added by scripts/schema_evolution_demo.py. Older events parse this as null.
            {"name": "risk_score", "spark_type": "double", "sql_type": "DOUBLE"},
        ],
    },
}


def topic_pattern():
    """Kafka subscribePattern for all configured Debezium table topics."""
    tables = "|".join(CDC_TABLES.keys())
    return rf"{KAFKA_TOPIC_PREFIX}\.({tables})"


def table_names():
    """Return configured source table names in deterministic processing order."""
    return list(CDC_TABLES.keys())
