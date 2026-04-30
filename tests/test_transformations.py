"""Tests for the config-driven multi-table CDC transformation layer."""
import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json

from pipeline_config import CDC_TABLES, topic_pattern
from spark_streaming import get_debezium_schema, latest_changes, spark_struct


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.appName("CDC-Tests").master("local[*]").getOrCreate()
    yield session
    session.stop()


class TestPipelineConfig:
    def test_all_tables_have_primary_keys_topics_and_columns(self):
        assert set(CDC_TABLES) == {"users", "merchants", "accounts", "financial_transactions"}

        for table_name, config in CDC_TABLES.items():
            assert config["primary_key"] in [column["name"] for column in config["columns"]]
            assert config["topic"].endswith(f"public.{table_name}")
            assert config["columns"]

    def test_topic_pattern_covers_configured_tables(self):
        pattern = topic_pattern()
        assert "postgres_server" in pattern
        for table_name in CDC_TABLES:
            assert table_name in pattern

    def test_financial_transactions_supports_evolved_risk_score(self):
        columns = [column["name"] for column in CDC_TABLES["financial_transactions"]["columns"]]
        assert "risk_score" in columns


class TestDebeziumParsing:
    def test_parse_user_insert_event(self, spark):
        config = CDC_TABLES["users"]
        event = {
            "payload": {
                "before": None,
                "after": {
                    "user_id": 1,
                    "full_name": "Ada Lovelace",
                    "email": "ada@example.com",
                    "status": "ACTIVE",
                    "created_at": "2026-04-30 10:00:00",
                    "updated_at": "2026-04-30 10:00:00",
                },
                "op": "c",
                "ts_ms": 1777543200000,
            }
        }

        df = spark.createDataFrame([(json.dumps(event),)], ["value"])
        parsed = df.withColumn("data", from_json("value", get_debezium_schema(config)))
        row = parsed.select("data.payload.op", "data.payload.after.user_id", "data.payload.after.email").first()

        assert row["op"] == "c"
        assert row["user_id"] == 1
        assert row["email"] == "ada@example.com"

    def test_parse_evolved_transaction_event(self, spark):
        config = CDC_TABLES["financial_transactions"]
        event = {
            "payload": {
                "before": None,
                "after": {
                    "transaction_id": 10,
                    "account_id": 20,
                    "user_id": 30,
                    "merchant_id": 40,
                    "transaction_amount": 999.99,
                    "transaction_type": "DEBIT",
                    "transaction_status": "REVIEW",
                    "timestamp": "2026-04-30 12:00:00",
                    "risk_score": 87.5,
                },
                "op": "c",
                "ts_ms": 1777550400000,
            }
        }

        df = spark.createDataFrame([(json.dumps(event),)], ["value"])
        parsed = df.withColumn("data", from_json("value", get_debezium_schema(config)))
        row = parsed.select("data.payload.after.transaction_id", "data.payload.after.risk_score").first()

        assert row["transaction_id"] == 10
        assert row["risk_score"] == 87.5


class TestLatestChangeLogic:
    def test_latest_delete_wins_per_primary_key(self, spark):
        config = CDC_TABLES["accounts"]
        record_schema = spark_struct(config["columns"])
        rows = [
            (
                "u",
                1000,
                None,
                {
                    "account_id": 1,
                    "user_id": 7,
                    "account_type": "CHECKING",
                    "balance": 500.0,
                    "status": "OPEN",
                    "updated_at": "t1",
                },
            ),
            (
                "d",
                2000,
                {
                    "account_id": 1,
                    "user_id": 7,
                    "account_type": "CHECKING",
                    "balance": 500.0,
                    "status": "OPEN",
                    "updated_at": "t1",
                },
                None,
            ),
        ]
        schema = (
            "op string, event_ts_ms long, "
            "before struct<account_id:int,user_id:int,account_type:string,"
            "balance:double,status:string,updated_at:string>, "
            "after struct<account_id:int,user_id:int,account_type:string,"
            "balance:double,status:string,updated_at:string>"
        )
        parsed_df = spark.createDataFrame(rows, schema)

        latest = latest_changes(parsed_df, config)
        row = latest.select("op", "account_id").first()

        assert row["op"] == "d"
        assert row["account_id"] == 1
        assert record_schema["balance"].dataType.simpleString() == "double"
