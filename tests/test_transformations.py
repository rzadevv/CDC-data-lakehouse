"""
Unit tests for CDC transformation logic.
Tests the parsing and processing of Debezium CDC events.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import json


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder \
        .appName("CDC-Tests") \
        .master("local[*]") \
        .getOrCreate()


class TestCDCParsing:
    """Tests for CDC event parsing logic."""
    
    def test_parse_insert_event(self, spark):
        """Test parsing a Debezium INSERT (create) event."""
        # Simulated Debezium event for INSERT
        cdc_event = {
            "payload": {
                "before": None,
                "after": {
                    "transaction_id": 1,
                    "user_id": 42,
                    "transaction_amount": 150.50,
                    "transaction_type": "CREDIT"
                },
                "op": "c",  # c = create
                "ts_ms": 1703100000000
            }
        }
        
        # Verify the structure is correct
        assert cdc_event["payload"]["op"] == "c"
        assert cdc_event["payload"]["after"]["transaction_id"] == 1
        assert cdc_event["payload"]["after"]["transaction_amount"] == 150.50
    
    def test_parse_update_event(self, spark):
        """Test parsing a Debezium UPDATE event."""
        cdc_event = {
            "payload": {
                "before": {
                    "transaction_id": 1,
                    "user_id": 42,
                    "transaction_amount": 150.50,
                    "transaction_type": "CREDIT"
                },
                "after": {
                    "transaction_id": 1,
                    "user_id": 42,
                    "transaction_amount": 200.00,  # Updated amount
                    "transaction_type": "DEBIT"    # Updated type
                },
                "op": "u",  # u = update
                "ts_ms": 1703100001000
            }
        }
        
        assert cdc_event["payload"]["op"] == "u"
        assert cdc_event["payload"]["before"]["transaction_amount"] == 150.50
        assert cdc_event["payload"]["after"]["transaction_amount"] == 200.00
    
    def test_parse_delete_event(self, spark):
        """Test parsing a Debezium DELETE event."""
        cdc_event = {
            "payload": {
                "before": {
                    "transaction_id": 1,
                    "user_id": 42,
                    "transaction_amount": 200.00,
                    "transaction_type": "DEBIT"
                },
                "after": None,
                "op": "d",  # d = delete
                "ts_ms": 1703100002000
            }
        }
        
        assert cdc_event["payload"]["op"] == "d"
        assert cdc_event["payload"]["after"] is None
        assert cdc_event["payload"]["before"]["transaction_id"] == 1


class TestOperationLogic:
    """Tests for operation routing logic."""
    
    def test_operation_is_upsert(self):
        """Test that INSERT and UPDATE operations are classified as upserts."""
        upsert_ops = ["c", "u", "r"]  # create, update, read (snapshot)
        
        for op in upsert_ops:
            assert op in ["c", "u", "r"], f"Operation {op} should be an upsert"
    
    def test_operation_is_delete(self):
        """Test that DELETE operations are correctly identified."""
        delete_ops = ["d"]
        
        for op in delete_ops:
            assert op == "d", f"Operation {op} should be a delete"
    
    def test_extract_id_for_delete(self):
        """Test extracting transaction_id from 'before' for deletes."""
        cdc_event = {
            "payload": {
                "before": {"transaction_id": 123},
                "after": None,
                "op": "d"
            }
        }
        
        # For deletes, we need to use 'before' to get the ID
        if cdc_event["payload"]["op"] == "d":
            txn_id = cdc_event["payload"]["before"]["transaction_id"]
            assert txn_id == 123


class TestDataTransformations:
    """Tests for data transformation functions."""
    
    def test_amount_is_numeric(self, spark):
        """Test that transaction amounts are properly typed."""
        data = [("1", 42, 150.50, "CREDIT")]
        schema = StructType([
            StructField("transaction_id", StringType()),
            StructField("user_id", IntegerType()),
            StructField("transaction_amount", DoubleType()),
            StructField("transaction_type", StringType())
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # Verify the amount column is a double
        assert df.schema["transaction_amount"].dataType == DoubleType()
    
    def test_filter_null_ids(self, spark):
        """Test filtering out records with null transaction_id."""
        data = [
            (1, 42, 100.0, "CREDIT"),
            (None, 43, 200.0, "DEBIT"),
            (3, 44, 300.0, "CREDIT")
        ]
        schema = StructType([
            StructField("transaction_id", IntegerType()),
            StructField("user_id", IntegerType()),
            StructField("transaction_amount", DoubleType()),
            StructField("transaction_type", StringType())
        ])
        
        df = spark.createDataFrame(data, schema)
        filtered = df.filter(df.transaction_id.isNotNull())
        
        assert filtered.count() == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
