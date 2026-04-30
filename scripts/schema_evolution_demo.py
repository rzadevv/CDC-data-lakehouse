"""Demonstrate source schema evolution and Schema Registry visibility.

This script adds `risk_score` to the source transaction table, writes evolved
records, and prints Schema Registry subjects if the Avro connector is active.
"""
import json
import os
import time
import urllib.error
import urllib.request
from decimal import Decimal

import psycopg2

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "mydb"),
    "user": os.getenv("POSTGRES_USER", "user"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
}
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def fetch_json(path: str):
    with urllib.request.urlopen(f"{SCHEMA_REGISTRY_URL}{path}", timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def print_schema_registry_state() -> None:
    try:
        subjects = fetch_json("/subjects")
    except (urllib.error.URLError, TimeoutError) as exc:
        print(f"Schema Registry is not reachable at {SCHEMA_REGISTRY_URL}: {exc}")
        return

    print("Schema Registry subjects:")
    for subject in subjects:
        versions = fetch_json(f"/subjects/{subject}/versions")
        print(f"- {subject}: versions={versions}")


def apply_schema_change(cursor) -> None:
    cursor.execute(
        """
        ALTER TABLE financial_transactions
        ADD COLUMN IF NOT EXISTS risk_score DECIMAL(5, 2);
        """
    )
    print("Applied schema change: financial_transactions.risk_score")


def insert_evolved_transaction(cursor) -> None:
    cursor.execute("SELECT account_id, user_id FROM accounts ORDER BY RANDOM() LIMIT 1;")
    account = cursor.fetchone()
    cursor.execute("SELECT merchant_id FROM merchants ORDER BY RANDOM() LIMIT 1;")
    merchant = cursor.fetchone()
    if not account or not merchant:
        raise RuntimeError("Run data_generator.py first so accounts and merchants exist")

    cursor.execute(
        """
        INSERT INTO financial_transactions (
            account_id,
            user_id,
            merchant_id,
            transaction_amount,
            transaction_type,
            transaction_status,
            risk_score
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING transaction_id;
        """,
        (account[0], account[1], merchant[0], Decimal("999.99"), "DEBIT", "REVIEW", Decimal("87.50")),
    )
    transaction_id = cursor.fetchone()[0]
    print(f"Inserted evolved transaction_id={transaction_id} with risk_score=87.50")


def main() -> None:
    with psycopg2.connect(**DB_CONFIG) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            apply_schema_change(cursor)
            insert_evolved_transaction(cursor)

    print("Waiting briefly for Debezium and Schema Registry updates...")
    time.sleep(5)
    print_schema_registry_state()


if __name__ == "__main__":
    main()
