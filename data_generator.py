"""Generate realistic multi-table CDC traffic for the local Postgres source."""
import logging
import os
import random
import time
from decimal import Decimal
from typing import Callable, Dict, Optional

import psycopg2
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)
fake = Faker()

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "mydb"),
    "user": os.getenv("POSTGRES_USER", "user"),
    "password": os.getenv("POSTGRES_PASSWORD", "password"),
}

OPERATION_WEIGHTS = {
    "insert_user": 8,
    "update_user": 8,
    "insert_merchant": 4,
    "update_merchant": 4,
    "insert_account": 14,
    "update_account": 12,
    "delete_account": 3,
    "insert_transaction": 30,
    "update_transaction": 12,
    "delete_transaction": 5,
}

metrics = {operation: 0 for operation in OPERATION_WEIGHTS}
metrics["errors"] = 0


def create_tables(cursor) -> None:
    """Create the source OLTP-style tables used by the CDC demo."""
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id SERIAL PRIMARY KEY,
            full_name VARCHAR(120) NOT NULL,
            email VARCHAR(160) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS merchants (
            merchant_id SERIAL PRIMARY KEY,
            merchant_name VARCHAR(160) NOT NULL,
            category VARCHAR(80) NOT NULL,
            country VARCHAR(2) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            account_id SERIAL PRIMARY KEY,
            user_id INT NOT NULL,
            account_type VARCHAR(20) NOT NULL,
            balance DECIMAL(12, 2) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS financial_transactions (
            transaction_id SERIAL PRIMARY KEY,
            account_id INT NOT NULL,
            user_id INT NOT NULL,
            merchant_id INT NOT NULL,
            transaction_amount DECIMAL(10, 2) NOT NULL,
            transaction_type VARCHAR(10) NOT NULL,
            transaction_status VARCHAR(20) NOT NULL DEFAULT 'SETTLED',
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    logger.info("Source tables are ready: users, merchants, accounts, financial_transactions")


def count_rows(cursor, table: str) -> int:
    cursor.execute(f"SELECT COUNT(*) FROM {table};")
    return cursor.fetchone()[0]


def random_id(cursor, table: str, id_column: str) -> Optional[int]:
    cursor.execute(f"SELECT {id_column} FROM {table} ORDER BY RANDOM() LIMIT 1;")
    result = cursor.fetchone()
    return result[0] if result else None


def insert_user(cursor) -> Optional[int]:
    full_name = fake.name()
    email = fake.unique.email()
    status = random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "SUSPENDED"])
    cursor.execute(
        """
        INSERT INTO users (full_name, email, status)
        VALUES (%s, %s, %s)
        RETURNING user_id;
        """,
        (full_name, email, status),
    )
    user_id = cursor.fetchone()[0]
    metrics["insert_user"] += 1
    logger.info("INSERT users | user_id=%s | status=%s", user_id, status)
    return user_id


def update_user(cursor) -> Optional[int]:
    user_id = random_id(cursor, "users", "user_id")
    if user_id is None:
        return insert_user(cursor)
    status = random.choice(["ACTIVE", "SUSPENDED", "CLOSED"])
    cursor.execute(
        """
        UPDATE users
        SET status = %s, updated_at = CURRENT_TIMESTAMP
        WHERE user_id = %s;
        """,
        (status, user_id),
    )
    metrics["update_user"] += 1
    logger.info("UPDATE users | user_id=%s | status=%s", user_id, status)
    return user_id


def insert_merchant(cursor) -> Optional[int]:
    categories = ["grocery", "travel", "retail", "fuel", "subscription", "restaurant"]
    cursor.execute(
        """
        INSERT INTO merchants (merchant_name, category, country)
        VALUES (%s, %s, %s)
        RETURNING merchant_id;
        """,
        (fake.company(), random.choice(categories), fake.country_code()),
    )
    merchant_id = cursor.fetchone()[0]
    metrics["insert_merchant"] += 1
    logger.info("INSERT merchants | merchant_id=%s", merchant_id)
    return merchant_id


def update_merchant(cursor) -> Optional[int]:
    merchant_id = random_id(cursor, "merchants", "merchant_id")
    if merchant_id is None:
        return insert_merchant(cursor)
    cursor.execute(
        """
        UPDATE merchants
        SET category = %s, updated_at = CURRENT_TIMESTAMP
        WHERE merchant_id = %s;
        """,
        (random.choice(["grocery", "travel", "retail", "fuel", "restaurant"]), merchant_id),
    )
    metrics["update_merchant"] += 1
    logger.info("UPDATE merchants | merchant_id=%s", merchant_id)
    return merchant_id


def insert_account(cursor) -> Optional[int]:
    user_id = random_id(cursor, "users", "user_id") or insert_user(cursor)
    balance = Decimal(str(round(random.uniform(100.0, 20000.0), 2)))
    cursor.execute(
        """
        INSERT INTO accounts (user_id, account_type, balance, status)
        VALUES (%s, %s, %s, %s)
        RETURNING account_id;
        """,
        (user_id, random.choice(["CHECKING", "SAVINGS", "CREDIT"]), balance, random.choice(["OPEN", "OPEN", "FROZEN"])),
    )
    account_id = cursor.fetchone()[0]
    metrics["insert_account"] += 1
    logger.info("INSERT accounts | account_id=%s | user_id=%s", account_id, user_id)
    return account_id


def update_account(cursor) -> Optional[int]:
    account_id = random_id(cursor, "accounts", "account_id")
    if account_id is None:
        return insert_account(cursor)
    balance_delta = Decimal(str(round(random.uniform(-500.0, 500.0), 2)))
    cursor.execute(
        """
        UPDATE accounts
        SET balance = GREATEST(balance + %s, 0), updated_at = CURRENT_TIMESTAMP
        WHERE account_id = %s;
        """,
        (balance_delta, account_id),
    )
    metrics["update_account"] += 1
    logger.info("UPDATE accounts | account_id=%s | delta=%s", account_id, balance_delta)
    return account_id


def delete_account(cursor) -> Optional[int]:
    account_id = random_id(cursor, "accounts", "account_id")
    if account_id is None:
        return None
    cursor.execute("DELETE FROM accounts WHERE account_id = %s;", (account_id,))
    metrics["delete_account"] += 1
    logger.info("DELETE accounts | account_id=%s", account_id)
    return account_id


def insert_transaction(cursor) -> Optional[int]:
    cursor.execute("SELECT account_id, user_id FROM accounts ORDER BY RANDOM() LIMIT 1;")
    account = cursor.fetchone()
    if not account:
        account_id = insert_account(cursor)
        cursor.execute("SELECT account_id, user_id FROM accounts WHERE account_id = %s;", (account_id,))
        account = cursor.fetchone()
    merchant_id = random_id(cursor, "merchants", "merchant_id") or insert_merchant(cursor)
    amount = Decimal(str(round(random.uniform(5.0, 2500.0), 2)))
    cursor.execute(
        """
        INSERT INTO financial_transactions (
            account_id, user_id, merchant_id, transaction_amount, transaction_type, transaction_status
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING transaction_id;
        """,
        (
            account[0],
            account[1],
            merchant_id,
            amount,
            random.choice(["CREDIT", "DEBIT"]),
            random.choice(["SETTLED", "SETTLED", "PENDING", "REVERSED"]),
        ),
    )
    transaction_id = cursor.fetchone()[0]
    metrics["insert_transaction"] += 1
    logger.info("INSERT financial_transactions | transaction_id=%s | amount=%s", transaction_id, amount)
    return transaction_id


def update_transaction(cursor) -> Optional[int]:
    transaction_id = random_id(cursor, "financial_transactions", "transaction_id")
    if transaction_id is None:
        return insert_transaction(cursor)
    cursor.execute(
        """
        UPDATE financial_transactions
        SET transaction_status = %s, timestamp = CURRENT_TIMESTAMP
        WHERE transaction_id = %s;
        """,
        (random.choice(["SETTLED", "PENDING", "REVERSED", "FAILED"]), transaction_id),
    )
    metrics["update_transaction"] += 1
    logger.info("UPDATE financial_transactions | transaction_id=%s", transaction_id)
    return transaction_id


def delete_transaction(cursor) -> Optional[int]:
    transaction_id = random_id(cursor, "financial_transactions", "transaction_id")
    if transaction_id is None:
        return None
    cursor.execute("DELETE FROM financial_transactions WHERE transaction_id = %s;", (transaction_id,))
    metrics["delete_transaction"] += 1
    logger.info("DELETE financial_transactions | transaction_id=%s", transaction_id)
    return transaction_id


def seed_initial_data(cursor) -> None:
    """Ensure every table has data so update/delete events are generated early."""
    if count_rows(cursor, "users") == 0:
        for _ in range(10):
            insert_user(cursor)
    if count_rows(cursor, "merchants") == 0:
        for _ in range(8):
            insert_merchant(cursor)
    if count_rows(cursor, "accounts") == 0:
        for _ in range(15):
            insert_account(cursor)
    if count_rows(cursor, "financial_transactions") == 0:
        for _ in range(20):
            insert_transaction(cursor)


def choose_operation() -> str:
    operations = []
    for operation, weight in OPERATION_WEIGHTS.items():
        operations.extend([operation] * weight)
    return random.choice(operations)


def print_metrics() -> None:
    total = sum(value for key, value in metrics.items() if key != "errors")
    breakdown = " | ".join(f"{key}={value}" for key, value in metrics.items())
    logger.info("METRICS | total=%s | %s", total, breakdown)


def main() -> None:
    operations: Dict[str, Callable] = {
        "insert_user": insert_user,
        "update_user": update_user,
        "insert_merchant": insert_merchant,
        "update_merchant": update_merchant,
        "insert_account": insert_account,
        "update_account": update_account,
        "delete_account": delete_account,
        "insert_transaction": insert_transaction,
        "update_transaction": update_transaction,
        "delete_transaction": delete_transaction,
    }

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        create_tables(cursor)
        seed_initial_data(cursor)

        logger.info("Starting multi-table CDC generation. Press Ctrl+C to stop.")
        operation_count = 0
        while True:
            operation = choose_operation()
            try:
                operations[operation](cursor)
            except Exception as exc:
                metrics["errors"] += 1
                logger.exception("Operation failed: %s", exc)

            operation_count += 1
            if operation_count % 10 == 0:
                print_metrics()
            time.sleep(float(os.getenv("GENERATOR_SLEEP_SECONDS", "2")))

    except KeyboardInterrupt:
        logger.info("Stopping data generation")
        print_metrics()
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
    finally:
        if "conn" in locals():
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()
