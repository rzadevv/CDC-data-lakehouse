"""
Data generator that simulates a live app making changes to PostgreSQL.
Randomly does inserts, updates, and deletes to test the CDC pipeline.
"""
import time
import random
import logging
import psycopg2
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# DB connection settings
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "user": "user",
    "password": "password"
}

# How often each operation happens (percentages)
OPERATION_WEIGHTS = {
    "insert": 60,
    "update": 30,
    "delete": 10
}

fake = Faker()

# Track what we've done
metrics = {
    "inserts": 0,
    "updates": 0,
    "deletes": 0,
    "errors": 0
}


def create_table(cursor):
    """Create the transactions table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS financial_transactions (
            transaction_id SERIAL PRIMARY KEY,
            user_id INT,
            transaction_amount DECIMAL(10, 2),
            transaction_type VARCHAR(10),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    logger.info("Table 'financial_transactions' is ready")


def insert_transaction(cursor):
    """Add a random transaction."""
    user_id = random.randint(1, 100)
    amount = round(random.uniform(10.0, 1000.0), 2)
    txn_type = random.choice(['CREDIT', 'DEBIT'])
    
    cursor.execute("""
        INSERT INTO financial_transactions (user_id, transaction_amount, transaction_type)
        VALUES (%s, %s, %s)
        RETURNING transaction_id;
    """, (user_id, amount, txn_type))
    
    txn_id = cursor.fetchone()[0]
    metrics["inserts"] += 1
    logger.info(f"INSERT | txn_id={txn_id} | user={user_id} | {txn_type} | ${amount}")
    return txn_id


def update_transaction(cursor):
    """Change a random existing transaction."""
    cursor.execute("""
        SELECT transaction_id FROM financial_transactions
        ORDER BY RANDOM() LIMIT 1;
    """)
    result = cursor.fetchone()
    
    if not result:
        logger.warning("UPDATE skipped - no transactions exist yet")
        return None
    
    txn_id = result[0]
    new_amount = round(random.uniform(10.0, 1000.0), 2)
    new_type = random.choice(['CREDIT', 'DEBIT'])
    
    cursor.execute("""
        UPDATE financial_transactions
        SET transaction_amount = %s, transaction_type = %s, timestamp = CURRENT_TIMESTAMP
        WHERE transaction_id = %s;
    """, (new_amount, new_type, txn_id))
    
    metrics["updates"] += 1
    logger.info(f"UPDATE | txn_id={txn_id} | new_amount=${new_amount} | new_type={new_type}")
    return txn_id


def delete_transaction(cursor):
    """Remove a random transaction."""
    cursor.execute("""
        SELECT transaction_id FROM financial_transactions
        ORDER BY RANDOM() LIMIT 1;
    """)
    result = cursor.fetchone()
    
    if not result:
        logger.warning("DELETE skipped - no transactions exist yet")
        return None
    
    txn_id = result[0]
    
    cursor.execute("""
        DELETE FROM financial_transactions WHERE transaction_id = %s;
    """, (txn_id,))
    
    metrics["deletes"] += 1
    logger.info(f"DELETE | txn_id={txn_id}")
    return txn_id


def choose_operation():
    """Pick a random operation based on weights."""
    operations = []
    for op, weight in OPERATION_WEIGHTS.items():
        operations.extend([op] * weight)
    return random.choice(operations)


def print_metrics():
    """Show summary of what we've done."""
    total = metrics["inserts"] + metrics["updates"] + metrics["deletes"]
    logger.info(
        f"METRICS | Total: {total} | "
        f"Inserts: {metrics['inserts']} | "
        f"Updates: {metrics['updates']} | "
        f"Deletes: {metrics['deletes']} | "
        f"Errors: {metrics['errors']}"
    )


def main():
    """Run the generator loop."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        
        create_table(cursor)
        
        logger.info("Starting data generation... Press Ctrl+C to stop")
        logger.info(f"Operation weights: INSERT={OPERATION_WEIGHTS['insert']}% | "
                   f"UPDATE={OPERATION_WEIGHTS['update']}% | "
                   f"DELETE={OPERATION_WEIGHTS['delete']}%")
        
        operation_count = 0
        
        while True:
            operation = choose_operation()
            
            try:
                if operation == "insert":
                    insert_transaction(cursor)
                elif operation == "update":
                    update_transaction(cursor)
                elif operation == "delete":
                    delete_transaction(cursor)
            except Exception as e:
                metrics["errors"] += 1
                logger.error(f"Operation failed: {e}")
            
            operation_count += 1
            
            # Show stats every 10 operations
            if operation_count % 10 == 0:
                print_metrics()
            
            time.sleep(2)
            
    except KeyboardInterrupt:
        logger.info("Stopping data generation...")
        print_metrics()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")


if __name__ == "__main__":
    main()