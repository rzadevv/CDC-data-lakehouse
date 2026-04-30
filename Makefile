.PHONY: up down logs stream-data spark-job query clean help register register-json register-avro schema-evolution schema-subjects

# Default target
help:
	@echo "CDC Lakehouse Pipeline - Available Commands"
	@echo "============================================"
	@echo "  make up          - Start all services"
	@echo "  make down        - Stop all services"
	@echo "  make logs        - View container logs"
	@echo "  make stream-data - Run data generator"
	@echo "  make spark-job   - Submit Spark streaming job"
	@echo "  make query       - Open Trino CLI"
	@echo "  make register    - Register JSON Debezium connector for Spark pipeline"
	@echo "  make register-avro - Register Avro connector for Schema Registry demo"
	@echo "  make schema-evolution - Add risk_score column and emit evolved CDC"
	@echo "  make clean       - Remove all containers and volumes"
	@echo "  make test        - Run unit tests"

# Infrastructure commands
up:
	docker-compose up -d
	@echo "Waiting for services to start..."
	@timeout /t 10 /nobreak > nul 2>&1 || sleep 10
	@echo "Services started. MinIO console: http://localhost:9001"

down:
	docker-compose down

logs:
	docker-compose logs -f

clean:
	docker-compose down -v --remove-orphans

# Register Debezium connector
register: register-json

register-json:
	curl -X POST -H "Content-Type: application/json" -d @debezium-connector.json http://localhost:8083/connectors
	@echo ""
	@echo "Connector registered. Check status: curl http://localhost:8083/connectors/postgres-connector-json/status"

register-avro:
	curl -X POST -H "Content-Type: application/json" -d @debezium-avro-connector.json http://localhost:8083/connectors
	@echo ""
	@echo "Avro connector registered. Check status: curl http://localhost:8083/connectors/postgres-connector-avro/status"

schema-subjects:
	curl -s http://localhost:8081/subjects

schema-evolution:
	python scripts/schema_evolution_demo.py

# Data generation
stream-data:
	python data_generator.py

# Spark job submission (run from host)
spark-job:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
		--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
		--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
		/app/spark_streaming.py

# Copy Spark script to container
copy-spark:
	docker cp spark_streaming.py spark-master:/app/spark_streaming.py

# Query via Trino
query:
	docker exec -it trino trino --catalog iceberg --schema db

# Development commands
test:
	python -m pytest tests/ -v

lint:
	black --check .
	isort --check-only .
	flake8 .

format:
	black .
	isort .

# Pre-commit setup
setup-hooks:
	pip install pre-commit
	pre-commit install
