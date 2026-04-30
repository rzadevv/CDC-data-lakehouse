"""Small Prometheus exporter for local CDC platform health."""
import os
import time
from typing import Any

import requests
from prometheus_client import Gauge, start_http_server

CONNECT_URL = os.getenv("CONNECT_URL", "http://connect:8083")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
ICEBERG_REST_URL = os.getenv("ICEBERG_REST_URL", "http://iceberg-rest:8181")
SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "15"))

connect_up = Gauge("cdc_connect_up", "Debezium Connect health, 1 when reachable")
schema_registry_up = Gauge("cdc_schema_registry_up", "Schema Registry health, 1 when reachable")
iceberg_rest_up = Gauge("cdc_iceberg_rest_up", "Iceberg REST catalog health, 1 when reachable")
connector_count = Gauge("cdc_connect_connector_count", "Registered Kafka Connect connectors")
schema_subject_count = Gauge("cdc_schema_subject_count", "Registered Schema Registry subjects")


def get_json(url: str) -> Any:
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()


def scrape_once() -> None:
    try:
        connectors = get_json(f"{CONNECT_URL}/connectors")
        connect_up.set(1)
        connector_count.set(len(connectors))
    except requests.RequestException:
        connect_up.set(0)
        connector_count.set(0)

    try:
        subjects = get_json(f"{SCHEMA_REGISTRY_URL}/subjects")
        schema_registry_up.set(1)
        schema_subject_count.set(len(subjects))
    except requests.RequestException:
        schema_registry_up.set(0)
        schema_subject_count.set(0)

    try:
        requests.get(f"{ICEBERG_REST_URL}/v1/config", timeout=5).raise_for_status()
        iceberg_rest_up.set(1)
    except requests.RequestException:
        iceberg_rest_up.set(0)


def main() -> None:
    start_http_server(9108)
    while True:
        scrape_once()
        time.sleep(SCRAPE_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
