FROM confluentinc/cp-kafka-connect:7.3.0

# Adds Debezium PostgreSQL CDC support while retaining Confluent Avro converters.
RUN confluent-hub install --no-prompt debezium/debezium-connector-postgresql:1.9.7
