# Graph Report - /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse  (2026-04-30)

## Corpus Check
- 4 files · ~277,308 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 55 nodes · 61 edges · 11 communities detected
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]

## God Nodes (most connected - your core abstractions)
1. `main()` - 8 edges
2. `main()` - 5 edges
3. `TestCDCParsing` - 5 edges
4. `TestOperationLogic` - 5 edges
5. `process_cdc_batch()` - 4 edges
6. `TestDataTransformations` - 4 edges
7. `create_table()` - 3 edges
8. `insert_transaction()` - 3 edges
9. `update_transaction()` - 3 edges
10. `delete_transaction()` - 3 edges

## Surprising Connections (you probably didn't know these)
- `main()` --calls--> `create_table()`  [EXTRACTED]
  /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py → /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py  _Bridges community 7 → community 5_
- `main()` --calls--> `insert_transaction()`  [EXTRACTED]
  /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py → /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py  _Bridges community 8 → community 5_
- `main()` --calls--> `update_transaction()`  [EXTRACTED]
  /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py → /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py  _Bridges community 4 → community 5_
- `main()` --calls--> `delete_transaction()`  [EXTRACTED]
  /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py → /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py  _Bridges community 6 → community 5_
- `main()` --calls--> `choose_operation()`  [EXTRACTED]
  /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py → /home/chisste/Desktop/Projects/dataengproject/CDC-data-lakehouse/data_generator.py  _Bridges community 9 → community 5_

## Communities

### Community 0 - "Community 0"
Cohesion: 0.23
Nodes (11): create_iceberg_table_if_not_exists(), create_spark_session(), get_debezium_schema(), main(), process_cdc_batch(), Spark streaming job that reads CDC events from Kafka and writes to Iceberg. Uses, Start the streaming job., Set up Spark with Iceberg and MinIO. (+3 more)

### Community 1 - "Community 1"
Cohesion: 0.2
Nodes (7): Unit tests for CDC transformation logic. Tests the parsing and processing of Deb, Tests for data transformation functions., Test that transaction amounts are properly typed., Create a SparkSession for testing., Test filtering out records with null transaction_id., spark(), TestDataTransformations

### Community 2 - "Community 2"
Cohesion: 0.25
Nodes (5): Tests for CDC event parsing logic., Test parsing a Debezium INSERT (create) event., Test parsing a Debezium UPDATE event., Test parsing a Debezium DELETE event., TestCDCParsing

### Community 3 - "Community 3"
Cohesion: 0.25
Nodes (5): Test that DELETE operations are correctly identified., Test extracting transaction_id from 'before' for deletes., Tests for operation routing logic., Test that INSERT and UPDATE operations are classified as upserts., TestOperationLogic

### Community 4 - "Community 4"
Cohesion: 0.5
Nodes (3): Data generator that simulates a live app making changes to PostgreSQL. Randomly, Change a random existing transaction., update_transaction()

### Community 5 - "Community 5"
Cohesion: 0.5
Nodes (4): main(), print_metrics(), Show summary of what we've done., Run the generator loop.

### Community 6 - "Community 6"
Cohesion: 1.0
Nodes (2): delete_transaction(), Remove a random transaction.

### Community 7 - "Community 7"
Cohesion: 1.0
Nodes (2): create_table(), Create the transactions table if it doesn't exist.

### Community 8 - "Community 8"
Cohesion: 1.0
Nodes (2): insert_transaction(), Add a random transaction.

### Community 9 - "Community 9"
Cohesion: 1.0
Nodes (2): choose_operation(), Pick a random operation based on weights.

### Community 10 - "Community 10"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **27 isolated node(s):** `Data generator that simulates a live app making changes to PostgreSQL. Randomly`, `Create the transactions table if it doesn't exist.`, `Add a random transaction.`, `Change a random existing transaction.`, `Remove a random transaction.` (+22 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 6`** (2 nodes): `delete_transaction()`, `Remove a random transaction.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 7`** (2 nodes): `create_table()`, `Create the transactions table if it doesn't exist.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 8`** (2 nodes): `insert_transaction()`, `Add a random transaction.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 9`** (2 nodes): `choose_operation()`, `Pick a random operation based on weights.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 10`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `TestCDCParsing` connect `Community 2` to `Community 1`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **Why does `TestOperationLogic` connect `Community 3` to `Community 1`?**
  _High betweenness centrality (0.101) - this node is a cross-community bridge._
- **What connects `Data generator that simulates a live app making changes to PostgreSQL. Randomly`, `Create the transactions table if it doesn't exist.`, `Add a random transaction.` to the rest of the system?**
  _27 weakly-connected nodes found - possible documentation gaps or missing edges._