## Architecture

### High-level components
- Databricks (Driver + Executors): Executes Spark jobs for ingestion, chunking, and detection.
- Input Sources: Unity Catalog tables, S3 CSV, DBFS, Workspace files, or mounted Google Drive.
- Amazon S3:
  - Transactions Folder: receives chunked CSVs from Mechanism X.
  - Detections Folder: stores JSON detection batches from Mechanism Y.
- Optional Postgres: Stores processed S3 keys to ensure idempotent polling in Mechanism Y.
- Pattern Detector (PatId1): Spark-based module joining transactions with importance to find eligible upgrades.

### Data flow
1. Load transactions and importance from the configured source (default: Unity Catalog).
2. Mechanism X:
   - Partition transactions into CHUNK_SIZE rows.
   - Upload CSV chunks to S3 every X_SLEEP_SECONDS under S3_TRANSACTIONS_FOLDER.
3. Mechanism Y:
   - Poll S3 every Y_POLL_INTERVAL_SECS for new chunk keys (up to Y_MAX_POLL_ROUNDS).
   - For each new chunk: read CSV, run PatId1 detection, and write detections as JSON (batched by DETECTION_BATCH_SIZE) to S3_DETECTIONS_FOLDER.
   - Optionally record processed keys in Postgres to prevent reprocessing.

### PatId1 detection logic
- Compute per-merchant customer transaction counts.
- Select top 10% customers by transaction count for each merchant.
- Compute average importance weightage per (merchant_id, customer_id).
- Compute the merchant’s 10th percentile weightage.
- Eligible if:
  - Merchant’s total transactions ≥ MIN_TRANSACTIONS_FOR_UPGRADE,
  - avg_weightage is not null and ≤ merchant’s 10th percentile (w10).

### Mermaid flow diagram
