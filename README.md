#### Transaction Processing with Pattern Detection (PatId1)
Overview
This Databricks notebook implements two main mechanisms (X and Y) for online transaction processing and pattern detection using Spark, S3, and optionally Postgres for tracking.

It supports multiple data input sources (S3, DBFS, Workspace, Unity Catalog, Google Drive) and can detect specific transaction patterns (PatId1) based on merchant–customer transaction activity and importance weightages.

Features
Mechanism X — Chunking & Upload to S3
Reads source transactions dataset from the configured source.

Splits transactions into chunks of CHUNK_SIZE rows (default: 10,000).

Uploads chunks to S3 every X_SLEEP_SECONDS seconds (default: 1s) in S3_TRANSACTIONS_FOLDER.

Mechanism Y — Polling & Detection
Polls S3 for new transaction chunks every Y_POLL_INTERVAL_SECS (default: 1s).

Runs PatId1 detection:

Finds top 10% customers by transaction count per merchant

Selects customers whose average importance weightage ≤ merchant's 10th percentile

Merchant must have at least MIN_TRANSACTIONS_FOR_UPGRADE total transactions (default: 1000)

Writes detections in batches of size DETECTION_BATCH_SIZE (default: 50) to JSON in S3_DETECTIONS_FOLDER.

Optionally stores processed S3 keys in Postgres to avoid reprocessing.

Supported Input Sources
Set INPUT_SOURCE in environment variables or within the notebook:

"s3": Directly read CSVs from S3 URIs.

"volume": Read from Unity Catalog Volumes.

"dbfs": Read from DBFS paths like dbfs:/FileStore/....

"workspace": Read from Databricks Workspace file paths.

"catalog": Read from Unity Catalog managed tables.

"gdrive": Read from mounted Google Drive.

Environment Variables & Config
AWS & S3

Variable	Default	Description
AWS_ACCESS_KEY_ID	—	AWS key
AWS_SECRET_ACCESS_KEY	—	AWS secret
AWS_REGION	eu-north-1	AWS Region
S3_BUCKET_NAME	aws18082003	S3 bucket for input/output
S3_TRANSACTIONS_FOLDER	test/transactions/	S3 folder for chunks
S3_DETECTIONS_FOLDER	test/detections/	S3 folder for detections
Processing

Variable	Default	Description
CHUNK_SIZE	10000	Rows per chunk for Mechanism X
DETECTION_BATCH_SIZE	50	Detections per JSON batch
MIN_TRANSACTIONS_FOR_UPGRADE	1000	Merchant transaction threshold
X_SLEEP_SECONDS	1	Sleep between chunk uploads
Y_POLL_INTERVAL_SECS	1	Y's polling interval
Y_MAX_POLL_ROUNDS	10	Max Y polling loops
Database (Optional)

Variable	Default	Description
PG_HOST	—	Postgres host
PG_PORT	5432	Postgres port
PG_DB	—	DB name
PG_USER	—	DB user
PG_PASSWORD	—	DB password
PG_TABLE_KEYS	processed_s3_keys	Table for processed S3 keys
Pattern Detection: PatId1
A customer–merchant pair is marked as eligible for UPGRADE if:

Customer is in top 10% by transaction count for that merchant.

Merchant has at least MIN_TRANSACTIONS_FOR_UPGRADE total transactions.

Customer’s average weightage ≤ merchant’s 10th percentile weightage.

How to Run
In Databricks
Upload this notebook to Databricks Workspace.

Set environment variables or update defaults in the config section.

Choose run mode:

"x" — Only Mechanism X (chunk to S3)

"y" — Only Mechanism Y (poll S3 & detect patterns)

"both" — Start Y streaming, run X, then finish Y

Execute the notebook.

Example:

python
%env INPUT_SOURCE=catalog
%env RUN_MODE=both
Outputs in S3
After execution:

Transactions Chunks:

text
s3://<S3_BUCKET_NAME>/test/transactions/20250815_120000_chunk_0.csv
Detections:

text
s3://<S3_BUCKET_NAME>/test/detections/20250815_120005_detections_batch_0.json
Optional: Postgres Integration
If DB credentials are configured:

A table (PG_TABLE_KEYS) will store processed S3 keys.

Prevents repeated processing of the same transaction chunk.

Dependencies
boto3

psycopg2-binary (optional for Postgres)

pytz

pandas

pyspark

Databricks runtime environment

Install missing packages:

python
%pip install boto3 psycopg2-binary pytz
Notes
Ensure AWS credentials have correct permissions for S3 bucket access.

Unity Catalog or Google Drive mounting must be configured in Databricks.

This notebook is intended as a streaming simulation, not for actual real-time production systems without modifications.

Do you want me to also include an architecture diagram showing Mechanism X → S3 → Mechanism Y → Detection results for this README? That would make it clearer for your team.
