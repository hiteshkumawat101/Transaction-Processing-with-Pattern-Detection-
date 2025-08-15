User->>Dbx: Run notebook (RUN_MODE=both|x|y)
Dbx->>Dbx: Load inputs (INPUT_SOURCE=catalog|s3|dbfs|workspace|gdrive)

par Mechanism X
    Dbx->>Dbx: Partition transactions (CHUNK_SIZE)
    loop Every X_SLEEP_SECONDS
        Dbx->>S3Tx: Put chunk CSV
    end
and Mechanism Y
    loop Y_MAX_POLL_ROUNDS every Y_POLL_INTERVAL_SECS
        Dbx->>S3Tx: List objects (Prefix)
        Dbx->>PG: Load processed keys (optional)
        Dbx->>Dbx: Filter new keys
        alt New chunks found
            Dbx->>S3Tx: GetObject CSV
            Dbx->>Dbx: Spark read CSV
            Dbx->>Dbx: PatId1 (top10%, avg<=p10, merchant txns>=threshold)
            loop Batch DETECTION_BATCH_SIZE
                Dbx->>S3Det: Put detections JSON
            end
            Dbx->>PG: Insert processed keys (optional)
        else No new chunks
            Dbx->>Dbx: Sleep and continue
        end
    end
end
