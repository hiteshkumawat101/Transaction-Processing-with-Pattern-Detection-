flowchart LR
    subgraph Databricks [Databricks Workspace (Driver + Executors)]
        A[Input Loader\n(S3/DBFS/Workspace/UC/Drive)] --> B[Mechanism X\nChunk & Upload]
        A --> C[Mechanism Y\nPoll & Detect]
        B -->|CSV Chunks| S3_IN[(S3\nTransactions Folder)]
        C -->|Poll keys| S3_IN
        C -->|Detections JSON| S3_OUT[(S3\nDetections Folder)]
        C -->|Processed keys| PG[(Postgres\n(optional))]
        subgraph Detection[Pattern Detector (PatId1)]
            D1[Top 10% customers per merchant]
            D2[Avg weightage by (merchant, customer)]
            D3[10th percentile per merchant]
            D4[Merchant total txns >= threshold]
            D1 --> D2 --> D3 --> D4
        end
        C --> Detection
    end

    subgraph Inputs[Input Sources]
        UC[Unity Catalog Tables\n(datadump.test.transactions,\n datadump.test.customer_importance)]
        S3SRC[S3 CSV]
        DBFS[DBFS Files]
        WS[Workspace Files]
        GD[Google Drive (mounted)]
    end

    Inputs --> A
