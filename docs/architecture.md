# Architecture

```mermaid
flowchart TB

    %% =========================
    %% Scheduling / Control
    %% =========================
    EB_BATCH[EventBridge Scheduler<br/>Batch Schedule]
    EB_STREAM[EventBridge Scheduler<br/>Market Hours Schedule]

    %% =========================
    %% External Sources
    %% =========================
    AV[Alpha Vantage API]
    FH[Finnhub WebSocket]

    %% =========================
    %% Batch Ingestion
    %% =========================
    LAMBDA[AWS Lambda]

    EB_BATCH --> LAMBDA
    LAMBDA --> AV
    AV --> LAMBDA

    %% =========================
    %% Streaming Ingestion
    %% =========================
    PRODUCER[ECS Fargate<br/>Producer]
    KINESIS[Amazon Kinesis<br/>Data Streams]
    CONSUMER[ECS Fargate<br/>Consumer]

    EB_STREAM -. Start / Stop .-> PRODUCER
    EB_STREAM -. Start / Stop .-> CONSUMER

    FH --> PRODUCER
    PRODUCER --> KINESIS
    KINESIS --> CONSUMER

    %% =========================
    %% S3 Data Lake
    %% =========================
    S3_LANDING[Amazon S3<br/>Landing Zone<br/>Alpha Vantage]
    S3_RAW[Amazon S3<br/>Raw Zone<br/>Finnhub]

    LAMBDA --> S3_LANDING
    CONSUMER --> S3_RAW

    %% =========================
    %% Databricks Processing
    %% =========================
    BRONZE[Bronze Layer<br/>Auto Loader / Incremental Ingestion]
    SILVER[Silver Layer<br/>Clean / Standardize / Deduplicate]
    DQ[Data Quality Validation]
    GOLD[Gold Layer<br/>Analytics-Ready Data]

    S3_LANDING --> BRONZE
    S3_RAW --> BRONZE

    BRONZE --> SILVER
    SILVER --> DQ
    DQ --> GOLD

    %% =========================
    %% Observability
    %% =========================
    AUDIT[Pipeline Audit Table<br/>workspace.gold.pipeline_audit]
    OBS[Observability Dashboard]

    DQ --> AUDIT
    AUDIT --> OBS

    %% =========================
    %% Consumption
    %% =========================
    ANALYTICS[Analytics Dashboard]

    GOLD --> ANALYTICS


