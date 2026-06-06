# ⚙️ Operational Data Pipeline & Workflow Automation

> **Stack:** Snowflake · Azure Data Factory (ADF) · SQL · PL/SQL  
> **Company:** NielsenIQ (NIQ)

---

## 📋 Project Overview

Automated the end-to-end operational data pipeline for NIQ's governance and compliance reporting. Replaced manual, error-prone ETL processes with **Azure Data Factory** orchestration and **Snowflake incremental processing** using Streams and Tasks, achieving 99.5% data quality compliance.

---

## 🏗️ Architecture

```
Source Systems (Oracle / SQL Server / Flat Files)
         │
         ▼
Azure Data Factory (ADF)
├── Linked Services   — Source connections
├── Datasets          — Schema definitions
├── Data Flows        — Transformation logic
└── Pipelines         — Orchestration + triggers
         │
         ▼
Snowflake RAW Schema
         │
         ▼
Snowflake Streams (CDC — capture inserts/updates/deletes)
         │
         ▼
Snowflake Tasks (scheduled incremental merge logic)
         │
         ▼
Operational Data Store (ODS) / Reporting Layer
         │
         ▼
Governance & BI Dashboards
```

---

## 🔑 Key Components

### Azure Data Factory Pipelines
| Pipeline | Trigger | Description |
|----------|---------|-------------|
| `pl_ingest_operational_data` | Scheduled / Event | Pulls data from source systems into Snowflake RAW |
| `pl_validate_source_data` | Post-ingest | Row count, null checks, schema validation |
| `pl_run_transformations` | Chain trigger | Executes Snowflake stored procedures |
| `pl_reconciliation_report` | Daily | Source-to-target reconciliation audit |
| `pl_alert_on_failure` | Error handler | Email/Teams notification on failure |

### Snowflake CDC with Streams & Tasks

```sql
-- Create stream on raw table to capture changes
CREATE OR REPLACE STREAM raw_operational_stream
ON TABLE raw.operational_events
APPEND_ONLY = FALSE;

-- Task: Merge changed records into ODS layer every 15 mins
CREATE OR REPLACE TASK task_merge_operational
    WAREHOUSE = OPS_WH
    SCHEDULE = '15 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('raw_operational_stream')
AS
MERGE INTO ods.operational_events tgt
USING (
    SELECT * FROM raw_operational_stream
    WHERE METADATA$ACTION = 'INSERT'
) src
ON tgt.event_id = src.event_id
WHEN MATCHED THEN UPDATE SET
    tgt.status = src.status,
    tgt.updated_at = src.updated_at
WHEN NOT MATCHED THEN INSERT
    VALUES (src.event_id, src.status, src.created_at, src.updated_at);
```

### Data Quality Framework
```sql
-- Audit validation checks (run post-load)
WITH quality_checks AS (
    SELECT
        'null_check_event_id'        AS check_name,
        COUNT(*) FILTER (WHERE event_id IS NULL)     AS failures
    FROM ods.operational_events
    UNION ALL
    SELECT
        'duplicate_check_event_id',
        COUNT(*) - COUNT(DISTINCT event_id)
    FROM ods.operational_events
    UNION ALL
    SELECT
        'orphan_foreign_key',
        COUNT(*)
    FROM ods.operational_events o
    LEFT JOIN ods.dim_customers c ON o.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
)
SELECT * FROM quality_checks WHERE failures > 0;
```

---

## 📈 Results & Impact

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Manual Interventions/Week | ~20 | ~6 | **70% reduction** |
| Data Processing Efficiency | Baseline | +30% | **30% faster** |
| Data Quality Compliance | ~94% | 99.5% | **+5.5pp** |
| Pipeline Monitoring | Manual | Automated alerts | ✅ Fully monitored |

---

## 🗂️ Repository Structure

```
operational-pipeline/
├── adf_pipelines/
│   ├── pl_ingest_operational_data.json
│   ├── pl_validate_source_data.json
│   └── pl_reconciliation_report.json
├── snowflake_sql/
│   ├── streams_tasks_setup.sql
│   ├── merge_logic.sql
│   ├── data_quality_checks.sql
│   ├── reconciliation_queries.sql
│   └── stored_procedures.sql
├── plsql_transformations/
│   └── transformation_logic.sql
└── README.md
```
