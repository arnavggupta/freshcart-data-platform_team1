# FreshCart Data Platform — End-to-End Data Engineering Pipeline

**Name:** 

---

## Project Overview

FreshCart is a hyperlocal grocery delivery startup operating across major Indian cities. This project builds a scalable data platform to enable real-time analytics, centralized data storage, and business reporting.

The pipeline covers ingestion, transformation, and analytics using AWS and Databricks.

---

## Architecture

```
GitHub → AWS S3 → AWS Glue → AWS Athena  
Kinesis Streams → Firehose → S3 (Streaming)  
AWS Lambda → CloudWatch → Redshift  

Databricks:
Bronze → Silver → Gold  
         ↓  
SQL Dashboard  
         ↓  
Workflows Automation
```

---

## Technology Stack

**AWS:** S3, Glue, Athena, Kinesis, Lambda, CloudWatch, Redshift
**Databricks:** Delta Lake, PySpark, Databricks SQL, Workflows
**Languages:** Python, SQL
**Tools:** Git, GitHub

---

## Repository Structure

```
freshcart-data-platform/
│
├── data/
├── scripts/
│   ├── freshcart_utils.py
│   ├── data_profiler.py
│   └── kinesis_producer.py
│
├── notebooks/
├── docs/
├── SKILLS_EVIDENCE.md
├── README.md
└── .gitignore
```

---

## Data Pipeline

### Ingestion

* Batch data stored in S3 (raw/)
* Streaming data ingested via Kinesis → Firehose → S3

### Processing

**Bronze Layer**

* Raw data stored as Delta tables
* Append-only with metadata columns
* Partitioned by ingest_date

**Silver Layer**

* Data cleaning and transformation
* Deduplication using MERGE
* Standardized schema

**Gold Layer**

* Aggregated business metrics
* Built from Silver tables only

---

## Sample Output (Gold Layer)

| city      | total_orders | total_revenue | avg_order_value |
| --------- | ------------ | ------------- | --------------- |
| Bangalore | 120          | 45000         | 375             |
| Mumbai    | 98           | 38200         | 390             |
| Pune      | 75           | 28500         | 380             |

---

## Workflow

Databricks Workflow: FreshCart_Daily_Pipeline

Tasks:

1. Bronze Ingestion
2. Silver Transformation
3. Gold Analytics

Scheduled daily execution.

---

## Skills Evidence

All Python, SQL, and Spark answers are available in:
SKILLS_EVIDENCE.md

---

## Notes

* All scripts are available in /scripts
* Databricks notebooks are exported in /notebooks
* Supporting screenshots are stored in /docs
* SKILLS_EVIDENCE.md is the primary evaluation document
