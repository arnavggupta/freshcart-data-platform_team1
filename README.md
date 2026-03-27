# FreshCart Data Platform — End-to-End Data Engineering Pipeline

## Team Members

| ID     | Name                  |
| ------ | --------------------- |
| TSV780 | Aditya Rajesh Gahukar |
| TSV859 | Aditya Sah            |
| TSV771 | Anshu                 |
| TSV781 | Arnav Gupta           |
| TSV848 | Shubham Kumar         |
| TSV839 | Soham Khanna          |
| TSV755 | Suraj Kumar Singh     |
| TSV865 | Suroj Verma           |

---

## Project Overview

FreshCart is a hyperlocal grocery delivery startup operating across major Indian cities. This project builds a scalable data platform to enable real-time analytics, centralized data storage, and business reporting.

The pipeline covers ingestion, transformation, and analytics using AWS and Databricks.

---

## Architecture

```id="o6l3dz"
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

* Scripts are available in /scripts
* Notebooks are in /notebooks
* Screenshots are in /docs
* SKILLS_EVIDENCE.md is the primary evaluation file
