# Financial Trades Data Engineering Project

## Overview

This project demonstrates the implementation of a modern Data Engineering pipeline using Databricks, PySpark and Delta Lake.

The goal is to process 75,000 financial transactions through a Medallion Architecture (Bronze, Silver, Gold) and expose business-ready datasets.

---

## Technologies

- Databricks
- PySpark
- SQL
- Delta Lake
- Databricks Workflows

---

## Architecture

CSV File
   │
   ▼
Bronze Layer
   │
   ▼
Silver Layer
   │
   ▼
Gold Layer
   │
   ▼
Analytics & Dashboards

---

## Bronze Layer

Raw ingestion of 75,000 transactions.

Checks performed:

- Row count validation
- Schema validation
- Currency quality analysis
- Duplicate detection
- Negative amount detection

---

## Silver Layer

Data cleansing and standardization:

- Currency normalization
- Duplicate removal
- Negative amount correction
- Error segregation

---

## Gold Layer

Business aggregations:

- Exposure by Currency
- Exposure by Country
- Exposure by Rating
- Exposure by Issuer

---

## Delta Lake Features

Implemented Delta Lake functionalities:

- ACID Transactions
- Delta Tables
- MERGE Operations
- Data Versioning

---

## Workflow Orchestration

Databricks Workflow:

Bronze → Silver → Gold

The workflow automatically executes notebooks in the correct order.

---
## Skills Demonstrated

- Data Engineering
- ETL / ELT Design
- PySpark Transformations
- Delta Lake
- SQL
- Data Quality
- Workflow Orchestration
- Medallion Architecture
- Git & GitHub

---

## Project Results

- 75,000 financial transactions processed
- 3,644 duplicate Trade_ID detected
- 769 negative amounts corrected
- 1,508 null currency values isolated
- 3-layer Medallion Architecture implemented
