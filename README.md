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

```text
CSV File
    ↓
Bronze Layer
    ↓
Silver Layer
    ↓
Gold Layer
    ↓
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

## Project Results

- 75,000 transactions processed
- Medallion Architecture implemented
- Delta Lake ACID validation completed
- Automated Databricks Workflow created
