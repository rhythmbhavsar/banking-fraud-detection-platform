# Banking Transaction Data Pipeline – Workflow

## Project Overview

This project simulates a **real-world banking data platform** that processes financial transactions in real time. The goal is to design a scalable data pipeline capable of ingesting streaming transaction data, performing data transformations, detecting suspicious activity, and delivering analytics-ready datasets for reporting.

The system demonstrates common data engineering concepts such as **streaming ingestion, distributed processing, data lake architecture, ETL pipelines, data warehousing, and business intelligence reporting**.



# End-to-End Workflow

The pipeline follows a layered architecture where data flows through several stages before becoming available for analytics.

```
Transaction Dataset
        ↓
Transaction Streaming Simulator
        ↓
Kafka Message Broker
        ↓
Spark Structured Streaming Processing
        ↓
AWS S3 Data Lake (Bronze Layer)
        ↓
ETL Transformations with PySpark
        ↓
Clean Data Storage (Silver Layer)
        ↓
Fraud Detection & Aggregations
        ↓
Analytics Data (Gold Layer)
        ↓
Snowflake Data Warehouse
        ↓
Power BI Dashboard
```

Each stage in this workflow plays a specific role in the overall data platform.



# 1. Data Source

The pipeline uses the **PaySim financial transactions dataset**, which simulates mobile banking transactions. The dataset includes information such as transaction type, account balances, and fraud indicators.

Example fields include:

* transaction type
* transaction amount
* origin account
* destination account
* timestamp
* fraud label

This dataset acts as a **simulated banking transaction system**.



# 2. Transaction Streaming Simulator

Since real banking systems generate continuous streams of transactions, a **Python-based streaming simulator** is used to emit transactions gradually instead of loading the dataset all at once.

The simulator reads records from the dataset and sends them as individual transaction events.

Purpose:

* mimic real-time banking transactions
* test streaming data pipelines
* simulate production-like event ingestion



# 3. Kafka Message Broker

The simulator publishes transaction events to a **Kafka topic**.

Kafka acts as a **distributed message broker** that decouples data producers from consumers.

Benefits of Kafka:

* high throughput event streaming
* fault tolerance
* message persistence
* scalability for high-volume data streams

Kafka ensures that downstream systems can consume transaction events reliably.



# 4. Stream Processing with Spark

**Spark Structured Streaming** consumes messages from Kafka and performs real-time processing.

Key operations include:

* parsing transaction events
* schema validation
* deduplication
* timestamp normalization
* transaction enrichment

Spark enables distributed processing, allowing the pipeline to scale as transaction volume increases.



# 5. Data Lake Storage (Bronze Layer)

Processed streaming data is stored in **AWS S3** as the raw data layer.

The Bronze layer stores data exactly as it arrives from the streaming pipeline. The data is partitioned by time to support efficient querying and processing.

Example structure:

```
s3://banking-data-lake/bronze/transactions/
    year=2026/
    month=03/
    day=10/
```

The Bronze layer provides:

* historical storage
* data traceability
* regulatory compliance support



# 6. ETL Transformations

PySpark ETL jobs transform the raw Bronze data into clean and structured datasets.

Key transformations include:

* removing duplicate transactions
* handling missing values
* validating transaction amounts
* standardizing timestamps
* applying schema normalization

These transformations prepare the data for reliable analytics.



# 7. Clean Data Storage (Silver Layer)

The cleaned dataset is stored in the **Silver layer** of the data lake.

The Silver layer contains validated and structured transactional data that can be used by downstream systems.

Example fields:

* transaction id
* account id
* transaction amount
* merchant id
* timestamp
* transaction location

This layer provides **trusted operational data**.



# 8. Fraud Detection and Aggregations (Gold Layer)

The Gold layer contains **business-ready datasets and analytical tables**.

Fraud detection rules are applied to identify suspicious activity.

Example fraud rules:

* transactions exceeding a defined threshold
* rapid multiple transactions from the same account
* transactions occurring across distant geographic locations within short time intervals

The Gold layer also generates aggregated datasets such as:

* daily transaction summaries
* fraud alerts
* account activity metrics



# 9. Data Warehouse Integration

Processed datasets are loaded into **Snowflake**, which acts as the central analytics warehouse.

The warehouse contains structured tables optimized for analytical queries.

Example tables include:

* fact_transactions
* dim_customers
* dim_accounts
* fraud_alerts
* transaction_summary

Snowflake allows analysts to query large volumes of data efficiently.



# 10. Dashboard and Reporting

The final step in the pipeline is visualization using **Power BI dashboards**.

Dashboards provide insights into the transaction ecosystem and highlight potential fraud patterns.

Example analytics views:

* total transactions over time
* fraud alerts by region
* high-risk customer accounts
* transaction volume by transaction type

These dashboards allow stakeholders to monitor financial activity and detect suspicious trends.



# Pipeline Orchestration

Pipeline tasks can be orchestrated using **Apache Airflow**, which schedules and manages ETL workflows.

Airflow DAGs automate tasks such as:

* data ingestion jobs
* ETL transformations
* warehouse loading
* data validation checks

This ensures the pipeline runs reliably and efficiently.



# Key Technologies Used

The project demonstrates a modern data engineering stack:

* Python
* Apache Kafka
* Apache Spark Structured Streaming
* AWS S3 Data Lake
* PySpark ETL
* Snowflake Data Warehouse
* Apache Airflow
* Power BI



# Conclusion

This project demonstrates how a financial institution can build a scalable platform for processing and analyzing transaction data.

The architecture supports:

* real-time event ingestion
* distributed data processing
* reliable data storage
* fraud detection analytics
* business intelligence reporting

