# IPL-Data-Analysis
An AWS-based batch processing pipeline to analyze IPL match and player performance data. Data: Ball-by-ball and match-level data in CSV format, processed using Apache Spark and SQL. Services used: AWS S3, AWS Glue.

📌 Overview
This project implements an end-to-end data engineering pipeline on AWS to process and analyze Indian Premier League (IPL) cricket data. It covers ball-by-ball match data and match-level summaries, enabling insights into player performance, team strategies, and match outcomes across IPL seasons.
The pipeline is organized into 4 progressive projects, each building on the previous to demonstrate increasingly advanced data engineering and analytics capabilities.


Architecture
Raw CSV Data (S3)
      │
      ▼
┌─────────────────────────────────────────────────────┐
│                   AWS S3 (Data Lake)                │
│   ┌──────────────┐       ┌────────────────────┐    │
│   │ Raw Zone     │  ───▶ │  Processed Zone    │    │
│   │ (CSV files)  │       │  (Parquet/Tables)  │    │
│   └──────────────┘       └────────────────────┘    │
└─────────────────────────────────────────────────────┘
      │                          │
      ▼                          ▼
┌────────────┐           ┌────────────────┐
│ AWS Glue   │           │  AWS Glue      │
│ Crawler    │           │  Data Catalog  │
│            │──────────▶│  (Metadata)    │
└────────────┘           └────────────────┘
      │
      ▼
┌─────────────────────────────┐
│    AWS Glue ETL Job         │
│  (Apache Spark / PySpark)   │
│  - Transformations          │
│  - Aggregations             │
│  - SQL Analytics            │
└─────────────────────────────┘
      │
      ▼
┌─────────────────┐
│  Output Layer   │
│  (S3 / Athena)  │
└─────────────────┘


📂 Project Structure
IPL-Data-Analysis/
├── Project 1/          # Data ingestion & S3 setup
├── Project 2/          # AWS Glue Crawler & Data Catalog
├── Project 3/          # Spark ETL transformations
├── Project 4/          # SQL analytics & insights
└── README.md


Projects Breakdown
Project 1 — Data Ingestion & S3 Setup

Create an S3 bucket and configure folder structure (raw zone, processed zone).
Upload raw IPL CSV files to the designated S3 raw path.
Establish IAM roles and permissions for Glue access.

Project 2 — AWS Glue Crawler & Data Catalog

Configure an AWS Glue Crawler to scan the S3 raw data.
Automatically infer schema and populate the Glue Data Catalog.
Create a Glue database and validate tables and column data types.

Project 3 — Spark ETL Transformations (AWS Glue Job)

Author a PySpark ETL script in AWS Glue Studio.
Read from the Glue Data Catalog using DynamicFrame.
Perform data cleaning, type casting, null handling, and column selection.
Write transformed output back to S3 in Parquet format.

Project 4 — SQL Analytics & Insights

Register processed Parquet data in the Glue Data Catalog.
Run Spark SQL queries to derive analytical insights:

Top run-scorers and strike rates per season
Most economical bowlers (runs conceded per over)
Toss win vs. match win correlation
Venue-wise scoring trends
Team performance across seasons



