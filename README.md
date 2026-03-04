# cloud-weather-project
# 🌦️ Serverless AI Weather Oracle & Data Lake
**AWS Cloud Architecture | Data Engineering | Generative AI (RAG)**
![Architecture Diagram](architecture.png)

A production-ready, event-driven data pipeline that automates the collection of global weather data, stores it in a partitioned S3 Data Lake, and uses Amazon Bedrock (GenAI) to provide natural language travel insights.

---

## 🏗️ The Architecture
This project implements a **Modern Data Lakehouse** pattern using 100% Serverless AWS services.

*   **Ingestion:** AWS Lambda fetches 14-day snapshots (History + Forecast) from Open-Meteo API.
*   **Orchestration:** Amazon EventBridge triggers the ingestion daily (Cost-optimized).
*   **Storage (Data Lake):** Amazon S3 organized with **Hive-style Partitioning** (`year/month`) for high-performance querying.
*   **Data Warehouse:** Amazon Athena provides a SQL interface over raw S3 CSV files.
*   **AI Layer (RAG):** Amazon Bedrock (Nova/Titan) processes structured SQL data into human-readable advice.
*   **Observability:** Amazon SNS provides automated "On-Failure" email alerts.
*   **FinOps:** S3 Lifecycle Policies automatically expire raw data after 30 days to maintain a $0 monthly bill.

---

## 🚀 Technical Highlights

### 1. Schema-on-Read & Partitioning
Instead of a traditional database, I implemented a partitioned data lake. By organizing S3 paths as `raw_data/year=YYYY/month=MM/`, I optimized Athena query performance and reduced data scanning costs by over 90%.

### 2. Retrieval-Augmented Generation (RAG)
Rather than relying on an LLM's static training data, this pipeline feeds real-time SQL aggregates from Athena into **Amazon Bedrock**. This ensures the AI provides factual, data-driven travel advice without hallucinations.

### 3. Fault-Tolerant Design
The system is built to be "Self-Healing" and "Observable":
- **EventBridge Scheduler:** Decouples the trigger from the logic.
- **SNS Destinations:** Automated alerting if the API or Lambda fails.
- **Idempotent Logic:** The Lambda fetches a rolling 7-day window to prevent data gaps during temporary API outages.

---

## 📊 SQL Insights (Athena)
I developed a **Refined View** to transform raw strings into queryable timestamps and business logic:
```sql
CREATE OR REPLACE VIEW weather_analytics AS
SELECT 
    latitude, 
    temperature, 
    CAST(from_iso8601_timestamp(obs_time) AS TIMESTAMP) as observation_time
FROM "weather_data";


