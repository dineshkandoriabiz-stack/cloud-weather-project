🌍 Global 50 Cities: AI Weather Oracle
An End-to-End Serverless Data Lakehouse & LLaMA-Powered Travel Assistant
🟢 Live Application / Demo

📌 Project Overview
The Global Weather AI Oracle is a production-grade, fully serverless Data Lakehouse. It automates the extraction, transformation, and loading (ETL) of real-time environmental data for 50 global hubs. By bridging a High-Performance Data Lake (S3 + Athena) with Generative AI (LLaMA 3.1), the application transforms raw meteorological metrics into witty, context-aware travel and culinary advice.

This architecture demonstrates advanced Cloud Engineering, automated DataOps, and GenAI prompt engineering, operating entirely on scalable, low-cost serverless infrastructure.

## 🏗️ System Architecture

```mermaid
graph TD
    %% Source to Ingestion
    A[Open-Meteo API] -->|Extract JSON| B(AWS Lambda)
    C[Amazon EventBridge] -->|Scheduled Trigger| B
    %% Storage and Processing
    B -->|Partitioned CSVs| D[Amazon S3 Data Lake]
    D -->|Schema-on-Read| E[Amazon Athena]
    %% Application Layer
    E -->|Structured Context| F[Streamlit Web App]
    F <-->|Time-Aware Prompting| G[Groq API: LLaMA 3.1]
    %% DevOps Flow (Explicit lines for better compatibility)
    H[GitHub Actions] -->|CI/CD| B
    H -->|CI/CD| E
    H -->|CI/CD| F '''

⚙️ Tech Stack & Pipeline Breakdown

1. Data Ingestion & Storage (The S3 Data Lake)

Python & Boto3: A custom-built extraction engine querying the Open-Meteo API for 15 granular data points (Temperature, AQI, Precipitation, Shortwave Radiation).

Hive-Style Partitioning: Data is organized in S3 using year=YYYY/month=MM/ prefixes. This optimizes query performance and significantly reduces AWS Athena scanning costs by limiting the data read per request.

FinOps & Lifecycle Policies: Automated S3 lifecycle rules purge temporary Athena query results after 7 days, maintaining a zero-maintenance, cost-efficient storage layer.

2. Data Transformation (Medallion Architecture)

Bronze Layer: External tables mapping raw S3 CSVs to SQL.

Silver Layer: SQL Views using TRY_CAST and COALESCE to clean data and handle schema drift.

Gold Layer: Aggregated business views that pre-calculate 7-day trends and generate "LLM-Ready" context strings, reducing token usage and latency at the application layer.

3. CI/CD & DataOps

GitHub Actions: A robust YAML pipeline that automatically deploys Python code to AWS Lambda and executes updated SQL schemas in Athena upon every git push.

Metadata Management: Implemented MSCK REPAIR TABLE logic to synchronize the S3 physical storage with the Athena metadata catalog, ensuring new daily partitions are instantly discoverable.

4. Frontend UI & Time-Aware GenAI

Streamlit & Plotly: A dynamic dashboard featuring 7-day trend visualizations for Temperature and Air Quality Index (AQI).

Time-Aware Prompt Engineering: Developed a specialized prompt injection layer that feeds the "System Clock" to LLaMA 3.1. This ensures the AI correctly distinguishes between "Today" and "Future Forecasts," eliminating temporal hallucinations.

Groq Inference: Leverages LLaMA 3.1 (8B/70B) via Groq for sub-second inference speeds, providing clothing, health tips, and local activity recommendations.

🧠 Solved Engineering Challenges

Partition Blindness: Resolved issues where Athena failed to recognize new S3 data by implementing automated partition discovery and fixing root-folder mapping.

Data Integrity: Identified and mitigated "Schema Mismatch" errors caused by stray files in the S3 root, ensuring strict adherence to the partitioning strategy.

Temporal Logic: Fixed AI "Future Hallucinations" by implementing a Python-based date-comparison layer that adjusts the AI's tense (Past/Present/Future) based on the user's selection.

🔮 Future Roadmap
[ ] Multi-Region Redundancy: Deploying Lambda across multiple AWS regions to ensure 100% API availability.

[ ] Vector Search Integration: RAG-enhanced activity recommendations using a vector database for specific city landmarks.

[ ] Advanced Monitoring: Integrating AWS CloudWatch Alarms for Lambda execution failures or S3 ingestion delays.


