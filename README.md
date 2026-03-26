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
    H -->|CI/CD| F
'''mermaid


