# 🌍 A 50-City Data Pipeline Feeding a LLaMA-Powered Travel Oracle

**[🟢 Live Application / Demo](https://cloud-aws-weather-project-9grnnrrrkqiksy55ch2.streamlit.app/)**

## 📌 Project Overview
An end-to-end, fully serverless Data Lakehouse and Generative AI application. This project extracts, transforms, and loads (ETL) real-time environmental data for 50 global tech hubs, utilizing a LLaMA 3.1 LLM to generate contextual, weather-aware travel and culinary advice.

This architecture demonstrates advanced Cloud Engineering, automated DataOps, and GenAI prompt engineering, operating entirely on scalable, low-cost serverless infrastructure.

## 🏗️ System Architecture

```mermaid
graph TD
    A[Open-Meteo API] -->|Extract JSON| B(AWS Lambda)
    C[Amazon EventBridge] -->|Scheduled Trigger| B
    B -->|Load Raw CSVs| D[(Amazon S3 Data Lake)]
    D -->|Serverless Query| E(Amazon Athena)
    E -->|Structured Context| F[Streamlit Web App]
    F <-->|Prompt & Response| G((Groq API: LLaMA 3.1))
    H[GitHub Actions] -->|CI/CD Automation| B & E & F

------------
⚙️ Tech Stack & Pipeline Breakdown
1. Data Ingestion & Storage (The S3 Data Lake)
Python & Boto3: A lightweight extraction script querying the Open-Meteo API for 15 granular data points (Temperature, AQI, Precipitation, Solar Radiation).

AWS Lambda & EventBridge: Fully automated, serverless compute triggered on a scheduled CRON job to fetch the latest forecasting data.

Amazon S3: Raw CSVs are partitioned by year and month for optimal query scanning and storage efficiency.

S3 Lifecycle Policies: Implemented automated FinOps rules to purge temporary Athena query logs after 7 days, maintaining a zero-maintenance, cost-effective storage layer.

2. Data Transformation (The Athena Lakehouse)
Amazon Athena (Presto SQL): Serverless query engine used to transform raw S3 files into structured, AI-ready insights.

Schema Evolution: Handled schema drift by building modular CREATE EXTERNAL TABLE and CREATE VIEW scripts to aggregate metrics (e.g., Average AQI, Max Temperatures) into concise context strings.

3. CI/CD & Automation (DataOps)
GitHub Actions: A robust YAML pipeline that automatically deploys Python code to AWS Lambda and executes updated SQL schemas directly in Amazon Athena upon every git push.

4. Frontend UI & Generative AI
Streamlit Community Cloud: A dynamic, interactive Python web application directly connected to the Athena Data Lake.

Pandas & Time-Series Filtering: Custom logic to block past-date selections, ensuring the UI only presents relevant forecasting data to the user.

Groq API (LLaMA 3.1 8B): Integrates the live Athena SQL summaries as system context for the LLM. Engineered prompts force the model to output structured, bulleted travel, clothing, and localized restaurant recommendations based strictly on the current climate.

🧠 Future Enhancements
[ ] Integrate AWS Glue Crawlers for automated schema detection.

[ ] Add a visual data dashboard using Plotly for temperature trending.

[ ] Expand to 100 cities with dynamic API pagination.