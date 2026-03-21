# 🌍 A 50-City Data Pipeline Feeding a LLaMA-Powered Travel Oracle
**[🟢 Live Application / Demo](https://cloud-aws-weather-project-9grnnrrrkqiksy55ch2.streamlit.app/)** ## 📌 Project Overview
An end-to-end, fully serverless Data Lakehouse and Generative AI application. This project extracts, transforms, and loads (ETL) real-time environmental data for 50 global tech hubs, utilizing a LLaMA 3.1 LLM to generate contextual, weather-aware travel and culinary advice.

This architecture demonstrates advanced Cloud Engineering, automated DataOps, and GenAI prompt engineering, operating entirely on scalable, low-cost serverless infrastructure.

## 🏗️ Architecture & Tech Stack

### 1. Data Ingestion & Storage (The S3 Data Lake)
* **Python & Boto3:** A lightweight extraction script querying the Open-Meteo API for 15 data points (Temperature, AQI, Precipitation, Solar Radiation).
* **AWS Lambda & EventBridge:** Fully automated, serverless compute triggered on a scheduled CRON job.
* **Amazon S3:** Raw CSVs are partitioned by `year` and `month` for optimal query scanning and storage efficiency.
* **S3 Lifecycle Policies:** Implemented automated FinOps rules to purge temporary Athena logs after 7 days, maintaining a zero-maintenance, cost-effective storage layer.

### 2. Data Transformation (The Athena Lakehouse)
* **Amazon Athena (Presto SQL):** Serverless query engine used to transform raw S3 files into structured, AI-ready insights.
* **Schema Evolution:** Handled schema drift by building modular `CREATE EXTERNAL TABLE` and `CREATE VIEW` scripts to aggregate metrics like Average AQI and Max Temperatures into summarized context strings.

### 3. CI/CD & Automation (DataOps)
* **GitHub Actions:** A robust YAML pipeline that automatically deploys Python code to AWS Lambda and executes updated SQL schemas directly in Amazon Athena upon every `git push`.

### 4. Frontend UI & Generative AI
* **Streamlit Community Cloud:** A dynamic, interactive Python web application directly connected to the Athena Data Lake.
* **Pandas & Time-Series Filtering:** Custom logic to block past-date selections, ensuring the UI only presents relevant forecasting data.
* **Groq API (LLaMA 3.1 8B):** Integrates the live Athena SQL summaries as context limits for the LLM. Engineered prompts force the model to output structured, bulleted travel, clothing, and localized restaurant recommendations based strictly on the current climate.

---

## 🚀 How to Run Locally

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/your-username/your-repo-name.git](https://github.com/your-username/your-repo-name.git)
   cd your-repo-name