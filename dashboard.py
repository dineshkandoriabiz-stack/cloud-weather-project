import streamlit as st
import boto3
import pandas as pd
import time
import io
from groq import Groq
from datetime import datetime

# --- CONFIGURATION ---
DATABASE = 'default'
# Updated to point to your new Gold View
TABLE = 'view_ai_travel_context' 
S3_OUTPUT = 's3://open-meteo-lake-dinesh-kandoria/athena_results/' 
REGION = 'ap-southeast-2'

st.set_page_config(page_title="Global Weather AI Oracle", page_icon="🌍", layout="wide")

# --- AWS ATHENA HELPER FUNCTION ---
@st.cache_data(ttl=3600)
def fetch_athena_data(query):
    # Using secrets for credentials as per your existing setup
    athena_client = boto3.client(
        'athena', 
        region_name=st.secrets["AWS_DEFAULT_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
    )
    s3_client = boto3.client(
        's3', 
        region_name=st.secrets["AWS_DEFAULT_REGION"],
        aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
    )

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    query_id = response['QueryExecutionId']

    with st.spinner("Consulting the Data Lake..."):
        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(1) # Faster polling for better UX

    if status == 'SUCCEEDED':
        s3_key = f"athena_results/{query_id}.csv"
        bucket_name = S3_OUTPUT.split('/')[2]
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df
    else:
        st.error(f"Athena error: {stats['QueryExecution']['Status']['StateChangeReason']}")
        return pd.DataFrame()

# --- SIDEBAR HEALTH CHECK ---
st.sidebar.header("⚙️ System Status")

# Query the latest ingestion time
health_query = 'SELECT MAX("observation_time") as last_run FROM "default"."weather_refined"'
health_df = fetch_athena_data(health_query)

if not health_df.empty and health_df['last_run'].iloc[0]:
    last_run = pd.to_datetime(health_df['last_run'].iloc[0])
    
    # Calculate how long ago the data was updated
    time_diff = datetime.now() - last_run
    hours_ago = time_diff.total_seconds() // 3600
    
    if hours_ago < 24:
        st.sidebar.success(f"● Data Lake: Healthy")
    else:
        st.sidebar.warning(f"● Data Lake: Stale ({int(hours_ago)}h ago)")
    
    st.sidebar.info(f"**Last Sync:** {last_run.strftime('%b %d, %H:%M')} UTC")
else:
    st.sidebar.error("● Data Lake: Disconnected")

st.sidebar.divider()
st.sidebar.markdown("""
**Infrastructure:**
- ☁️ AWS S3 (Bronze/Silver)
- 🏛️ Amazon Athena (SQL)
- 🤖 LLaMA 3.1 (Groq AI)
""")

# --- UI FRONTEND ---
st.title("🌍 Global 50 Cities: AI Weather Oracle")

# 1. Fetch cities from the Gold View
city_query = f'SELECT DISTINCT "city" FROM "default"."{TABLE}" ORDER BY "city"'
cities_df = fetch_athena_data(city_query)

if not cities_df.empty:
    cities_list = cities_df['city'].tolist()
    selected_city = st.selectbox("Select a Destination:", cities_list)

    if selected_city:
        # 2. Query the data for the selected city
        data_query = f'SELECT * FROM "default"."{TABLE}" WHERE "city" = \'{selected_city}\''
        city_data = fetch_athena_data(data_query)
        
        if not city_data.empty:
            # Ensure forecast_date is string for comparison
            city_data['forecast_date'] = city_data['forecast_date'].astype(str)
            available_dates = sorted(city_data['forecast_date'].unique().tolist())
            
            selected_date = st.selectbox("Select Travel Date:", available_dates)
            
            # Filter data for the specific chosen date
            day_data = city_data[city_data['forecast_date'] == selected_date].iloc[0]
            
            # --- NEW: VISUAL METRICS ROW ---
            st.markdown(f"### 📊 Environmental Metrics for {selected_city}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Avg Temp", f"{day_data['avg_temp']}°C")
            m2.metric("Air Quality (AQI)", f"{int(day_data['avg_aqi'])}")
            m3.metric("Rainfall", f"{day_data['total_precip']}mm")
            m4.metric("Sunlight", f"{int(day_data['avg_sunlight'])} W/m²")

            # --- THE AI ORACLE ---
            st.divider()
            st.subheader(f"🤖 Ask the AI Oracle about {selected_city}")

            groq_client = Groq(api_key=st.secrets["GROQ_API_KEY"])

            if st.button(f"Generate Contextual Advice for {selected_date}"):
                with st.spinner(f"Analyzing {selected_city}'s local climate..."):
                    try:
                        # Use the llm_raw_data string we created in the Gold View
                        raw_context = day_data['llm_raw_data']
                        
                        chat_completion = groq_client.chat.completions.create(
                            messages=[
                                {
                                    "role": "system", 
                                    "content": (
                                        f"You are a localized Travel Expert for {selected_city}. "
                                        "Interpret raw data based on local climate and cultural norms. "
                                        "Do not just repeat numbers; explain how they feel. "
                                        "Format with Markdown and emojis."
                                    )
                                },
                                {
                                    "role": "user", 
                                    "content": (
                                        f"Based on this data: '{raw_context}', provide: \n"
                                        f"1. Subjective weather feel for {selected_city}.\n"
                                        "2. Specific clothing and gear recommendations.\n"
                                        "3. Health tips regarding AQI and sunlight.\n"
                                        "4. A local activity and restaurant recommendation that fits this specific weather."
                                    )
                                }
                            ],
                            model="llama-3.1-8b-instant",
                            max_tokens=1024
                        )

                        st.success(chat_completion.choices[0].message.content)
                    except Exception as e:
                        st.error(f"AI Oracle is offline: {e}")

else:
    st.warning("No data found. Check your S3 bucket and Lambda status.")