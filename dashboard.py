import streamlit as st
import boto3
import pandas as pd
import time
import io
from groq import Groq
from datetime import datetime

# --- CONFIGURATION ---
DATABASE = 'default'
# Pointing to your Gold View that aggregates the 7-day forecast
TABLE = 'view_ai_travel_context' 
S3_OUTPUT = 's3://open-meteo-lake-dinesh-kandoria/athena_results/' 

st.set_page_config(page_title="Global Weather AI Oracle", page_icon="🌍", layout="wide")

# --- AWS ATHENA HELPER FUNCTION ---
@st.cache_data(ttl=3600)
def fetch_athena_data(query):
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

    with st.spinner("Querying the Data Lake..."):
        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.5) 

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

health_query = 'SELECT MAX("observation_time") as last_run FROM "default"."weather_refined"'
health_df = fetch_athena_data(health_query)

if not health_df.empty and health_df['last_run'].iloc[0]:
    last_run = pd.to_datetime(health_df['last_run'].iloc[0])
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
- ☁️ AWS S3 & Athena
- 🤖 LLaMA 3.1 (Groq)
- 📊 7-Day Ensemble Forecast
""")

# --- UI FRONTEND ---
st.title("🌍 Global 50 Cities: AI Weather Oracle")

# 1. Fetch cities
city_query = f'SELECT DISTINCT "city" FROM "default"."{TABLE}" ORDER BY "city"'
cities_df = fetch_athena_data(city_query)

if not cities_df.empty:
    cities_list = cities_df['city'].tolist()
    selected_city = st.selectbox("Select a Destination:", cities_list)

    if selected_city:
        # 2. Query ALL available forecast dates for the selected city
        data_query = f'SELECT * FROM "default"."{TABLE}" WHERE "city" = \'{selected_city}\' ORDER BY "forecast_date" ASC'
        city_data = fetch_athena_data(data_query)
        
        if not city_data.empty:
            # --- WEATHER TREND CHART ---
            st.subheader(f"📈 7-Day Forecast Trend: {selected_city}")
            
            # Prepare data for charting
            chart_df = city_data.copy()
            chart_df['forecast_date'] = pd.to_datetime(chart_df['forecast_date'])
            chart_df = chart_df.set_index('forecast_date')

            # Display Temperature and AQI trends
            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.line_chart(chart_df['avg_temp'], color="#FF4B4B")
                st.caption("Temperature Forecast (°C)")
            with col_chart2:
                st.area_chart(chart_df['avg_aqi'], color="#29B5E8")
                st.caption("Air Quality Index (AQI) Forecast")

            st.divider()

            # --- DAILY SELECTION & METRICS ---
            st.subheader("📍 Daily Deep-Dive")
            available_dates = sorted(city_data['forecast_date'].unique().tolist())
            selected_date = st.select_slider("Pick a date to consult the Oracle:", options=available_dates)
            
            # Filter data for the specific chosen date
            day_data = city_data[city_data['forecast_date'] == selected_date].iloc[0]
            
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Avg Temp", f"{day_data['avg_temp']}°C")
            m2.metric("Air Quality", f"{int(day_data['avg_aqi'])} AQI")
            m3.metric("Expected Rain", f"{day_data['total_precip']}mm")
            m4.metric("Solar Energy", f"{int(day_data['avg_sunlight'])} W/m²")

            # --- THE AI ORACLE ---
            st.divider()
            st.subheader(f"🤖 Oracle's Advice for {selected_date}")

            if st.button(f"✨ Ask the Oracle"):
                groq_client = Groq(api_key=st.secrets["GROQ_API_KEY"])
                with st.spinner(f"Interpreting data for {selected_city}..."):
                    try:
                        raw_context = day_data['llm_raw_data']
                        
                        chat_completion = groq_client.chat.completions.create(
                            messages=[
                                {
                                    "role": "system", 
                                    "content": (
                                        f"You are a Travel Oracle for {selected_city}. "
                                        "Use the provided weather and AQI metrics to give travel advice. "
                                        "Be conversational, witty, and helpful. Use Markdown and emojis."
                                    )
                                },
                                {
                                    "role": "user", 
                                    "content": f"Here is the data for {selected_date}: {raw_context}. Please provide travel tips, clothing suggestions, and a recommended activity."
                                }
                            ],
                            model="llama-3.1-8b-instant",
                            max_tokens=800
                        )
                        st.markdown(chat_completion.choices[0].message.content)
                    except Exception as e:
                        st.error(f"Oracle is meditating (Error: {e})")

else:
    st.warning("No data found. Please ensure your Lambda has populated S3 and Athena views are created.")