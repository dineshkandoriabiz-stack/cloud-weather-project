import streamlit as st
import boto3
import pandas as pd
import time
import io
import math
from groq import Groq
from datetime import datetime, date, timedelta, timezone

# --- CONFIGURATION ---
DATABASE = 'default'
TABLE = 'view_ai_travel_context' 
S3_OUTPUT = 's3://open-meteo-lake-dinesh-kandoria/athena_results/' 

st.set_page_config(
    page_title="Global Weather AI Oracle", 
    page_icon="🌍", 
    layout="wide"
)

# --- HELPER FUNCTIONS ---
def safe_int(val):
    """Safely converts a value to an integer, handling NaN and None."""
    try:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return None
        return int(val)
    except:
        return None

@st.cache_data(ttl=3600)
def fetch_athena_data(query):
    """Executes Athena query and returns a pandas DataFrame."""
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

    # Simple polling logic
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
        return pd.read_csv(io.BytesIO(obj['Body'].read()))
    else:
        return pd.DataFrame()

# --- TIMEZONE LOGIC (IST: UTC +5:30) ---
ist_now = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
ist_today = ist_now.date()

# --- SIDEBAR: SYSTEM METADATA ---
st.sidebar.header("🕒 Data Freshness")

# Fetch lineage from Silver Layer
health_query = 'SELECT MAX("batch_processed_at") as last_run FROM "weather_refined"'
health_df = fetch_athena_data(health_query)

if not health_df.empty and health_df['last_run'].iloc[0]:
    # Adjust UTC timestamp from Athena to IST for UI
    last_run_utc = pd.to_datetime(health_df['last_run'].iloc[0])
    last_run_ist = last_run_utc + timedelta(hours=5, minutes=30)
    st.sidebar.write(f"**Last Sync:** {last_run_ist.strftime('%b %d, %H:%M')} IST")
else:
    st.sidebar.error("Last Sync: Unknown")

st.sidebar.divider()
st.sidebar.markdown("""
**Infrastructure:**
- ☁️ AWS S3 & Athena
- 🕒 EventBridge & Lambda
- 🤖 LLaMA 3.1 (Groq)
- 📊 7-Day Ensemble
""")

# --- MAIN UI ---
st.title("🌍 Global 50 Cities: AI Weather Oracle")

# 1. Fetch available cities
city_query = f'SELECT DISTINCT "city" FROM "default"."{TABLE}" ORDER BY "city"'
cities_df = fetch_athena_data(city_query)

if not cities_df.empty:
    cities_list = cities_df['city'].tolist()
    selected_city = st.selectbox("Select a Destination:", cities_list)

    if selected_city:
        # 2. Query all forecast data for the city
        data_query = f'SELECT * FROM "default"."{TABLE}" WHERE "city" = \'{selected_city}\' ORDER BY "forecast_date" ASC'
        raw_city_data = fetch_athena_data(data_query)
        
        if not raw_city_data.empty:
            # Clean and filter: Today + Future only
            raw_city_data['forecast_date'] = pd.to_datetime(raw_city_data['forecast_date']).dt.date
            city_data = raw_city_data[raw_city_data['forecast_date'] >= ist_today].copy()

            if city_data.empty:
                st.warning(f"⚠️ No current forecast data available for {selected_city}. Please trigger a manual sync.")
            else:
                # --- SECTION: TREND CHARTS ---
                st.subheader(f"📈 7-Day Forecast Trend: {selected_city}")
                chart_df = city_data.copy().set_index('forecast_date')

                col_chart1, col_chart2 = st.columns(2)
                with col_chart1:
                    st.line_chart(chart_df['avg_temp'], color="#FF4B4B")
                    st.caption("Temperature Forecast (°C)")
                with col_chart2:
                    st.area_chart(chart_df['avg_aqi'].fillna(0), color="#29B5E8")
                    st.caption("Air Quality Index (AQI)")

                st.divider()

                # --- SECTION: DAILY SELECTION ---
                st.subheader("📍 Daily Deep-Dive")
                available_dates = sorted(city_data['forecast_date'].unique())
                selected_date = st.select_slider("Pick a date to consult the Oracle:", options=available_dates)
                
                # Fetch data for the specific slider date
                day_data = city_data[city_data['forecast_date'] == selected_date].iloc[0]
                
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Avg Temp", f"{day_data['avg_temp']:.1f}°C")
                
                aqi_val = safe_int(day_data['avg_aqi'])
                m2.metric("Air Quality", f"{aqi_val} AQI" if aqi_val is not None else "No Data")
                
                m3.metric("Expected Rain", f"{day_data['total_precip']:.1f}mm")
                
                sun_val = safe_int(day_data['avg_sunlight'])
                m4.metric("Solar Energy", f"{sun_val} W/m²" if sun_val is not None else "No Data")

                # --- SECTION: THE AI ORACLE ---
                st.divider()
                st.subheader(f"🤖 Oracle's Advice for {selected_date}")
                
                if st.button(f"✨ Ask the Oracle"):
                    groq_client = Groq(api_key=st.secrets["GROQ_API_KEY"])
                    with st.spinner(f"Connecting to the Oracle..."):
                        try:
                            context = day_data['llm_raw_data']
                            chat_completion = groq_client.chat.completions.create(
                                messages=[
                                    {
                                        "role": "system", 
                                        "content": (
                                            f"You are a Travel Oracle. Today is {ist_now.strftime('%B %d, %Y')}. "
                                            "You provide clothing suggestions and activity tips based on weather data. "
                                            "Use future tense for upcoming dates. Format using Markdown and emojis."
                                        )
                                    },
                                    {
                                        "role": "user", 
                                        "content": f"Data for {selected_date} in {selected_city}: {context}."
                                    }
                                ],
                                model="llama-3.1-8b-instant",
                            )
                            st.markdown(chat_completion.choices[0].message.content)
                        except Exception as e:
                            st.error("The Oracle is currently unavailable. Please try again in a moment.")

else:
    st.info("Establishing connection to the AWS Data Lake...")