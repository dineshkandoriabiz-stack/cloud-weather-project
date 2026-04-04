import streamlit as st
import boto3
import pandas as pd
import time
import io
import math
from groq import Groq
from datetime import datetime, date

# --- CONFIGURATION ---
DATABASE = 'default'
TABLE = 'view_ai_travel_context' 
S3_OUTPUT = 's3://open-meteo-lake-dinesh-kandoria/athena_results/' 

st.set_page_config(page_title="Global Weather AI Oracle", page_icon="🌍", layout="wide")

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

health_query = 'SELECT MAX("batch_processed_at") as last_run FROM "weather_refined"'
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
- ☁️ AWS S3 
- 🧠 AWS Athena   
- 🕒 AWS EventBridge   
- 🕒 AWS Lambda Functions (Python)                                     
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
            # Convert forecast_date to datetime objects for easy filtering
            city_data['forecast_date'] = pd.to_datetime(city_data['forecast_date']).dt.date

            # --- WEATHER TREND CHART ---
            st.subheader(f"📈 7-Day Forecast Trend: {selected_city}")
            
            # Prepare data for charting
            chart_df = city_data.copy()
            chart_df = chart_df.set_index('forecast_date')

            col_chart1, col_chart2 = st.columns(2)
            with col_chart1:
                st.line_chart(chart_df['avg_temp'], color="#FF4B4B")
                st.caption("Temperature Forecast (°C)")
            with col_chart2:
                # Fill NaNs for chart display only so it doesn't break
                st.area_chart(chart_df['avg_aqi'].fillna(0), color="#29B5E8")
                st.caption("Air Quality Index (AQI) Forecast")

            st.divider()

            # --- DAILY SELECTION & METRICS ---
            st.subheader("📍 Daily Deep-Dive")
            
            # FILTER: Only show dates from TODAY onwards to hide historical noise
            today = date.today()
            available_dates = sorted([d for d in city_data['forecast_date'].unique() if d >= today])
            
            if not available_dates:
                 st.warning("No future forecast data available. Please check back later.")
            else:
                selected_date = st.select_slider("Pick a date to consult the Oracle:", options=available_dates)
                
                # Filter data for the specific chosen date
                day_data = city_data[city_data['forecast_date'] == selected_date].iloc[0]
                
                m1, m2, m3, m4 = st.columns(4)
                
                # Metric 1: Temperature
                m1.metric("Avg Temp", f"{day_data['avg_temp']:.1f}°C")
                
                # Metric 2: AQI (THE DEFENSIVE FIX)
                aqi = safe_int(day_data['avg_aqi'])
                m2.metric("Air Quality", f"{aqi} AQI" if aqi is not None else "No Data")
                
                # Metric 3: Rain
                m3.metric("Expected Rain", f"{day_data['total_precip']:.1f}mm")
                
                # Metric 4: Solar (THE DEFENSIVE FIX)
                sun = safe_int(day_data['avg_sunlight'])
                m4.metric("Solar Energy", f"{sun} W/m²" if sun is not None else "No Data")

                # --- THE AI ORACLE ---
                st.divider()
                st.subheader(f"🤖 Oracle's Advice for {selected_date}")
                
                today_str = datetime.now().strftime("%B %d, %Y")
                
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
                                            f"You are a Travel Oracle. Today is {today_str}. "
                                            "You will be given weather data for a specific date. "
                                            "If the forecast date is in the future relative to today, "
                                            "use future tense. Do not say 'today' if the date is in the future. "
                                            "Format with Markdown and emojis."
                                        )
                                    },
                                    {
                                        "role": "user", 
                                        "content": f"Here is the data for {selected_date}: {raw_context}. Please provide travel tips and clothing suggestions."
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