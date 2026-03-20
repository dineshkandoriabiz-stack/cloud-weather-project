import streamlit as st
import boto3
import pandas as pd
import time
import io
from groq import Groq
from datetime import datetime

# --- CONFIGURATION ---
DATABASE = 'default'
TABLE = 'weather_ai_training'
S3_OUTPUT = 's3://open-meteo-lake-dinesh-kandoria/athena_results/' 
REGION = 'ap-southeast-2'

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

    with st.spinner("Querying AWS Athena Data Lake..."):
        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(2)

    if status == 'SUCCEEDED':
        s3_key = f"athena_results/{query_id}.csv"
        bucket_name = S3_OUTPUT.split('/')[2]
        
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df
    else:
        st.error(f"Athena query failed: {stats['QueryExecution']['Status']['StateChangeReason']}")
        return pd.DataFrame()

# --- UI FRONTEND ---
st.title("🌍 Global 50 Cities: AI Weather Oracle")

# 1. Fetch the unique list of cities directly from Athena
city_query = f"SELECT DISTINCT city FROM {TABLE} ORDER BY city"
cities_df = fetch_athena_data(city_query)

if not cities_df.empty:
    cities_list = cities_df['city'].tolist()
    
    # 2. Create a dropdown menu for the user
    selected_city = st.selectbox("Select a Global 50 City:", cities_list)

    # 3. When a city is selected, query its specific data
    if selected_city:
        # We query all available columns to be safe with schema updates
        data_query = f"SELECT * FROM {TABLE} WHERE city = '{selected_city}'"
        city_data = fetch_athena_data(data_query)
        
        if not city_data.empty:
            
            # Check if the date column exists to apply the Time Travel Blocker
            if 'obs_date' in city_data.columns:
                # 1. Get today's date as a string (Format: YYYY-MM-DD)
                today_str = datetime.now().strftime('%Y-%m-%d')
                
                # Ensure data is string format for clean comparison
                city_data['obs_date'] = city_data['obs_date'].astype(str)
                
                # 2. Filter the Pandas DataFrame to ONLY include today or future dates
                future_data = city_data[city_data['obs_date'] >= today_str]
                
                # 3. Extract unique dates for the dropdown
                available_dates = future_data['obs_date'].unique().tolist()
                
                if not available_dates:
                    st.warning("No future forecast data available for this city right now.")
                    latest_summary = city_data['ai_summary'].iloc[0] if 'ai_summary' in city_data.columns else "No summary available."
                    st.info(latest_summary)
                else:
                    selected_date = st.selectbox("Select Travel Date:", sorted(available_dates))
                    st.subheader(f"Latest Insights for {selected_city} on {selected_date}")
                    
                    # Filter to ONLY show the summary for the chosen date
                    final_display_data = future_data[future_data['obs_date'] == selected_date]
                    latest_summary = final_display_data['ai_summary'].iloc[0]
                    st.info(latest_summary)
            else:
                # Fallback if the 'obs_date' column hasn't been added to the SQL view yet
                st.subheader(f"Latest Insights for {selected_city}")
                latest_summary = city_data['ai_summary'].iloc[0] if 'ai_summary' in city_data.columns else "No summary available."
                st.info(latest_summary)

            # --- THE ON-DEMAND AI ORACLE ---
            st.divider()
            st.subheader(f"🤖 Ask the AI Oracle about {selected_city}")

            # Initialize the Groq client using hidden secrets
            groq_client = Groq(api_key=st.secrets["GROQ_API_KEY"])

            # Button to generate advice
            if st.button(f"Generate Travel, Clothing & Food Advice for {selected_city}"):
                with st.spinner("The Oracle is consulting the data..."):
                    try:
                        # Pass the Athena summary as context
                        chat_completion = groq_client.chat.completions.create(
                            messages=[
                                {"role": "system", "content": "You are a helpful travel assistant. Provide advice formatting it with bullet points. Always include top local food and restaurant recommendations suited for the current weather."},
                                {"role": "user", "content": f"Based on this weather summary: '{latest_summary}', provide Travel & Clothing Advice for {selected_city}. Also, provide information about top food/restaurants to try as per the weather/climate."}
                            ],
                            model="llama-3.1-8b-instant",
                            max_tokens=400
                        )

                        # Display the AI's response
                        st.success(chat_completion.choices[0].message.content)
                    except Exception as e:
                        st.error(f"AI Connection Error: {e}")

else:
    st.warning("No data found in Athena. Make sure your extraction Lambda has run successfully!")