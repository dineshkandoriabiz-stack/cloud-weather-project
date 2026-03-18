import streamlit as st
import boto3
import pandas as pd
import time
import io
from groq import Groq
# --- CONFIGURATION ---
DATABASE = 'default'
TABLE = 'weather_ai_training'
# Keep your exact bucket name, but add /athena_results/ at the end
S3_OUTPUT = 's3://open-meteo-lake-dinesh-kandoria/athena_results/' 
REGION = 'ap-southeast-2' # Adjust if your AWS region is different

st.set_page_config(page_title="Global Weather AI Oracle", page_icon="🌍", layout="wide")

# --- AWS ATHENA HELPER FUNCTION ---
@st.cache_data(ttl=3600) # Caches the data for 1 hour so it doesn't query AWS on every click
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

    # Start the query
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT}
    )
    query_id = response['QueryExecutionId']

    # Wait for the query to finish
    with st.spinner("Querying AWS Athena Data Lake..."):
        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(2)

    if status == 'SUCCEEDED':
        # Fetch the results from S3
        s3_key = f"athena_results/{query_id}.csv"
        bucket_name = S3_OUTPUT.split('/')[2]
        
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        return df
    else:
        st.error(f"Athena query failed: {stats['QueryExecution']['Status']['StateChangeReason']}")
        return pd.DataFrame()

# --- UI FRONTEND ---
st.title("🌍 Global Tech Hubs: AI Weather Oracle")
st.markdown("A Serverless Data Lakehouse built on AWS S3, Athena, and EventBridge.")

# 1. Fetch the unique list of cities directly from Athena
city_query = f"SELECT DISTINCT city FROM {TABLE} ORDER BY city"
cities_df = fetch_athena_data(city_query)

if not cities_df.empty:
    cities_list = cities_df['city'].tolist()
    
    # 2. Create a dropdown menu for the user
    selected_city = st.selectbox("Select a Global Tech Hub:", cities_list)

    # 3. When a city is selected, query its specific data
    if selected_city:
        data_query = f"SELECT obs_date, ai_summary, climate_category FROM {TABLE} WHERE city = '{selected_city}' ORDER BY obs_date DESC"
        city_data = fetch_athena_data(data_query)
        
        st.subheader(f"Latest Insights for {selected_city}")
        
        # Display the AI Summary string we built in Athena
        latest_summary = city_data['ai_summary'].iloc[0] if not city_data.empty else "No data available."
        st.info(latest_summary)

        # Show the raw data table
        st.dataframe(city_data, use_container_width=True)

        # --- THE ON-DEMAND AI ORACLE ---
        st.divider()
        st.subheader(f"🤖 Ask the AI Oracle about {selected_city}")

        # Initialize the Groq client using your hidden secrets
        groq_client = Groq(api_key=st.secrets["GROQ_API_KEY"])

        # Give the user a button to generate travel advice
        if st.button(f"Generate Travel & Clothing Advice for {selected_city}"):
            with st.spinner("The Oracle is consulting the data..."):
                try:
                    # We pass the Athena summary as the context!
                    chat_completion = groq_client.chat.completions.create(
                        messages=[
                            {"role": "system", "content": "You are a helpful travel assistant. Keep advice short, formatting it with bullet points."},
                            {"role": "user", "content": f"Based on this weather: '{latest_summary}', suggest 3 activities and what to wear."}
                        ],
                        model="llama-3.1-8b-instant",
                        max_tokens=300
                    )

                    # Display the AI's response in a nice UI box
                    st.success(chat_completion.choices[0].message.content)
                except Exception as e:
                    st.error(f"AI Connection Error: {e}")

else:
    st.warning("No data found in Athena. Make sure your extraction Lambda has run successfully!")