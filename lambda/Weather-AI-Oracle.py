import boto3
import json
import http.client
import time
import os

# --- PROFESSIONAL CONFIGURATION ---
# These will be pulled from AWS Lambda Environment Variables or your local .env
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "open-meteo-lake-dinesh-kandoria")
ATHENA_DB = os.environ.get("ATHENA_DB", "default")

def lambda_handler(event, context):
    athena = boto3.client('athena')
    
    # 1. Athena Query Execution
    query = "SELECT ai_summary FROM weather_ai_training LIMIT 1;"
    s3_output = f"s3://{S3_BUCKET_NAME}/athena_results/"
    
    print(f"Starting Athena Query in bucket: {S3_BUCKET_NAME}")
    execution = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DB},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    execution_id = execution['QueryExecutionId']
    
    # 2. Smart Waiter (Better than time.sleep)
    status = 'RUNNING'
    while status in ['RUNNING', 'QUEUED']:
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['FAILED', 'CANCELLED']:
            return {"statusCode": 500, "error": f"Athena Query {status}"}
        time.sleep(1)

    try:
        # 3. Fetch Results
        results = athena.get_query_results(QueryExecutionId=execution_id)
        weather_text = results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
        print(f"Weather Context: {weather_text}")

        # 4. Call Groq AI
        conn = http.client.HTTPSConnection("api.groq.com")
        payload = json.dumps({
            "model": "llama-3.1-8b-instant",
            "messages": [
                {"role": "system", "content": "You are a helpful travel assistant. Keep advice short."},
                {"role": "user", "content": f"Based on this weather: {weather_text}, suggest 3 activities and what to wear."}
            ],
            "max_tokens": 300
        })
        
        headers = {
            'Authorization': f'Bearer {GROQ_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        conn.request("POST", "/openai/v1/chat/completions", payload, headers)
        res = conn.getresponse()
        ai_response = json.loads(res.read().decode())
        
        advice = ai_response['choices'][0]['message']['content']
        
        return {
            'statusCode': 200,
            'weather_context': weather_text,
            'ai_travel_advice': advice
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {'statusCode': 500, 'error': str(e)}
