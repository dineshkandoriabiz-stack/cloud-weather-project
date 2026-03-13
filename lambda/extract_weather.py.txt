import json
import boto3
import http.client
from datetime import datetime

def lambda_handler(event, context):
    BUCKET = "open-meteo-lake-dinesh-kandoria"
    LAT, LON = "51.5074", "-0.1278" # Fixed for now
    
    try:
        conn = http.client.HTTPSConnection("api.open-meteo.com")
        # Request: Hourly Temp for 7 days past + 7 days future
        path = f"/v1/forecast?latitude={LAT}&longitude={LON}&hourly=temperature_2m&past_days=7&forecast_days=7"
        conn.request("GET", path)
        
        data = json.loads(conn.getresponse().read().decode())
        hourly = data['hourly']
        times, temps = hourly['time'], hourly['temperature_2m']
        
        # Build CSV with 336 rows
        csv_body = "latitude,longitude,temperature,time\n"
        for i in range(len(times)):
            csv_body += f"{LAT},{LON},{temps[i]},{times[i]}\n"
        
        # Upload
        s3 = boto3.client('s3')
        # Organize by Year/Month folders automatically
        now = datetime.now()
        file_path = f"raw_data/year={now.year}/month={now.month:02d}/weather_{now.strftime('%d_%H%M')}.csv"
        
        s3.put_object(Bucket=BUCKET, Key=file_path, Body=csv_body)
        return {"status": "success", "rows_saved": len(times)}

    except Exception as e:
        return {"status": "error", "message": str(e)}
