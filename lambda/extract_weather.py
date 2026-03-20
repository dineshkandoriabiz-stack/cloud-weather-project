import json
import csv
import http.client
import boto3
from datetime import datetime
import os
import time

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'open-meteo-lake-dinesh-kandoria' # Keep your exact bucket name
    
    # --- 50 GLOBAL TECH HUBS ---
    cities = {
        # --- NORTH AMERICA (12) ---
        "San_Francisco":    {"lat": 37.7749,  "lon": -122.4194},
        "New_York":         {"lat": 40.7128,  "lon": -74.0060},
        "Seattle":          {"lat": 47.6062,  "lon": -122.3321},
        "Austin":           {"lat": 30.2672,  "lon": -97.7431},
        "Boston":           {"lat": 42.3601,  "lon": -71.0589},
        "Los_Angeles":      {"lat": 34.0522,  "lon": -118.2437},
        "Chicago":          {"lat": 41.8781,  "lon": -87.6298},
        "Toronto":          {"lat": 43.6510,  "lon": -79.3470},
        "Vancouver":        {"lat": 49.2827,  "lon": -123.1207},
        "Montreal":         {"lat": 45.5017,  "lon": -73.5673},
        "Mexico_City":      {"lat": 19.4326,  "lon": -99.1332},
        "Washington_DC":    {"lat": 38.9072,  "lon": -77.0369},
        # --- EUROPE (12) ---
        "London":           {"lat": 51.5074,  "lon": -0.1278},
        "Berlin":           {"lat": 52.5200,  "lon": 13.4050},
        "Amsterdam":        {"lat": 52.3676,  "lon": 4.9041},
        "Stockholm":        {"lat": 59.3293,  "lon": 18.0686},
        "Paris":            {"lat": 48.8566,  "lon": 2.3522},
        "Dublin":           {"lat": 53.3498,  "lon": -6.2603},
        "Zurich":           {"lat": 47.3769,  "lon": 8.5417},
        "Helsinki":         {"lat": 60.1699,  "lon": 24.9384},
        "Tallinn":          {"lat": 59.4370,  "lon": 24.7536},
        "Barcelona":        {"lat": 41.3851,  "lon": 2.1734},
        "Warsaw":           {"lat": 52.2297,  "lon": 21.0122},
        "Lisbon":           {"lat": 38.7169,  "lon": -9.1399},
        # --- MIDDLE EAST (4) ---
        "Tel_Aviv":         {"lat": 32.0853,  "lon": 34.7818},
        "Dubai":            {"lat": 25.2048,  "lon": 55.2708},
        "Riyadh":           {"lat": 24.7136,  "lon": 46.6753},
        "Abu_Dhabi":        {"lat": 24.4539,  "lon": 54.3773},
        # --- ASIA (14) ---
        "Bangalore":        {"lat": 12.9716,  "lon": 77.5946},
        "Tokyo":            {"lat": 35.6895,  "lon": 139.6917},
        "Beijing":          {"lat": 39.9042,  "lon": 116.4074},
        "Shanghai":         {"lat": 31.2304,  "lon": 121.4737},
        "Shenzhen":         {"lat": 22.5431,  "lon": 114.0579},
        "Seoul":            {"lat": 37.5665,  "lon": 126.9780},
        "Singapore":        {"lat": 1.3521,   "lon": 103.8198},
        "Hyderabad":        {"lat": 17.3850,  "lon": 78.4867},
        "Mumbai":           {"lat": 19.0760,  "lon": 72.8777},
        "Jakarta":          {"lat": -6.2088,  "lon": 106.8456},
        "Kuala_Lumpur":     {"lat": 3.1390,   "lon": 101.6869},
        "Taipei":           {"lat": 25.0330,  "lon": 121.5654},
        "Ho_Chi_Minh_City": {"lat": 10.8231,  "lon": 106.6297},
        "Osaka":            {"lat": 34.6937,  "lon": 135.5023},
        # --- AFRICA (4) ---
        "Nairobi":          {"lat": -1.2921,  "lon": 36.8219},
        "Lagos":            {"lat": 6.5244,   "lon": 3.3792},
        "Cape_Town":        {"lat": -33.9249, "lon": 18.4241},
        "Cairo":            {"lat": 30.0444,  "lon": 31.2357},
        # --- SOUTH AMERICA (4) ---
        "Sao_Paulo":        {"lat": -23.5505, "lon": -46.6333},
        "Buenos_Aires":     {"lat": -34.6037, "lon": -58.3816},
        "Bogota":           {"lat": 4.7110,   "lon": -74.0721},
        "Santiago":         {"lat": -33.4489, "lon": -70.6693},
        # --- OCEANIA (2) ---
        "Sydney":           {"lat": -33.8688, "lon": 151.2093},
        "Melbourne":        {"lat": -37.8136, "lon": 144.9631},
    }
    
    now = datetime.utcnow()
    year = now.strftime('%Y')
    month = now.strftime('%m')
    timestamp = now.strftime('%d_%H%M')

    total_records = 0

    # 2. Loop through each city
    for city_name, coords in cities.items():
        conn = http.client.HTTPSConnection("api.open-meteo.com")
        # Extended URL with temperature, air quality, precipitation, and solar radiation
        url = f"/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&hourly=temperature_2m,air_quality,precipitation,rain,showers,shortwave_radiation&air_quality_variables=aqi,pm2_5,pm10,no2,so2,co&past_days=7"
        
        try:
            conn.request("GET", url)
            response = conn.getresponse()
            data = json.loads(response.read().decode("utf-8"))
            
            # 3. Create a unique CSV for each city in the /tmp/ directory
            tmp_filename = f"/tmp/weather_{city_name}.csv"
            
            with open(tmp_filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'city', 'latitude', 'longitude', 'time', 'temperature_2m',
                    'aqi', 'pm2_5', 'pm10', 'no2', 'so2', 'co',
                    'precipitation', 'rain', 'showers', 'shortwave_radiation'
                ])
                
                times = data['hourly']['time']
                temps = data['hourly']['temperature_2m']
                
                # Extract air quality data (nested under 'air_quality' key)
                air_quality_data = data['hourly'].get('air_quality', {})
                aqi_data = air_quality_data.get('aqi', [None] * len(times))
                pm2_5_data = air_quality_data.get('pm2_5', [None] * len(times))
                pm10_data = air_quality_data.get('pm10', [None] * len(times))
                no2_data = air_quality_data.get('no2', [None] * len(times))
                so2_data = air_quality_data.get('so2', [None] * len(times))
                co_data = air_quality_data.get('co', [None] * len(times))
                
                # Extract precipitation and radiation data
                precipitation = data['hourly'].get('precipitation', [None] * len(times))
                rain = data['hourly'].get('rain', [None] * len(times))
                showers = data['hourly'].get('showers', [None] * len(times))
                solar_radiation = data['hourly'].get('shortwave_radiation', [None] * len(times))
                
                for i, t in enumerate(times):
                    writer.writerow([
                        city_name,
                        coords['lat'],
                        coords['lon'],
                        t,
                        temps[i] if i < len(temps) else None,
                        aqi_data[i] if i < len(aqi_data) else None,
                        pm2_5_data[i] if i < len(pm2_5_data) else None,
                        pm10_data[i] if i < len(pm10_data) else None,
                        no2_data[i] if i < len(no2_data) else None,
                        so2_data[i] if i < len(so2_data) else None,
                        co_data[i] if i < len(co_data) else None,
                        precipitation[i] if i < len(precipitation) else None,
                        rain[i] if i < len(rain) else None,
                        showers[i] if i < len(showers) else None,
                        solar_radiation[i] if i < len(solar_radiation) else None
                    ])
                    total_records += 1
            
            # 4. Upload to S3 with the city name in the file path
            s3_key = f"raw_data/year={year}/month={month}/{city_name}_weather_{timestamp}.csv"
            s3.upload_file(tmp_filename, bucket_name, s3_key)
            print(f"Successfully uploaded {city_name} data to S3.")
            
        except Exception as e:
            print(f"Error processing {city_name}: {e}")
            # We don't raise e here so one failed city doesn't break the rest!
        finally:
            conn.close()
            
        # Brief pause to avoid hammering the free Open-Meteo API too fast
        time.sleep(0.1) 

    return {
        'statusCode': 200,
        'body': json.dumps(f'Successfully processed {total_records} records across 50 cities!')
    }