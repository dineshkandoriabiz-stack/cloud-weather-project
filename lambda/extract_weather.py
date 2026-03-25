import json
import csv
import http.client
import boto3
from datetime import datetime, timezone
import os
import time

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'open-meteo-lake-dinesh-kandoria'
    
    # Environment check for local debugging vs AWS execution
    local_test = os.getenv('LOCAL_TEST') == 'true'
    tmp_dir = './tmp' if local_test else '/tmp'
    os.makedirs(tmp_dir, exist_ok=True)

    cities = {
        "San_Francisco": {"lat": 37.7749, "lon": -122.4194},
        "New_York": {"lat": 40.7128, "lon": -74.0060},
        "Seattle": {"lat": 47.6062, "lon": -122.3321},
        "Austin": {"lat": 30.2672, "lon": -97.7431},
        "Boston": {"lat": 42.3601, "lon": -71.0589},
        "Los_Angeles": {"lat": 34.0522, "lon": -118.2437},
        "Chicago": {"lat": 41.8781, "lon": -87.6298},
        "Toronto": {"lat": 43.6510, "lon": -79.3470},
        "Vancouver": {"lat": 49.2827, "lon": -123.1207},
        "Montreal": {"lat": 45.5017, "lon": -73.5673},
        "Mexico_City": {"lat": 19.4326, "lon": -99.1332},
        "Washington_DC": {"lat": 38.9072, "lon": -77.0369},
        "London": {"lat": 51.5074, "lon": -0.1278},
        "Berlin": {"lat": 52.5200, "lon": 13.4050},
        "Amsterdam": {"lat": 52.3676, "lon": 4.9041},
        "Stockholm": {"lat": 59.3293, "lon": 18.0686},
        "Paris": {"lat": 48.8566, "lon": 2.3522},
        "Dublin": {"lat": 53.3498, "lon": -6.2603},
        "Zurich": {"lat": 47.3769, "lon": 8.5417},
        "Helsinki": {"lat": 60.1699, "lon": 24.9384},
        "Tallinn": {"lat": 59.4370, "lon": 24.7536},
        "Barcelona": {"lat": 41.3851, "lon": 2.1734},
        "Warsaw": {"lat": 52.2297, "lon": 21.0122},
        "Lisbon": {"lat": 38.7169, "lon": -9.1399},
        "Tel_Aviv": {"lat": 32.0853, "lon": 34.7818},
        "Dubai": {"lat": 25.2048, "lon": 55.2708},
        "Riyadh": {"lat": 24.7136, "lon": 46.6753},
        "Abu_Dhabi": {"lat": 24.4539, "lon": 54.3773},
        "Bangalore": {"lat": 12.9716, "lon": 77.5946},
        "Tokyo": {"lat": 35.6895, "lon": 139.6917},
        "Beijing": {"lat": 39.9042, "lon": 116.4074},
        "Shanghai": {"lat": 31.2304, "lon": 121.4737},
        "Shenzhen": {"lat": 22.5431, "lon": 114.0579},
        "Seoul": {"lat": 37.5665, "lon": 126.9780},
        "Singapore": {"lat": 1.3521, "lon": 103.8198},
        "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
        "Mumbai": {"lat": 19.0760, "lon": 72.8777},
        "Jakarta": {"lat": -6.2088, "lon": 106.8456},
        "Kuala_Lumpur": {"lat": 3.1390, "lon": 101.6869},
        "Taipei": {"lat": 25.0330, "lon": 121.5654},
        "Ho_Chi_Minh_City": {"lat": 10.8231, "lon": 106.6297},
        "Osaka": {"lat": 34.6937, "lon": 135.5023},
        "Nairobi": {"lat": -1.2921, "lon": 36.8219},
        "Lagos": {"lat": 6.5244, "lon": 3.3792},
        "Cape_Town": {"lat": -33.9249, "lon": 18.4241},
        "Cairo": {"lat": 30.0444, "lon": 31.2357},
        "Sao_Paulo": {"lat": -23.5505, "lon": -46.6333},
        "Buenos_Aires": {"lat": -34.6037, "lon": -58.3816},
        "Bogota": {"lat": 4.7110, "lon": -74.0721},
        "Santiago": {"lat": -33.4489, "lon": -70.6693},
        "Sydney": {"lat": -33.8688, "lon": 151.2093},
        "Melbourne": {"lat": -37.8136, "lon": 144.9631},
    }

    now = datetime.now(timezone.utc)
    year = now.strftime('%Y')
    month = now.strftime('%m')
    timestamp = now.strftime('%d_%H%M')

    total_records = 0
    
    # Define fields for CSV and API
    weather_fields = ['temperature_2m', 'precipitation', 'rain', 'showers', 'shortwave_radiation']
    air_quality_fields = ['european_aqi', 'pm2_5', 'pm10', 'nitrogen_dioxide', 'sulphur_dioxide', 'carbon_monoxide']
    csv_headers = ['city', 'latitude', 'longitude', 'time', 'temperature_2m', 'aqi', 'pm2_5', 'pm10', 'no2', 'so2', 'co', 'precipitation', 'rain', 'showers', 'shortwave_radiation']

    def fetch_json(base_host, path):
        conn = http.client.HTTPSConnection(base_host)
        try:
            conn.request('GET', path)
            resp = conn.getresponse()
            if resp.status != 200:
                print(f"Error: {base_host}{path} returned {resp.status}")
                return None
            return json.loads(resp.read().decode('utf-8'))
        finally:
            conn.close()

    def get_series(source, field_list, num_times):
        if not source or 'hourly' not in source:
            return {f: [None] * num_times for f in field_list}
        output = {}
        for field in field_list:
            data = source['hourly'].get(field, [None] * num_times)
            # Pad if shorter than expected time range
            if len(data) < num_times:
                data = data + [None] * (num_times - len(data))
            output[field] = data
        return output

    for city_name, coords in cities.items():
        # Optimization: Forecast only, timezone=auto for localized accuracy
        weather_path = f"/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&hourly={','.join(weather_fields)}&forecast_days=7&timezone=auto"
        air_path = f"/v1/air-quality?latitude={coords['lat']}&longitude={coords['lon']}&hourly={','.join(air_quality_fields)}&forecast_days=7&timezone=auto"

        try:
            weather_data = fetch_json('api.open-meteo.com', weather_path)
            air_data = fetch_json('air-quality-api.open-meteo.com', air_path)

            if not weather_data or 'hourly' not in weather_data or 'time' not in weather_data['hourly']:
                print(f"Skipping {city_name}: Invalid response structure")
                continue

            times = weather_data['hourly']['time']
            num_rows = len(times)
            
            weather_series = get_series(weather_data, weather_fields, num_rows)
            air_series = get_series(air_data, air_quality_fields, num_rows)

            tmp_filename = os.path.join(tmp_dir, f"weather_{city_name}.csv")
            with open(tmp_filename, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(csv_headers)

                for i, t in enumerate(times):
                    row = [
                        city_name,
                        coords['lat'],
                        coords['lon'],
                        t,
                        weather_series['temperature_2m'][i],
                        air_series['european_aqi'][i],
                        air_series['pm2_5'][i],
                        air_series['pm10'][i],
                        air_series['nitrogen_dioxide'][i],
                        air_series['sulphur_dioxide'][i],
                        air_series['carbon_monoxide'][i],
                        weather_series['precipitation'][i],
                        weather_series['rain'][i],
                        weather_series['showers'][i],
                        weather_series['shortwave_radiation'][i],
                    ]
                    writer.writerow(row)
                    total_records += 1

            s3_key = f"raw_data/year={year}/month={month}/{city_name}_weather_{timestamp}.csv"
            
            if not local_test:
                s3.upload_file(tmp_filename, bucket_name, s3_key)
                print(f"✅ Uploaded {city_name} to S3")
            else:
                print(f"🏠 Local Test: {city_name} saved to {tmp_filename}")

        except Exception as e:
            print(f"❌ Error for {city_name}: {str(e)}")

        time.sleep(0.1) # Safe API spacing

    return {
        'statusCode': 200, 
        'body': json.dumps(f'Successfully processed {total_records} rows (Today + 7 Days) for {len(cities)} cities!')
    }

if __name__ == "__main__":
    os.environ['LOCAL_TEST'] = 'true'
    lambda_handler({}, None)