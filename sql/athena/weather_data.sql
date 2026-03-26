CREATE EXTERNAL TABLE IF NOT EXISTS weather_data (
  city string,
  latitude string,
  longitude string,
  time string,
  temperature_2m string,
  precipitation string,
  rain string,
  showers string,
  shortwave_radiation string,
  aqi string,
  pm2_5 string,
  pm10 string,
  no2 string,
  so2 string,
  co string
)
PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
LOCATION 's3://open-meteo-lake-dinesh-kandoria/weather_data/'
TBLPROPERTIES ("skip.header.line.count"="1");