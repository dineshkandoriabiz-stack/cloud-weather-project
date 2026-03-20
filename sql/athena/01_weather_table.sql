CREATE EXTERNAL TABLE IF NOT EXISTS default.weather_raw (
    city STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    `time` STRING,
    temperature_2m DOUBLE,
    aqi DOUBLE,
    pm2_5 DOUBLE,
    pm10 DOUBLE,
    no2 DOUBLE,
    so2 DOUBLE,
    co DOUBLE,
    precipitation DOUBLE,
    rain DOUBLE,
    showers DOUBLE,
    shortwave_radiation DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://open-meteo-lake-dinesh-kandoria/raw_data/'
TBLPROPERTIES ('skip.header.line.count'='1');