CREATE EXTERNAL TABLE IF NOT EXISTS default.weather_raw_data (
  city string,
  latitude double,
  longitude double,
  temperature double,
  time string
)
PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://open-meteo-lake-dinesh-kandoria/raw_data/'
TBLPROPERTIES ('skip.header.line.count'='1');