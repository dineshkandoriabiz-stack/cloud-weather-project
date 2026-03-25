CREATE EXTERNAL TABLE `weather_data` (
  `city` string,
  `latitude` double,
  `longitude` double,
  `time` string,
  `temperature_2m` double,
  `aqi` double,
  `pm2_5` double,
  `pm10` double,
  `no2` double,
  `so2` double,
  `co` double,
  `precipitation` double,
  `rain` double,
  `showers` double,
  `shortwave_radiation` double
)
PARTITIONED BY (`year` string, `month` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://open-meteo-lake-dinesh-kandoria/raw_data/'
TBLPROPERTIES ('skip.header.line.count'='1');