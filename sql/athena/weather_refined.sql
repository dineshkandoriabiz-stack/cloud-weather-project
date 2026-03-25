CREATE OR REPLACE VIEW "default"."weather_refined" AS
SELECT 
    "city",
    CAST("latitude" AS double) AS "latitude",
    CAST("longitude" AS double) AS "longitude",
    -- Weather Data
    CAST("temperature_2m" AS double) AS "temperature",
    CAST("precipitation" AS double) AS "precipitation",
    CAST("rain" AS double) AS "rain",
    CAST("showers" AS double) AS "showers",
    CAST("shortwave_radiation" AS double) AS "sunlight",
    -- Air Quality Data
    CAST("aqi" AS double) AS "aqi",
    CAST("pm2_5" AS double) AS "pm2_5",
    CAST("pm10" AS double) AS "pm10",
    CAST("no2" AS double) AS "no2",
    CAST("so2" AS double) AS "so2",
    CAST("co" AS double) AS "co",
    -- Time Parsing
    date_parse("time", '%Y-%m-%dT%H:%i') AS "observation_time",
    CAST(substr("time", 1, 10) AS DATE) AS "obs_date",
    "year",
    "month"
FROM "default"."weather_data";

