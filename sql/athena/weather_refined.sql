CREATE OR REPLACE VIEW "default"."weather_refined" AS
SELECT 
    "city",
    TRY_CAST("latitude" AS double) AS "latitude",
    TRY_CAST("longitude" AS double) AS "longitude",
    -- Weather Data
    TRY_CAST("temperature_2m" AS double) AS "temperature",
    TRY_CAST("precipitation" AS double) AS "precipitation",
    TRY_CAST("rain" AS double) AS "rain",
    TRY_CAST("showers" AS double) AS "showers",
    TRY_CAST("shortwave_radiation" AS double) AS "sunlight",
    -- Air Quality Data
    TRY_CAST("aqi" AS double) AS "aqi",
    TRY_CAST("pm2_5" AS double) AS "pm2_5",
    TRY_CAST("pm10" AS double) AS "pm10",
    TRY_CAST("no2" AS double) AS "no2",
    TRY_CAST("so2" AS double) AS "so2",
    TRY_CAST("co" AS double) AS "co",
    -- Time Parsing (Where your "Malformed" error was happening)
    date_parse("time", '%Y-%m-%dT%H:%i') AS "observation_time",
    CAST(substr("time", 1, 10) AS DATE) AS "obs_date",
    CAST(ingestion_time AS TIMESTAMP) as "batch_processed_at",
    "year",
    "month"
FROM "default"."weather_data";
