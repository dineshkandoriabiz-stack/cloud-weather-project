CREATE OR REPLACE VIEW "view_ai_travel_context" AS 
SELECT 
    "city",
    "obs_date" AS "forecast_date", -- <--- THIS FIXES THE PYTHON ERROR
    ROUND(AVG("temperature"), 1) as "avg_temp",
    ROUND(AVG("aqi"), 0) as "avg_aqi",
    ROUND(SUM("precipitation"), 1) as "total_precip",
    ROUND(AVG("sunlight"), 0) as "avg_sunlight",
    -- Your Python code expects 'llm_raw_data' for the Groq prompt
    CONCAT(
        'Data for ', "city", ' on ', CAST("obs_date" AS VARCHAR), ': ',
        'Temp is ', CAST(ROUND(AVG("temperature"), 1) AS VARCHAR), 'C, ',
        'AQI is ', CAST(ROUND(AVG("aqi"), 0) AS VARCHAR), '.'
    ) AS "llm_raw_data"
FROM "default"."weather_refined"
GROUP BY "city", "obs_date";