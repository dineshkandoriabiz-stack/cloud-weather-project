CREATE OR REPLACE VIEW "default"."view_ai_travel_context" AS 
SELECT 
    "city",
    "obs_date" AS "forecast_date",
    -- Aggregating the refined data
    ROUND(AVG("temperature"), 1) as "avg_temp",
    ROUND(AVG("aqi"), 0) as "avg_aqi",
    ROUND(AVG("pm2_5"), 1) as "avg_pm25",
    ROUND(SUM("precipitation"), 1) as "total_precip",
    ROUND(AVG("sunlight"), 0) as "avg_sunlight",
    
    -- The Raw Data Packet for LLM interpretation
    CONCAT(
        'City: ', "city", ' | Date: ', CAST("obs_date" AS VARCHAR), 
        ' | Avg Temp: ', CAST(ROUND(AVG("temperature"), 1) AS VARCHAR), 'C',
        ' | AQI: ', CAST(ROUND(AVG("aqi"), 0) AS VARCHAR), 
        ' | PM2.5: ', CAST(ROUND(AVG("pm2_5"), 1) AS VARCHAR),
        ' | Total Rain: ', CAST(ROUND(SUM("precipitation"), 1) AS VARCHAR), 'mm',
        ' | Sunlight: ', CAST(ROUND(AVG("sunlight"), 0) AS VARCHAR), ' W/m2.'
    ) as "llm_raw_data"

FROM "default"."weather_refined"
WHERE "obs_date" >= CURRENT_DATE
GROUP BY "city", "obs_date";