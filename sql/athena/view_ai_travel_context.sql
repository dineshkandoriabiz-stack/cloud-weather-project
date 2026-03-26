CREATE OR REPLACE VIEW "view_ai_forecast_context" AS 
SELECT 
    "city",
    "obs_date",
    -- Create the AI summary for the specific date selected
    CONCAT(
        'Forecast for ', "city", ' on ', CAST("obs_date" AS VARCHAR), ': ',
        'The expected average temperature is ', CAST(ROUND(AVG("temperature"), 1) AS VARCHAR), '°C. ',
        'The predicted AQI is ', CAST(ROUND(AVG("aqi"), 0) AS VARCHAR), 
        ' with precipitation around ', CAST(ROUND(SUM("precipitation"), 1) AS VARCHAR), 'mm.'
    ) AS "ai_prompt_context",
    -- Raw metrics for your dashboard charts
    ROUND(AVG("temperature"), 1) AS "temp",
    ROUND(AVG("aqi"), 0) AS "aqi",
    ROUND(SUM("precipitation"), 1) AS "precip",
    ROUND(AVG("sunlight"), 0) AS "sunlight"
FROM "weather_refined"
GROUP BY "city", "obs_date";