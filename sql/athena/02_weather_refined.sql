CREATE OR REPLACE VIEW weather_refined AS
SELECT 
    city,
    ROUND(AVG(temperature_2m), 2) AS avg_temp_c,
    ROUND(AVG(precipitation), 2) AS avg_precipitation,
    ROUND(AVG(aqi), 2) AS avg_aqi,
    MAX(temperature_2m) AS max_temp,
    MIN(temperature_2m) AS min_temp,
    COUNT(*) AS total_hourly_records,
    MAX(`time`) AS latest_forecast_time
FROM default.weather_raw
GROUP BY city;