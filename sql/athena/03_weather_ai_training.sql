CREATE OR REPLACE VIEW weather_ai_training AS
SELECT 
    city,
    obs_date,
    CONCAT(
        'The city of ', city, 
        ' has an average temperature of ', CAST(avg_temp_c AS VARCHAR), '°C ',
        'with a high of ', CAST(max_temp AS VARCHAR), '°C. ',
        'Average precipitation is ', CAST(avg_precipitation AS VARCHAR), 'mm, ',
        'and the Air Quality Index (AQI) is ', CAST(avg_aqi AS VARCHAR), '. ',
        'Forecast generated at: ', latest_forecast_time
    ) AS ai_summary
FROM default.weather_refined;