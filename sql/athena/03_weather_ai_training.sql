CREATE OR REPLACE VIEW default.weather_ai_training AS
SELECT 
  city, 
  obs_date,
  -- Dynamic tense selection based on the current date
  concat(
    'On ', cast(obs_date as varchar), 
    ', the average temperature in ', city, ' ',
    CASE 
      WHEN obs_date < CURRENT_DATE THEN 'was '
      ELSE 'is expected to be '
    END,
    cast(round(avg(temperature), 1) as varchar), ' degrees Celsius.'
  ) as ai_summary,
  CASE 
    WHEN avg(temperature) < 10 THEN 'Cold'
    WHEN avg(temperature) >= 10 AND avg(temperature) < 25 THEN 'Mild'
    ELSE 'Hot'
  END as climate_category
FROM default.weather_refined
GROUP BY city, obs_date;