CREATE OR REPLACE VIEW default.weather_refined AS
SELECT 
  city,  -- NEW COLUMN
  CAST(latitude AS double) AS latitude,
  CAST(longitude AS double) AS longitude,
  CAST(temperature AS double) AS temperature,
  date_parse(time, '%Y-%m-%dT%H:%M') AS observation_time,
  CAST(substr(time, 1, 10) AS date) AS obs_date,
  CAST(year AS integer) AS year,
  CAST(month AS integer) AS month
FROM default.weather_raw_data;