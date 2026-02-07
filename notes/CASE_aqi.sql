CASE
    WHEN aqi <= 50 THEN 'Good'
    WHEN aqi <= 100 THEN 'Moderate'
    WHEN aqi <= 150 THEN 'Unhealthy for Sensitive Groups'
    WHEN aqi <= 200 THEN 'Unhealthy'
    ELSE 'Very Unhealthy'
END as aqi_category,