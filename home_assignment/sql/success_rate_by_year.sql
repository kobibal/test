SELECT 
EXTRACT(YEAR FROM date_utc) AS year,
1.0*SUM(CASE WHEN success = True THEN 1 ELSE 0 END) / COUNT(*) AS success_rate
FROM stg
GROUP BY 1