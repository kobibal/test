SELECT
    EXTRACT(YEAR FROM date_utc) AS year,
    MAX(
        CASE
            WHEN date_utc >= COALESCE(static_fire_date_utc, date_utc)
            THEN EXTRACT(
                EPOCH FROM (date_utc - COALESCE(static_fire_date_utc, date_utc))
            ) / 3600
        END
    ) AS max_delay,
    AVG(
        CASE
            WHEN date_utc >= COALESCE(static_fire_date_utc, date_utc)
            THEN EXTRACT(
                EPOCH FROM (date_utc - COALESCE(static_fire_date_utc, date_utc))
            ) / 3600
        END
    ) AS hour_diff -- assuming that if static_fire_date is null meaning that was no delay,
                   -- also filter out records with static date greater than the actual date because it's not valid
FROM stg
GROUP BY 1;
