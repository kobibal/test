DROP TABLE IF EXISTS public.aggregated;

CREATE TABLE public.aggregated AS
WITH prep AS (
    SELECT
        id,
        UNNEST(CAST(payloads AS TEXT[])) AS payloads_id
    FROM public.raw_level
),

main AS (
    SELECT
        p.id,
        SUM(mass_kg) AS total_mass_kg
    FROM prep AS p
    LEFT JOIN dim_payloads AS dp
        ON p.payloads_id = dp.id
    GROUP BY 1
)

SELECT
    COUNT(s.id) AS total_launches,
    SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) AS successful_launches,
    AVG(total_mass_kg) AS avg_payload_mass,
    AVG(
        CASE
            WHEN date_utc >= COALESCE(static_fire_date_utc, date_utc)
            THEN EXTRACT(
                EPOCH FROM (date_utc - COALESCE(static_fire_date_utc, date_utc))
            ) / 3600
        END
    ) AS hour_diff -- assuming that if static_fire_date is null, meaning that there was no delay,
                   -- also filter out records with static dates greater than the actual date because it's not valid
FROM public.raw_level AS s
LEFT JOIN main AS m
    ON s.id = m.id;
