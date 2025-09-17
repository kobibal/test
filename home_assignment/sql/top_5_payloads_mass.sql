WITH prep AS (
    SELECT
        id,
        UNNEST(CAST(payloads AS TEXT[])) AS payloads_id
    FROM stg
),

main AS (
    SELECT
        p.id,
        SUM(mass_kg) AS total_mass_kg
    FROM prep AS p
    LEFT JOIN dim_payloads AS dp
        ON p.payloads_id = dp.id
    GROUP BY 1
),

rn AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY total_mass_kg DESC) AS rank_num
    FROM main
    WHERE total_mass_kg IS NOT NULL
)

SELECT *
FROM rn
WHERE rank_num <= 5;
