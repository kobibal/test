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
)

SELECT
    dl.locality AS site,
    COUNT(s.id) AS total_launches,
    AVG(total_mass_kg) AS avg_payload_mass
FROM stg AS s
LEFT JOIN main AS m
    ON s.id = m.id
LEFT JOIN dim_launchpads AS dl
    ON s.launchpad = dl.id
GROUP BY 1;
