SELECT id
FROM postgresql.public.raw_level

-- The increental load also can also happend in the database side 
-- INSERT INTO postgresql.public.raw_level
-- SELECT * 
-- FROM postgresql.public.raw_level_latest
-- WHERE id NOT IN ( SELECT id FROM postgresql.public.raw_level GROUP BY 1)