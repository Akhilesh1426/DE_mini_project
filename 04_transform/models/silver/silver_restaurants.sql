{{ config(materialized='table') }}

WITH raw_rest AS (
    SELECT * FROM delta_scan('../data/delta_lake/bronze/restaurants')
)
SELECT 
    restaurant_id,
    name,
    cuisine_type,
    city,
    CAST(avg_rating AS DOUBLE) as avg_rating,
    is_active
FROM raw_rest
