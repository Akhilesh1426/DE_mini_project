{{ config(materialized='table') }}

SELECT 
    restaurant_id,
    name as restaurant_name,
    cuisine_type,
    city,
    avg_rating,
    is_active
FROM {{ ref('silver_restaurants') }}
