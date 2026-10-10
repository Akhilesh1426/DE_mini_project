{{ config(materialized='table') }}

WITH stream_orders AS (
    SELECT order_id, customer_id, restaurant_id, total_amount, status, event_timestamp, 'streaming' as source
    FROM {{ ref('silver_orders') }}
),
batch_orders AS (
    SELECT order_id, customer_id, restaurant_id, total_amount, status, event_timestamp, 'batch' as source
    FROM {{ ref('silver_historical_orders') }}
)
SELECT * FROM stream_orders
UNION ALL
SELECT * FROM batch_orders
