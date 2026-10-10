
  
    
    

    create  table
      "swiggy"."main"."silver_orders__dbt_tmp"
  
    as (
      

WITH raw_orders AS (
    SELECT * FROM delta_scan('../data/delta_lake/bronze/orders')
),
deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY timestamp DESC) as rn
    FROM raw_orders
)
SELECT 
    event_id,
    event_type,
    CAST(timestamp AS TIMESTAMP) AS event_timestamp,
    order_id,
    customer_id,
    restaurant_id,
    total_amount,
    status,
    delivery_address
FROM deduplicated
WHERE rn = 1
    );
  
  