
  
    
    

    create  table
      "swiggy"."main"."silver_historical_orders__dbt_tmp"
  
    as (
      

WITH historical AS (
    SELECT * FROM delta_scan('../data/delta_lake/bronze/historical_orders')
)
SELECT 
    order_id,
    customer_id,
    restaurant_id,
    status,
    CAST(total_amount AS DOUBLE) as total_amount,
    delivery_address,
    CAST(placed_at AS TIMESTAMP) as event_timestamp
FROM historical
    );
  
  