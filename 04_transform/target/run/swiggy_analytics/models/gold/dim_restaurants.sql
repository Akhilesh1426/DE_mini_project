
    

    create  table
      "swiggy"."main"."dim_restaurants__dbt_tmp"
  
    
    as (
      

SELECT 
    restaurant_id,
    name as restaurant_name,
    cuisine_type,
    city,
    avg_rating,
    is_active
FROM "swiggy"."main"."silver_restaurants"
    );
    
  