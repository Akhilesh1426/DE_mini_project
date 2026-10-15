import duckdb

def main():
    db_path = "/home/jovyan/work/data/duckdb/swiggy.db"
    
    try:
        print(f"Connecting to DuckDB at {db_path}...")
        con = duckdb.connect(db_path)
        
        print("Installing and loading delta extension...")
        con.execute("INSTALL delta;")
        con.execute("LOAD delta;")
        
        print("Creating view for live streaming orders...")
        con.execute("""
            CREATE OR REPLACE VIEW swiggy.main.live_orders AS 
            SELECT * FROM delta_scan('/app/delta_lake/bronze/orders')
            WHERE status != 'placed';
        """)
        
        print("Creating enriched view for live streaming orders with restaurant dimensions...")
        con.execute("""
            CREATE OR REPLACE VIEW swiggy.main.v_live_orders AS 
            SELECT 
                o.order_id,
                o.customer_id,
                o.restaurant_id,
                COALESCE(r.restaurant_name, 'Other Restaurant') as restaurant_name,
                COALESCE(r.cuisine_type, 'Indian') as cuisine_type,
                COALESCE(r.city, 'Bangalore') as city,
                COALESCE(r.avg_rating, 4.2) as avg_rating,
                o.total_amount,
                o.status,
                o.event_type,
                o.delivery_address,
                o.ingested_at
            FROM swiggy.main.live_orders o
            LEFT JOIN swiggy.main.dim_restaurants r ON o.restaurant_id = r.restaurant_id;
        """)

        print("Creating view for streaming DLQ (corrupted records)...")
        con.execute("""
            CREATE OR REPLACE VIEW swiggy.main.live_dlq AS 
            SELECT * FROM delta_scan('/app/delta_lake/bronze/dlq');
        """)
        
        print("Successfully created 'live_orders' and 'live_dlq' views in DuckDB!")
        print("You can now query these directly in Superset under swiggy.main schema.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'con' in locals():
            con.close()

if __name__ == "__main__":
    main()
