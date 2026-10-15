import duckdb

def create_batch_views():
    db_path = "/home/jovyan/work/data/duckdb/swiggy.db"
    con = duckdb.connect(db_path)
    
    print("Creating v_batch_orders view in DuckDB...")
    con.execute("""
        CREATE OR REPLACE VIEW swiggy.main.v_batch_orders AS 
        SELECT 
            f.order_id,
            f.customer_id,
            f.restaurant_id,
            r.restaurant_name,
            r.cuisine_type,
            r.city,
            r.avg_rating,
            f.total_amount,
            f.status,
            f.event_timestamp
        FROM swiggy.main.fact_orders f
        LEFT JOIN swiggy.main.dim_restaurants r ON f.restaurant_id = r.restaurant_id
        WHERE f.source = 'batch';
    """)
    
    cnt = con.execute("SELECT count(*) FROM swiggy.main.v_batch_orders").fetchone()[0]
    print(f"v_batch_orders created successfully with {cnt:,} rows!")
    con.close()

if __name__ == "__main__":
    create_batch_views()
