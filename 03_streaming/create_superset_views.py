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
            SELECT * FROM delta_scan('/app/delta_lake/bronze/orders');
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
