import csv
import json
import psycopg2

PG_CONFIG = {
    "host": "127.0.0.1", "port": 5433,
    "dbname": "swiggy", "user": "swiggy_user", "password": "swiggy_pass"
}

def export_raw_data():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    
    # Export restaurants to CSV
    cur.execute("SELECT restaurant_id, name, cuisine_type, city, avg_rating, is_active FROM restaurants")
    with open("data/raw/restaurants.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["restaurant_id", "name", "cuisine_type", "city", "avg_rating", "is_active"])
        writer.writerows(cur.fetchall())
        
    # Export menu items to JSON
    cur.execute("SELECT item_id, restaurant_id, name, category, price, is_available FROM menu_items")
    items = []
    for r in cur.fetchall():
        items.append({
            "item_id": str(r[0]),
            "restaurant_id": str(r[1]),
            "name": r[2],
            "category": r[3],
            "price": float(r[4]),
            "is_available": r[5]
        })
    with open("data/raw/menu_items.json", "w") as f:
        json.dump(items, f, indent=2)
        
    conn.close()
    print("Raw CSV and JSON exported to data/raw/")

if __name__ == "__main__":
    export_raw_data()
