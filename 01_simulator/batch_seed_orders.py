import io
import os
import random
import time
import uuid
from datetime import datetime, timedelta
import psycopg2

PG_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname": "swiggy",
    "user": "swiggy_user",
    "password": "swiggy_pass"
}

def get_connection():
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        return conn
    except Exception:
        # Fallback to local host port 5433 if running outside container
        fallback_config = dict(PG_CONFIG)
        fallback_config["host"] = "127.0.0.1"
        fallback_config["port"] = 5433
        return psycopg2.connect(**fallback_config)

def seed_historical_orders(target_count=1_000_000, batch_size=50_000, truncate_existing=True):
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()
    
    # Query customers by city
    cur.execute("SELECT customer_id, city FROM customers")
    cust_rows = cur.fetchall()
    city_customers = {}
    for c_id, city in cust_rows:
        city_customers.setdefault(city, []).append(str(c_id))

    # Query restaurants by city
    cur.execute("SELECT restaurant_id, city, name FROM restaurants")
    rest_rows = cur.fetchall()
    city_restaurants = {}
    for r_id, city, name in rest_rows:
        city_restaurants.setdefault(city, []).append(str(r_id))

    if not city_restaurants or not city_customers:
        print("ERROR: Customers or restaurants table is empty. Please run seed_data.py first!")
        conn.close()
        return

    # Real-world Swiggy metro order weights
    CITY_WEIGHTS = {
        "Bangalore": 0.28,
        "Mumbai": 0.22,
        "Delhi NCR": 0.18,
        "Hyderabad": 0.14,
        "Pune": 0.08,
        "Chennai": 0.05,
        "Kolkata": 0.03,
        "Ahmedabad": 0.02
    }

    # Match weights only to available cities
    available_cities = [c for c in CITY_WEIGHTS if c in city_restaurants and c in city_customers]
    weights = [CITY_WEIGHTS[c] for c in available_cities]
    total_w = sum(weights)
    normalized_weights = [w / total_w for w in weights]

    if truncate_existing:
        print("Truncating existing orders table before realistic 1M batch seeding...")
        cur.execute("TRUNCATE TABLE orders CASCADE;")

    print(f"Generating and bulk inserting {target_count:,} realistic historical orders...")
    print(f"City distribution: {', '.join([f'{c}: {w*100:.0f}%' for c, w in zip(available_cities, normalized_weights)])}")
    
    start_time = time.time()
    total_inserted = 0

    # Common realistic delivery localities per metro
    CITY_LOCALITIES = {
        "Bangalore": ["Koramangala, 5th Block", "Indiranagar, 100ft Rd", "HSR Layout, Sector 3", "Whitefield Main Rd", "Jayanagar, 4th Block"],
        "Mumbai": ["Bandra West, Hill Rd", "Andheri East, MIDC", "Powai, Hiranandani", "Lower Parel, Phoenix", "Juhu, Tara Rd"],
        "Delhi NCR": ["Connaught Place, Block B", "Cyber Hub, Gurugram", "Sector 18, Noida", "Hauz Khas Village", "Saket, Select City"],
        "Hyderabad": ["Hitec City, Madhapur", "Gachibowli, Financial Dist", "Jubilee Hills, Rd 36", "Banjara Hills, Rd 12", "Kondapur"],
        "Pune": ["Koregaon Park, Lane 7", "Viman Nagar, Phoenix", "Kothrud, Paud Rd", "Baner, High St", "Hinjawadi, Phase 1"],
        "Chennai": ["T. Nagar, Usman Rd", "Adyar, Gandhi Nagar", "Velachery Main Rd", "Anna Nagar, 2nd Ave", "OMR, Thoraipakkam"],
        "Kolkata": ["Park Street", "Salt Lake, Sector V", "Ballygunge Circular Rd", "New Town, Action Area 1", "Alipore"],
        "Ahmedabad": ["SG Highway, Bodakdev", "Prahlad Nagar", "Vastrapur Lake Rd", "Navrangpura", "Satellite, Shivranjani"]
    }

    while total_inserted < target_count:
        chunk_count = min(batch_size, target_count - total_inserted)
        buffer = io.StringIO()
        
        # Fast bulk generation
        chosen_cities = random.choices(available_cities, weights=normalized_weights, k=chunk_count)
        
        for city in chosen_cities:
            order_id = str(uuid.uuid4())
            c_id = random.choice(city_customers[city])
            r_id = random.choice(city_restaurants[city])
            
            # Realistic Swiggy AOV distribution: Median ~390, 80% between 220 and 650
            total = round(max(120.0, random.gauss(420, 140)), 2)
            
            # Realistic order timestamps: last 30 days, peaked around Lunch (12-15) and Dinner (19-23)
            days_ago = random.randint(1, 30)
            if random.random() < 0.65:  # 65% dinner peak
                hour = random.randint(19, 23)
            elif random.random() < 0.85: # 20% lunch peak
                hour = random.randint(12, 15)
            else:                        # 15% breakfast/snacks
                hour = random.choice([8, 9, 10, 16, 17, 18])
                
            minute = random.randint(0, 59)
            placed_at = datetime.now() - timedelta(days=days_ago)
            placed_at = placed_at.replace(hour=hour, minute=minute, second=random.randint(0, 59))
            delivered_at = placed_at + timedelta(minutes=random.randint(22, 55))
            
            localities = CITY_LOCALITIES.get(city, ["Main Street"])
            address = f"{random.randint(1, 250)}, {random.choice(localities)}, {city}"
            
            # CSV: order_id, customer_id, restaurant_id, status, total_amount, delivery_address, placed_at, delivered_at
            buffer.write(f'{order_id},{c_id},{r_id},delivered,{total},"{address}",{placed_at.strftime("%Y-%m-%d %H:%M:%S")},{delivered_at.strftime("%Y-%m-%d %H:%M:%S")}\n')
            
        buffer.seek(0)
        cur.copy_expert(
            """COPY orders (order_id, customer_id, restaurant_id, status, total_amount, delivery_address, placed_at, delivered_at)
               FROM STDIN WITH (FORMAT CSV)""",
            buffer
        )
        total_inserted += chunk_count
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        pct = (total_inserted / target_count) * 100
        print(f"Progress: {total_inserted:,} / {target_count:,} ({pct:5.1f}%) | {rate:,.0f} rows/sec | Elapsed: {elapsed:.1f}s")

    elapsed_total = time.time() - start_time
    print(f"\nSuccessfully seeded {total_inserted:,} realistic orders in {elapsed_total:.2f} seconds ({total_inserted/elapsed_total:,.0f} rows/sec)!")
    conn.close()

if __name__ == "__main__":
    target = int(os.environ.get("BATCH_ORDER_COUNT", 1_000_000))
    seed_historical_orders(target_count=target)


