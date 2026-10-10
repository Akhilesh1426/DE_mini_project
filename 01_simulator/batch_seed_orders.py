import psycopg2
import random
import uuid
from datetime import datetime, timedelta

PG_CONFIG = {
    "host": "127.0.0.1", "port": 5433,
    "dbname": "swiggy", "user": "swiggy_user", "password": "swiggy_pass"
}

def seed_historical_orders():
    conn = psycopg2.connect(**PG_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    
    cur.execute("SELECT customer_id FROM customers")
    customers = [str(r[0]) for r in cur.fetchall()]
    cur.execute("SELECT restaurant_id FROM restaurants")
    restaurants = [str(r[0]) for r in cur.fetchall()]
    
    print("Seeding 10000 historical orders...")
    for _ in range(10000):
        order_id = str(uuid.uuid4())
        c_id = random.choice(customers)
        r_id = random.choice(restaurants)
        total = round(random.uniform(100, 1500), 2)
        days_ago = random.randint(1, 30)
        placed_at = datetime.now() - timedelta(days=days_ago, minutes=random.randint(10, 60))
        delivered_at = placed_at + timedelta(minutes=random.randint(20, 90))
        
        cur.execute(
            """INSERT INTO orders (order_id, customer_id, restaurant_id, status, total_amount, delivery_address, placed_at, delivered_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (order_id, c_id, r_id, 'delivered', total, "123 History St", placed_at, delivered_at)
        )
    print("Historical orders seeded successfully!")
    conn.close()

if __name__ == "__main__":
    seed_historical_orders()
