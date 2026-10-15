import uuid
import random
import os
import psycopg2
from faker import Faker

fake = Faker('en_IN')

PG_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname": "swiggy",
    "user": "swiggy_user",
    "password": "swiggy_pass"
}

def get_connection():
    try:
        return psycopg2.connect(**PG_CONFIG)
    except Exception:
        fallback = dict(PG_CONFIG)
        fallback["host"] = "127.0.0.1"
        fallback["port"] = 5433
        return psycopg2.connect(**fallback)

# Real-world iconic Swiggy restaurants per major metro
REAL_RESTAURANTS = [
    # Bangalore (Tech & Food Capital - High Volume)
    {"name": "Meghana Foods", "cuisine": "Biryani", "city": "Bangalore", "rating": 4.6},
    {"name": "Truffles", "cuisine": "Fast Food", "city": "Bangalore", "rating": 4.5},
    {"name": "Empire Restaurant", "cuisine": "North Indian", "city": "Bangalore", "rating": 4.3},
    {"name": "Nagarjuna", "cuisine": "South Indian", "city": "Bangalore", "rating": 4.4},
    {"name": "Corner House Ice Cream", "cuisine": "Desserts", "city": "Bangalore", "rating": 4.7},

    # Mumbai (High Volume & Premium AOV)
    {"name": "Social", "cuisine": "Fast Food", "city": "Mumbai", "rating": 4.3},
    {"name": "Bademiya", "cuisine": "North Indian", "city": "Mumbai", "rating": 4.2},
    {"name": "Theobroma", "cuisine": "Desserts", "city": "Mumbai", "rating": 4.6},
    {"name": "Amar Juice Centre", "cuisine": "Fast Food", "city": "Mumbai", "rating": 4.1},

    # Delhi NCR (Massive North Indian & Fast Food Volume)
    {"name": "Haldiram's", "cuisine": "North Indian", "city": "Delhi NCR", "rating": 4.4},
    {"name": "Karim's", "cuisine": "North Indian", "city": "Delhi NCR", "rating": 4.3},
    {"name": "Wow! Momo", "cuisine": "Chinese", "city": "Delhi NCR", "rating": 4.2},
    {"name": "Moti Mahal Delux", "cuisine": "North Indian", "city": "Delhi NCR", "rating": 4.1},

    # Hyderabad (The Biryani Capital)
    {"name": "Paradise Biryani", "cuisine": "Biryani", "city": "Hyderabad", "rating": 4.5},
    {"name": "Bawarchi", "cuisine": "Biryani", "city": "Hyderabad", "rating": 4.4},
    {"name": "Chutneys", "cuisine": "South Indian", "city": "Hyderabad", "rating": 4.3},
    {"name": "Shah Ghouse", "cuisine": "Biryani", "city": "Hyderabad", "rating": 4.4},

    # Pune
    {"name": "Vaishali", "cuisine": "South Indian", "city": "Pune", "rating": 4.5},
    {"name": "Goodluck Cafe", "cuisine": "Fast Food", "city": "Pune", "rating": 4.2},
    {"name": "Camp Burger", "cuisine": "Fast Food", "city": "Pune", "rating": 4.4},

    # Chennai
    {"name": "Saravana Bhavan", "cuisine": "South Indian", "city": "Chennai", "rating": 4.3},
    {"name": "Murugan Idli Shop", "cuisine": "South Indian", "city": "Chennai", "rating": 4.4},
    {"name": "Dindigul Thalappakatti", "cuisine": "Biryani", "city": "Chennai", "rating": 4.2},

    # Kolkata
    {"name": "Arsalan", "cuisine": "Biryani", "city": "Kolkata", "rating": 4.5},
    {"name": "Peter Cat", "cuisine": "North Indian", "city": "Kolkata", "rating": 4.4},

    # Ahmedabad
    {"name": "Honest Restaurant", "cuisine": "Fast Food", "city": "Ahmedabad", "rating": 4.2},
    {"name": "Gordhan Thal", "cuisine": "North Indian", "city": "Ahmedabad", "rating": 4.3}
]

CUISINE_MENU_TEMPLATES = {
    "Biryani": [
        ("Special Chicken Biryani", 320), ("Mutton Dum Biryani", 420),
        ("Paneer Biryani", 260), ("Boneless Chicken Biryani", 360),
        ("Egg Biryani", 230), ("Gulab Jamun (2 pcs)", 80)
    ],
    "North Indian": [
        ("Butter Chicken", 340), ("Dal Makhani", 240),
        ("Paneer Butter Masala", 280), ("Butter Naan", 45),
        ("Tandoori Roti", 25), ("Jeera Rice", 160)
    ],
    "South Indian": [
        ("Masala Dosa", 110), ("Ghee Roast Dosa", 130),
        ("Idli Vada Combo", 95), ("Filter Coffee", 40),
        ("Bisi Bele Bath", 120), ("Curd Rice", 90)
    ],
    "Fast Food": [
        ("All American Veg Burger", 180), ("Crispy Chicken Burger", 240),
        ("Peri Peri French Fries", 120), ("Cheese Loaded Nachos", 190),
        ("Cold Coffee with Ice Cream", 140), ("Chocolate Thickshake", 160)
    ],
    "Chinese": [
        ("Chicken Steamed Momos (6 pcs)", 160), ("Chilli Paneer Dry", 230),
        ("Chicken Fried Rice", 210), ("Veg Hakka Noodles", 180),
        ("Chicken Manchurian", 250), ("Spring Rolls", 150)
    ],
    "Desserts": [
        ("Death by Chocolate", 220), ("Dutch Chocolate Pastry", 150),
        ("Hot Chocolate Fudge", 190), ("Red Velvet Cake Slice", 160),
        ("Brownie with Vanilla Ice Cream", 180), ("Caramel Custard", 120)
    ]
}

def seed_data():
    conn = get_connection()
    conn.autocommit = True
    cur = conn.cursor()

    print("Cleaning existing tables for fresh realistic seeding...")
    cur.execute("TRUNCATE TABLE order_items, orders, menu_items, restaurants, customers CASCADE;")

    print("Seeding realistic customers across metro cities...")
    cities_weighted = ["Bangalore"]*28 + ["Mumbai"]*22 + ["Delhi NCR"]*18 + ["Hyderabad"]*14 + ["Pune"]*8 + ["Chennai"]*5 + ["Kolkata"]*3 + ["Ahmedabad"]*2
    customer_ids = []
    for _ in range(100):
        c_id = str(uuid.uuid4())
        city = random.choice(cities_weighted)
        customer_ids.append(c_id)
        cur.execute(
            "INSERT INTO customers (customer_id, name, email, phone, city) VALUES (%s, %s, %s, %s, %s)",
            (c_id, fake.name(), fake.unique.email(), fake.phone_number()[:15], city)
        )

    print(f"Seeding {len(REAL_RESTAURANTS)} iconic restaurants across 8 major metros...")
    for r in REAL_RESTAURANTS:
        r_id = str(uuid.uuid4())
        r["restaurant_id"] = r_id
        cur.execute(
            "INSERT INTO restaurants (restaurant_id, name, cuisine_type, city, avg_rating) VALUES (%s, %s, %s, %s, %s)",
            (r_id, r["name"], r["cuisine"], r["city"], r["rating"])
        )

        # Seed authentic menu items for this restaurant
        items = CUISINE_MENU_TEMPLATES.get(r["cuisine"], CUISINE_MENU_TEMPLATES["North Indian"])
        for item_name, price in items:
            cur.execute(
                "INSERT INTO menu_items (item_id, restaurant_id, name, category, price) VALUES (%s, %s, %s, %s, %s)",
                (str(uuid.uuid4()), r_id, item_name, "Main Course" if price > 150 else "Starter", price)
            )

    print("Realistic seed data successfully loaded into PostgreSQL!")
    conn.close()

if __name__ == "__main__":
    seed_data()

