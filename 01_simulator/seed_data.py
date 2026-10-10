import uuid
import random
import psycopg2
from faker import Faker
from datetime import datetime

fake = Faker('en_IN')

# DB Connection
conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="swiggy",
    user="swiggy_user",
    password="swiggy_pass"
)
conn.autocommit = True
cur = conn.cursor()

def seed_data():
    print("Seeding customers...")
    for _ in range(50):
        cur.execute(
            "INSERT INTO customers (customer_id, name, email, phone, city) VALUES (%s, %s, %s, %s, %s)",
            (str(uuid.uuid4()), fake.name(), fake.unique.email(), fake.phone_number()[:15], fake.city())
        )
    
    print("Seeding restaurants...")
    cuisines = ["Indian", "Chinese", "Italian", "Mexican", "Thai"]
    restaurant_ids = []
    for _ in range(20):
        r_id = str(uuid.uuid4())
        restaurant_ids.append(r_id)
        cur.execute(
            "INSERT INTO restaurants (restaurant_id, name, cuisine_type, city, avg_rating) VALUES (%s, %s, %s, %s, %s)",
            (r_id, fake.company(), random.choice(cuisines), fake.city(), round(random.uniform(3.0, 5.0), 2))
        )
        
    print("Seeding menu items...")
    categories = ["Starter", "Main Course", "Dessert", "Beverage"]
    for r_id in restaurant_ids:
        for _ in range(5): # 5 items per restaurant
            cur.execute(
                "INSERT INTO menu_items (item_id, restaurant_id, name, category, price) VALUES (%s, %s, %s, %s, %s)",
                (str(uuid.uuid4()), r_id, fake.word().capitalize() + " " + fake.word().capitalize(), random.choice(categories), round(random.uniform(50, 500), 2))
            )
            
    print("Seed data successfully loaded!")

if __name__ == "__main__":
    seed_data()
