import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

import os
import psycopg2
from kafka import KafkaProducer

PG_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname": "swiggy", "user": "swiggy_user", "password": "swiggy_pass"
}

def get_db_context(max_retries=15, delay=2):
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**PG_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT customer_id, city FROM customers")
            customers = cur.fetchall()
            cur.execute("SELECT restaurant_id, city FROM restaurants")
            restaurants = cur.fetchall()
            conn.close()
            if customers and restaurants:
                return customers, restaurants
        except Exception as e:
            print(f"Waiting for PostgreSQL context (attempt {attempt+1}/{max_retries})... {e}")
            time.sleep(delay)
    return [], []

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='redpanda:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Redpanda broker!")
            return producer
        except Exception as e:
            print(f"Waiting for Redpanda... {e}")
            time.sleep(2)

def generate_order_event(customers, restaurants):
    event_types = ["order_placed", "order_updated", "order_delivered"]
    statuses = ["PENDING", "PREPARING", "OUT_FOR_DELIVERY", "DELIVERED"]
    
    # 10% chance of creating a corrupted record (missing order_id or invalid event type)
    is_corrupted = random.random() < 0.10
    order_id = None if is_corrupted else f"ORD-{uuid.uuid4().hex[:8].upper()}"
    event_type = "invalid_event" if is_corrupted else random.choice(event_types)
    
    if restaurants:
        r_id, city = random.choice(restaurants)
    else:
        r_id, city = str(uuid.uuid4()), "Bangalore"
        
    matching_customers = [c[0] for c in customers if c[1] == city] if customers else []
    c_id = str(random.choice(matching_customers)) if matching_customers else str(uuid.uuid4())
    
    total = round(max(140.0, random.gauss(410, 130)), 2) if not is_corrupted else 0.0
    
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "order_id": order_id,
        "customer_id": c_id,
        "restaurant_id": str(r_id),
        "items": [
            {"item_id": "ITEM-1", "quantity": random.randint(1, 2), "unit_price": 180.0},
            {"item_id": "ITEM-2", "quantity": random.randint(1, 3), "unit_price": 120.0}
        ],
        "total_amount": total,
        "status": random.choice(statuses),
        "delivery_address": f"{random.randint(1, 250)}, Swiggy Street, {city}",
        "is_corrupted": is_corrupted,
        "_error_reason": "null_order_id_or_non_positive" if is_corrupted else ""
    }

def main():
    print("Loading restaurant and customer IDs from database...")
    customers, restaurants = get_db_context()
    print(f"Loaded {len(customers)} customers and {len(restaurants)} restaurants.")

    producer = get_producer()
    topic = "orders.events"
    
    print(f"Starting real-time order stream to '{topic}' (1 event/sec, 10% DLQ)...")
    try:
        while True:
            event = generate_order_event(customers, restaurants)
            producer.send(topic, event)
            status_tag = "[CORRUPT/DLQ]" if event["is_corrupted"] else "[CLEAN]"
            print(f"{status_tag} {event['event_type']} - Order: {event['order_id']} | Total: ₹{event['total_amount']}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
