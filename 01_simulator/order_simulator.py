import uuid
import json
import random
import time
import argparse
import psycopg2
from kafka import KafkaProducer
from datetime import datetime, timezone

# --- Config ---
REDPANDA_BOOTSTRAP = "redpanda:9092"
TOPIC_ORDERS = "orders.events"

# Use port 5433 as configured
PG_CONFIG = {
    "host": "postgres", "port": 5432,
    "dbname": "swiggy", "user": "swiggy_user", "password": "swiggy_pass"
}

STATUSES = ["placed", "preparing", "out_for_delivery", "delivered"]

def get_db_ids():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT customer_id FROM customers")
    customers = [str(r[0]) for r in cur.fetchall()]
    cur.execute("SELECT restaurant_id FROM restaurants")
    restaurants = [str(r[0]) for r in cur.fetchall()]
    cur.execute("SELECT item_id, restaurant_id, price FROM menu_items")
    items = [{"item_id": str(r[0]), "restaurant_id": str(r[1]), "price": float(r[2])} for r in cur.fetchall()]
    conn.close()
    return customers, restaurants, items

def make_good_event(customers, restaurants, all_items):
    restaurant_id = random.choice(restaurants)
    menu = [item for item in all_items if item["restaurant_id"] == restaurant_id]
    
    num_items = random.randint(1, 5)
    items = []
    total = 0.0
    for _ in range(num_items):
        item = random.choice(menu)
        qty = random.randint(1, 3)
        items.append({
            "item_id": item["item_id"],
            "quantity": qty,
            "unit_price": item["price"]
        })
        total += item["price"] * qty
        
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order_placed",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(customers),
        "restaurant_id": restaurant_id,
        "items": items,
        "total_amount": round(total, 2),
        "status": "placed",
        "delivery_address": "123 Main St",
        "is_corrupted": False
    }

def make_bad_event(customers, restaurants, items):
    bad_type = random.choice(["null_order_id", "negative_price", "invalid_type", "missing_field"])
    event = make_good_event(customers, restaurants, items)
    event["is_corrupted"] = True

    if bad_type == "null_order_id":
        event["order_id"] = None
        event["_error_reason"] = "null_order_id"
    elif bad_type == "negative_price":
        event["total_amount"] = -abs(event["total_amount"])
        event["_error_reason"] = "negative_total_amount"
    elif bad_type == "invalid_type":
        event["event_type"] = "INVALID_EVENT_XYZ"
        event["_error_reason"] = "invalid_event_type"
    elif bad_type == "missing_field":
        del event["customer_id"]
        event["_error_reason"] = "missing_customer_id"

    return event

def produce_events(rate=2, bad_rate=0.1, dry_run=False, count=None):
    customers, restaurants, items = get_db_ids()
    
    producer = None
    if not dry_run:
        producer = KafkaProducer(
            bootstrap_servers=REDPANDA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        
    print(f"[Simulator] Starting: {rate} events/sec, {bad_rate*100:.0f}% bad records")
    events_sent = 0
    
    try:
        while True:
            if count is not None and events_sent >= count:
                break
                
            event = make_bad_event(customers, restaurants, items) if random.random() < bad_rate else make_good_event(customers, restaurants, items)
            
            if not dry_run:
                producer.send(
                    TOPIC_ORDERS,
                    key=event.get("order_id", "null").encode() if event.get("order_id") else b"null",
                    value=event
                )
                
            status = "BAD" if event.get("is_corrupted") else "OK"
            print(f"[{events_sent+1:05d}] {status} | event_id={event['event_id'][:8]} | type={event.get('event_type')}")
            
            events_sent += 1
            if rate > 0:
                time.sleep(1 / rate)
                
    except KeyboardInterrupt:
        pass
    finally:
        if producer:
            producer.flush()
            producer.close()
        print(f"\n[Simulator] Stopped after {events_sent} events.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=2)
    parser.add_argument("--bad-rate", type=float, default=0.1)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--count", type=int, default=None)
    args = parser.parse_args()
    
    produce_events(args.rate, args.bad_rate, args.dry_run, args.count)
