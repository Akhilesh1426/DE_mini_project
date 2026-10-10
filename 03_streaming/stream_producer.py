import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

def get_producer():
    # Keep trying to connect to Redpanda
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers='redpanda:9092',
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Connected to Redpanda!")
            return producer
        except Exception as e:
            print(f"Waiting for Redpanda... {e}")
            time.sleep(2)

def generate_order_event():
    event_types = ["order_placed", "order_updated", "order_delivered", "invalid_event"]
    statuses = ["PENDING", "PREPARING", "OUT_FOR_DELIVERY", "DELIVERED"]
    
    # 20% chance of creating a corrupted record (missing order_id)
    is_corrupted = random.random() < 0.2
    order_id = None if is_corrupted else f"ORD-{uuid.uuid4().hex[:6].upper()}"
    
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(event_types),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "order_id": order_id,
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "restaurant_id": f"REST-{random.randint(10, 99)}",
        "items": [
            {"item_id": "ITEM-1", "quantity": 1, "unit_price": 10.50},
            {"item_id": "ITEM-2", "quantity": 2, "unit_price": 5.00}
        ],
        "total_amount": round(random.uniform(5.0, 50.0), 2) if not is_corrupted else 0.0,
        "status": random.choice(statuses),
        "delivery_address": "123 Swiggy Street, Food City",
        "is_corrupted": is_corrupted,
        "_error_reason": ""
    }

def main():
    producer = get_producer()
    topic = "orders.events"
    
    print(f"Starting to produce live events to '{topic}' topic...")
    try:
        while True:
            event = generate_order_event()
            producer.send(topic, event)
            print(f"Produced: {event['event_type']} - {event['order_id']}")
            
            # Send an event every 2 seconds
            time.sleep(2)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
