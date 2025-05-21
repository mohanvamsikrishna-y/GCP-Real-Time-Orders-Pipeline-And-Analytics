import json
import time
import random
import uuid
from datetime import datetime
from google.cloud import pubsub_v1

PROJECT_ID = "fleet-parity-458321-e4"
TOPIC_ID = "orders-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

categories = ['electronics', 'books', 'fashion', 'grocery']
payment_methods = ['credit_card', 'debit_card', 'paypal', 'cash']

def generate_order():
    price = round(random.uniform(10.0, 500.0), 2)
    quantity = random.randint(1, 5)
    return {
        "order_id": f"ORD{uuid.uuid4().hex[:6].upper()}",
        "user_id": f"USR{random.randint(100, 999)}",
        "product_id": f"PROD{random.randint(100, 999)}",
        "category": random.choice(categories),
        "price": price,
        "quantity": quantity,
        "total_amount": round(price * quantity, 2),
        "payment_method": random.choice(payment_methods),
        "order_timestamp": datetime.utcnow().isoformat() + "Z"
    }

if __name__ == "__main__":
    while True:
        order = generate_order()
        data = json.dumps(order).encode("utf-8")
        publisher.publish(topic_path, data=data)
        print(f"Published: {order}")
        time.sleep(3)  
