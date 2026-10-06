from confluent_kafka import Producer
from cloud_config import kafka_config
import time
import random
import json

p = Producer(kafka_config)
print("📱 Mobile App Online. Sending live transactions to Aiven Cloud...")

try:
    while True:
        # Simulating a live bank or Swiggy transaction
        txn = {
            'user': f'U_{random.randint(100, 199)}', 
            'amount': random.randint(50, 5000)
        }
        
        # Convert Python dictionary to JSON string before sending
        json_data = json.dumps(txn)
        
        # Send to our new topic
        p.produce('ml_topic', value=json_data.encode('utf-8'))
        print(f"💸 Transaction Sent: {txn}")
        
        time.sleep(0.5) # App generates a new transaction every 0.5 seconds
        
except KeyboardInterrupt:
    print("🛑 App Stopped.")
finally:
    p.flush()
