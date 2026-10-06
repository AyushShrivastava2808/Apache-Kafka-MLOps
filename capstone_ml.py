from confluent_kafka import Consumer
from cloud_config import kafka_config
import json

kafka_config['group.id'] = 'ml_fraud_engine'
kafka_config['auto.offset.reset'] = 'latest' # Sirf abhi ka live data chahiye, purana nahi

c = Consumer(kafka_config)
c.subscribe(['ml_topic'])

print("🤖 ML Fraud Detection Engine Online. Scanning live stream...")

try:
    while True:
        msg = c.poll(1.0)
        
        if msg is None or msg.error():
            continue
            
        # NEW LOGIC: Try-Except block to handle bad data safely
        try:
            raw_data = msg.value().decode('utf-8')
            txn = json.loads(raw_data)
        except json.JSONDecodeError:
            print(f"⚠️ Warning: Non-JSON format received and skipped -> {raw_data}")
            continue # Kharab data skip karke agle message par chalo
        
        # 2. Apply our ML Business Rule
        # Ek aur chhota check lagana safe rahega taaki KeyError na aaye
        if 'amount' in txn and 'user' in txn:
            if txn['amount'] > 4000:
                print(f"🚨 FRAUD BLOCKED FOR {txn['user']}! (Amount: ₹{txn['amount']})")
            else:
                print(f"✅ Transaction Approved: {txn['user']} (Amount: ₹{txn['amount']})")
        else:
            print("⚠️ Warning: Message mein 'amount' ya 'user' field missing hai.")
            
except KeyboardInterrupt:
    print("🛑 ML Engine Offline.")
finally:
    c.close()