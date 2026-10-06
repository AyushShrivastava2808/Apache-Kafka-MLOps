from confluent_kafka import Producer
from cloud_config import kafka_config
import time

p = Producer(kafka_config)

# We are simulating two different users clicking on our app
users = ["User_A", "User_B"]

for i in range(100):
    # Alternate between User_A and User_B
    current_user = users[i % 2]
    data = f"Action {i} from {current_user}"
    
    # Adding the 'key' ensures this user's data always goes to the same partition (lane)
    p.produce(
        'ml_topic', 
        key=current_user.encode('utf-8'), # The Key dictates the lane
        value=data.encode('utf-8')        # The Value is the actual data
    )
    
    print(f"🚀 Routed data specifically for {current_user}")
    time.sleep(1)

p.flush()
