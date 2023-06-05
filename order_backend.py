# Import required libaries
import json
import time
from kafka import KafkaProducer

# Define constants
ORDER_KAFKA_TOPIC = "order_details"  # Kafka topic name for order details
ORDER_LIMIT = 1000000  # Number of orders to generate

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers="localhost:9092")                # Recived error here because the port number were not same
                                                                            # producer = KafkaProducer(bootstrap_servers="localhost:29092") -> producer = KafkaProducer(bootstrap_servers="localhost:9092")    

# Print initial messages
print("GOING TO GENERATE AFTER EVERY 10 SECONDS")
print("WILL GENERATE ONE UNIQUE ORDER EVERY 10 SECONDS")

# Generate and send orders
for i in range(1, ORDER_LIMIT):
    # Create order data dictionary
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost": i * 2,
        "items": "burger, sandwich"
    }

    # Send order data to Kafka topic
    producer.send(ORDER_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))

    # Print status
    print(f"Done sending order {i}")

    # Wait for 10 seconds before the next iteration
    time.sleep(1)
