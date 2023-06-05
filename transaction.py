import json
from kafka import KafkaConsumer, KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# Create Kafka consumer and producer instances
consumer = KafkaConsumer(ORDER_KAFKA_TOPIC, bootstrap_servers="localhost:9092")
producer = KafkaProducer(bootstrap_servers="localhost:9092")

print("Start listening")
while True:
    for message in consumer:
        print("Transaction ongoing")
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)

        user_id = consumed_message['user_id']
        total_cost = consumed_message['total_cost']

        # Prepare data for confirmation
        data = {
            "customer_id": user_id,
            "customer_email": f"{user_id}@gmail.com",
            "total_cost": total_cost
        }

        print("Successful transaction!")
        
        # Send confirmation data to Kafka topic
        producer.send(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
