from kafka import KafkaProducer
import json

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_user_query_to_kafka(user_query):
    data = {"query": user_query}
    producer.send('user-queries', data)
    producer.flush()