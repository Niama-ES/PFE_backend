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

def send_business_rules_to_kafka(business_rules):
    data = {"rules": business_rules}
    producer.send('business-rules', data)
    producer.flush()

def send_elasticsearch_results_to_kafka(documents):
    data = {"results": documents}
    producer.send('elasticsearch-results', data)
    producer.flush()