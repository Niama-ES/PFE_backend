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
    print(f"Sent {len(user_query)} input to Kafka.")

def send_business_rules_to_kafka(business_rules):
    data = {"rules": business_rules}
    producer.send('business-rules', data)
    producer.flush()
    print(f"Sent {len(business_rules)} business rules to Kafka.")

def send_elasticsearch_results_to_kafka(documents):
    """
    Send Elasticsearch search results to Kafka.
    """
    # Extract the actual data from the Elasticsearch response
    if 'hits' in documents and 'hits' in documents['hits']:
        es_data = documents['hits']['hits']  # This is the actual search data

        # format the data
        formatted_data = [{'id': doc['_id'], 'source': doc['_source']} for doc in es_data]

        # Send to Kafka
        for data in formatted_data:
            producer.send('elasticsearch-results', data)
            producer.flush()

        print(f"Sent {len(formatted_data)} Elasticsearch results to Kafka.")
    else:
        print("No valid Elasticsearch results found.")






 