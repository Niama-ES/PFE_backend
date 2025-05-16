from kafka import KafkaConsumer
import json
from llm_processor import process_with_llm

# Kafka Consumer setup for multiple topics
consumer_queries = KafkaConsumer(
    'user-queries',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def consume_and_process():
        #for message_query in consumer_queries :          kant iteration 3la 3 variables
        message_query = next(consumer_queries)
        user_query = message_query.value['query']
        
        # Send this data to LLM for processing
        processed_result = process_with_llm(user_query)
        return processed_result  # Return the processed result
        