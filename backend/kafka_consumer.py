from kafka import KafkaConsumer
import json
from llm_processor import process_with_llm

# Kafka Consumer setup for multiple topics
consumer_queries = KafkaConsumer(
    'user-queries',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

consumer_rules = KafkaConsumer(
    'business-rules',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

consumer_results = KafkaConsumer(
    'elasticsearch-results',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def consume_and_process():
        #message_query = next(consumer_queries)           when we had only user queries 
        #for message_query, message_rules in zip(consumer_queries, consumer_rules):   #both BR and user input

        # Get one message from the 'user-queries' topic and one from the 'business-rules' topic
        message_query = next(consumer_queries)  # Consume the next message from the user queries topic
        message_rules = next(consumer_rules)   # Consume the next message from the business rules topic
        message_results = next(consumer_results)   # Consume the next message from the elasticsearch results topic

        # Extract user query and business rules and search results from the messages
        user_query = message_query.value['query']
        business_rules = message_rules.value['rules']
        search_results = message_results.value['results']
        
        # Send this data to LLM for processing
        processed_result = process_with_llm(user_query, business_rules, search_results)
        return processed_result  # Return the processed result
