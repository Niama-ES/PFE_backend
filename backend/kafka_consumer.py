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
        #for message_query, message_rules, message_results in zip(consumer_queries, consumer_rules, consumer_results):   #thiis was the first method and it didn't work

        # Get the messages from each topic (it worked when we had BR + input but when we added SR it didn't work)
        message_query = next(consumer_queries)  
        message_rules = next(consumer_rules)   
        message_results = next(consumer_results)   

        # Print the raw message to ensure they are not empty
        print(f"Message Query: {message_query.value}")
        print(f"Message Rules: {message_rules.value}")
        print(f"Message Results: {message_results.value}")

        # Extract user query and business rules and search results from the messages
        user_query = message_query.value['query']
        business_rules = message_rules.value['rules']
        search_results = message_results.value['results']
        
        # Check if all fields are valid (we did check the topics from cmd they were nit empty but they are not consumed here)
        if not user_query or not business_rules or not search_results:
            print("Error: One or more messages are empty!")
            return None
        print("Data received successfully.")
        
        # Send this data to LLM for processing
        processed_result = process_with_llm(user_query, business_rules, search_results)
        return processed_result  # Return the processed result