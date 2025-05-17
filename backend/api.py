import json
from flask import Flask, jsonify, request
from kafka_producer import send_user_query_to_kafka, send_business_rules_to_kafka
from kafka_consumer import consume_and_process
from pymongo import MongoClient

app = Flask(__name__)

# MongoDB connection setup
def get_mongo_client():
    # MongoDB is running in a Docker container
    client = MongoClient('mongodb://localhost:27017')  # MongoDB URL
    return client

def get_business_rules():
    """
    Retrieves business rules from MongoDB.

    Returns:
    - dict: The business rules retrieved from MongoDB.
    """
    client = get_mongo_client()
    db = client['businessrules']  # Replace with your database name
    collection = db['rules']  # Replace with your collection name

    # Assuming the business rules are stored in the collection as documents
    business_rules = list(collection.find()) # Retrieve all documents

    # If rules are found, return them, otherwise return a default message or empty dictionary
    if business_rules:
        return business_rules  # Return the business rules as a dictionary
    else:
        return {}  # Empty dictionary if no business rules found


@app.route('/send_query', methods=['POST'])
def send_query():
    user_query = request.json['query']

    # Retrieve business rules from MongoDB
    business_rules = get_business_rules()

    # Send user query and business rules to Kafka
    send_user_query_to_kafka(user_query)
    send_business_rules_to_kafka(business_rules)
    

    if user_query:
        processed_result = consume_and_process()

        return jsonify({
            "status": "success", 
            "message": "Query and Business rules sent to Kafka", 
            "query": user_query,
            "business_rules": business_rules,
            "processed_result": processed_result  # Include the processed result from LLM
        }), 200
    else:
        return jsonify({
            "status": "error", 
            "message": "No query received"
        }), 400
    

if __name__ == '__main__':
    app.run(debug=True, port=5000)