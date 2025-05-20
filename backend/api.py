import json
from flask import Flask, jsonify, request
from kafka_producer import send_user_query_to_kafka, send_business_rules_to_kafka, send_elasticsearch_results_to_kafka
from kafka_consumer import consume_and_process
from pymongo import MongoClient
import pymongo
from elasticsearch import Elasticsearch
from bson.json_util import dumps

#########################THE POWER OF ALLAH AND THE LUCK OF SRA9ZIT ##############################

app = Flask(__name__)

#MongoDB connection setup   
def get_mongo_client():
    # MongoDB is running in a Docker container
    client = pymongo.MongoClient('mongodb://localhost:27017')
    #testing mongodb connection 
    server_info = client.server_info()  # This will raise an exception if unable to connect 
    print("Connected to MongoDB:", server_info)
    return client
    

def get_business_rules():
    """
    Retrieves business rules from MongoDB.

    Returns:
    - dict: The business rules retrieved from MongoDB.
    """
    
    client = get_mongo_client() 
    db = client['businessrules']  # Connect to the 'businessrules' database
    # Print the available collections to ensure we are looking at the right collection
    print("Collections in businessrules:", db.list_collection_names())
    collection = db['rules']
    
    """#method 2 directly from file 
    with open('aukhtubut-schema.dynamicDocument.json', 'r') as f:
      data = json.load(f)"""

    #business_rules = list(collection.find()) # Retrieve all documents
    business_rules = list(collection.find({"data": {"$exists": True}}))  # Ensure 'data' field exists
    print(f"Number of business rules fetched: {len(business_rules)}")

    # Print the extracted business rules
    for idx, rule in enumerate(business_rules):
            print(f"Document {idx + 1}:")
            print(f"ID: {rule['_id']}")
            print(f"Model: {rule['model']}")
            print(f"Data: {rule['data']}")  # Printing the 'data' field which contains the actual business rule
            print("-" * 50)  # Separator for better readability
     
    # If rules are found, return them, otherwise return a default message or empty dictionary
    if business_rules:
        return business_rules  # Return the business rules as a dictionary
        #return json.loads(dumps(business_rules))
        #return [rule['data'] for rule in business_rules]  # Return the business rules
    else:
        #return "no business rules found "  # Empty dictionary if no business rules found
        return []


def get_search_results(user_query):
    #connecting to elastic search
    es = Elasticsearch(
        "http://localhost:9200/",
        basic_auth=("elastic", "elasticpfe25")    # The credentials for the Elasticsearch instance
    )  

    # Check if Elasticsearch is connected
    if es.ping():
       print("Successfully connected to Elasticsearch!")
    else:
       print("Could not connect to Elasticsearch.")
    
    #documents = es.search(index="university_index", query={"match": {"content": user_query}})
    #documents = es.search(index="university_index", body={"query": {"match": {"content": user_query}}})
    documents = es.search(index="university_index", body={
    "query": {
        "multi_match": {
            "query": user_query,
            "fields": ["*"]
        }
    }
})
    # Print the retrieved documents
    for doc in documents['hits']['hits']:
        print(f"Document ID: {doc['_id']}")
        print(f"Document Source: {doc['_source']}")
 
    return documents
    

#######################################################################SENDING#########################################################################

@app.route('/send_query', methods=['POST'])
def send_query():
    user_query = request.json['query']

    # Retrieve search results from Elasticsearch
    documents = get_search_results(user_query)
    print(f"Fetched {len(documents['hits']['hits'])} documents from Elasticsearch")
    # Retrieve business rules from MongoDB
    business_rules = get_business_rules()
    print(f"Fetched {len(business_rules)} business rules from MongoDB")

    # Send user query and business rules to Kafka
    send_user_query_to_kafka(user_query)
    send_business_rules_to_kafka(business_rules)
    send_elasticsearch_results_to_kafka(documents)
    

    if user_query:
        processed_result = consume_and_process()

         # Print the response data to the terminal/console
        print("Response Data:", processed_result)

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