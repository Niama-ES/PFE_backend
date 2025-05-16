import json
from flask import Flask, jsonify, request
from kafka_producer import send_user_query_to_kafka
from kafka_consumer import consume_and_process

app = Flask(__name__)

@app.route('/send_query', methods=['POST'])
def send_query():
    user_query = request.json['query']

    # Send user query to Kafka
    send_user_query_to_kafka(user_query)
    
    if user_query:
        processed_result = consume_and_process()

        return jsonify({
            "status": "success", 
            "message": "Query sent to Kafka", 
            "query": user_query,
            "processed_result": processed_result  # Include the processed result from LLM
        }), 200
    else:
        return jsonify({
            "status": "error", 
            "message": "No query received"
        }), 400
    

if __name__ == '__main__':
    app.run(debug=True, port=5000)