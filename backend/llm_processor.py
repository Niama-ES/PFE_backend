import requests

def process_with_llm(user_query, business_rules, search_results):
    """
    Process the user query with LLM by sending a request to the LLM API.

    Args:
    - user_query (str): The query input by the user.
    - business_rules (dict): Business rules that need to be applied to the results.
    - search_results (list): The search results obtained from Elasticsearch.

    Returns:
    - str: The processed result or analysis from the LLM.
    """
    # Construct the prompt to send to the LLM 
    prompt = f"Query: {user_query}\nResults: {search_results}\nBusiness Rules: {business_rules}"

    # LLM API endpoint
    url = "http://18.170.3.63:8001/prompt/normal"

    # Request payload
    data = {
        "token": "sYTcKeH9bEtW7NPIwSlL4jc39TVu4g4T",  
        "lang": "ENGLISH",  
        "prompt": prompt,
        "fields": ""  
    }

    # Send POST request to the LLM API
    try:

        print("Ylh Ansiftoh LLM API")  

        response = requests.post(url, json=data)

        print("Everything went LLM API") 

        # Check if the request was successful
        if response.status_code == 200:
            # parse the response as JSON
            return response.json()
        else:
            # If the API request failed, return an error message
            return f"Error: LLM API request failed with status code {response.status_code}"
    except Exception as e:
        return f"Exception occurred: {str(e)}"