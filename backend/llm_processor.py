import requests

def process_with_llm(user_query):
    """
    Process the user query with LLM by sending a request to the LLM API.

    Args:
    - user_query (str): The query input by the user.

    Returns:
    - str: The processed result or analysis from the LLM.
    """
    # Construct the prompt to send to the LLM
    prompt = f"Query: {user_query}"

    # LLM API endpoint
    url = "http://18.170.3.63:8001/prompt/normal"

    # Request payload
    data = {
        "token": "sYTcKeH9bEtW7NPIwSlL4jc39TVu4g4T",  # Replace with your actual token
        "lang": "ENGLISH",  # Or dynamically set the language if needed
        "prompt": prompt,
        "fields": ""  # If needed, you can provide additional fields here
    }

    # Send POST request to the LLM API
    try:
        response = requests.post(url, json=data)

        # Check if the request was successful
        if response.status_code == 200:
            # Attempt to parse the response as JSON
            try:
                json_response = response.json()
                # Check if the 'result' key exists
                llm_result = json_response.get('result', 'No result field in response')
                return llm_result
            except ValueError:
                return response.text
        else:
            # If the API request failed, return an error message
            return f"Error: LLM API request failed with status code {response.status_code}"
    except Exception as e:
        return f"Exception occurred: {str(e)}"