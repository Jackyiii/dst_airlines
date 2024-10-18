import os
import requests
import yaml
from dotenv import load_dotenv

from config.url import TOKEN_URL

load_dotenv()

def get_access_token():
    """
    This function fetches the OAuth access token from Lufthansa API.

    Returns:
    str: Access token for authenticating API requests.
    """
    client_id = os.getenv("CLIENT_ID")
    client_secret = os.getenv("CLIENT_SECRET")

    token_url = TOKEN_URL

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
    }

    try:
        response = requests.post(token_url, headers=headers, data=data)
        # status 200
        response.raise_for_status()
        token_data = response.json()
        return token_data.get("access_token")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching access token: {e}")
        return None