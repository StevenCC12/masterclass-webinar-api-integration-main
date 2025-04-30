import os
import requests
import json
from authlib.integrations.requests_client import OAuth2Session
import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get environment variables
ZOOM_ACCOUNT_ID = os.getenv("ZOOM_ACCOUNT_ID")
ZOOM_CLIENT_ID_AUTO_REG = os.getenv("ZOOM_CLIENT_ID_AUTO_REG")
ZOOM_CLIENT_SECRET_AUTO_REG = os.getenv("ZOOM_CLIENT_SECRET_AUTO_REG")
WEBINAR_ID = os.getenv("WEBINAR_ID")
print(f"ZOOM_ACCOUNT_ID: {ZOOM_ACCOUNT_ID}")
print(f"ZOOM_CLIENT_ID: {ZOOM_CLIENT_ID_AUTO_REG}")
print(f"ZOOM_CLIENT_SECRET_AUTO_REG: {ZOOM_CLIENT_SECRET_AUTO_REG}")
print(f"WEBINAR_ID: {WEBINAR_ID}")

# Check if all environment variables are set
if not all([ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID_AUTO_REG, ZOOM_CLIENT_SECRET_AUTO_REG, WEBINAR_ID]):
    print("Error: One or more required environment variables are missing.")
    print("Please ensure ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET, and WEBINAR_ID are set.")
    exit(1)

# Zoom OAuth2 endpoints
TOKEN_URL = f"https://zoom.us/oauth/token"

# Create OAuth2 session
session = OAuth2Session(
    client_id=ZOOM_CLIENT_ID_AUTO_REG,
    client_secret=ZOOM_CLIENT_SECRET_AUTO_REG,
    token_endpoint=TOKEN_URL,
    grant_type="account_credentials",
    token_endpoint_auth_method="client_secret_basic"
)

# Get access token with account_credentials grant type
token = session.fetch_token(
    url=TOKEN_URL,
    grant_type="account_credentials",
    account_id=ZOOM_ACCOUNT_ID
)

# Access token from response
access_token = token.get("access_token")
print(f"Successfully obtained access token")

# Webinar registrant endpoint
url = f"https://api.zoom.us/v2/webinars/{WEBINAR_ID}/registrants"

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}"
}

# Set the request type - change to "GET" to list registrants or "POST" to add a registrant
REQUEST_TYPE = "GET"  # Options: "GET" or "POST"

try:
    if REQUEST_TYPE == "POST":
        # Create a timestamp suffix to make the email unique for testing
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        # Example payload for registering a participant
        payload = {
            "first_name": "Ben",
            "last_name": "Lomon",
            "email": "ben@cleanconversion.com",
            "address": "Kungsgatan 37I",
            "city": "Katrineholm",
            "state": "CA",
            "zip": "94045",
            "country": "SE",
            "phone": "+46760221555",
            "comments": "Looking forward to the discussion.",
            "custom_questions": [
                {
                    "title": "What do you hope to learn from this?",
                    "value": "Look forward to learning how you come up with new recipes and what other services you offer."
                }
            ],
            "industry": "Food",
            "job_title": "Chef",
            "no_of_employees": "1-20",
            "org": "Cooking Org",
            "purchasing_time_frame": "1-3 months",
            "role_in_purchase_process": "Influencer",
            "language": "en-US",
            "source_id": "4816766181770"
        }

        print("Sending POST request to register a new webinar participant...")
        response = requests.post(url, json=payload, headers=headers)

    elif REQUEST_TYPE == "GET":
        # Query parameters for listing registrants (optional)
        params = {
            "status": "approved",  # Options: pending, approved, denied
            "page_size": 100
        }
        
        print("Sending GET request to list all webinar registrants...")
        response = requests.get(url, params=params, headers=headers)
        
    else:
        print(f"Error: Invalid REQUEST_TYPE '{REQUEST_TYPE}'. Must be either 'GET' or 'POST'.")
        exit(1)
    
    # Print status code
    print(f"Status Code: {response.status_code}")
    
    # Print response headers
    print("\nResponse Headers:")
    for header, value in response.headers.items():
        print(f"{header}: {value}")
    
    # Print formatted response JSON if available
    try:
        response_json = response.json()
        print("\nResponse Body:")
        print(json.dumps(response_json, indent=2))
        
        if REQUEST_TYPE == "POST" and response.status_code == 201:
            print("\nSuccess! Webinar registration was created.")
            print(f"Registration ID: {response_json.get('registrant_id')}")
            print(f"Join URL: {response_json.get('join_url')}")
            
        elif REQUEST_TYPE == "GET" and response.status_code == 200:
            print("\nSuccess! Retrieved webinar registrants list.")
            registrants = response_json.get("registrants", [])
            total_records = response_json.get("total_records", 0)
            print(f"Total registrants: {total_records}")
            
            if registrants:
                print(f"Showing {len(registrants)} registrants:")
                for i, reg in enumerate(registrants, 1):
                    print(f"\n{i}. {reg.get('first_name')} {reg.get('last_name')} ({reg.get('email')})")
                    print(f"   Status: {reg.get('status')}")
                    print(f"   Registered: {reg.get('create_time')}")
        else:
            print(f"\nError: API request failed. Status code: {response.status_code}")
            
    except ValueError:
        print("\nResponse Body (not JSON):")
        print(response.text)
        
except Exception as e:
    print(f"An error occurred while making the request: {str(e)}")
