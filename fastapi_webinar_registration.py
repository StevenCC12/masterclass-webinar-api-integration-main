from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr
import os
import requests
from dotenv import load_dotenv
from authlib.integrations.requests_client import OAuth2Session
import datetime

# Load environment variables
load_dotenv()

# Get environment variables
ZOOM_ACCOUNT_ID = os.getenv("ZOOM_ACCOUNT_ID")
ZOOM_CLIENT_ID_AUTO_REG = os.getenv("ZOOM_CLIENT_ID_AUTO_REG")
ZOOM_CLIENT_SECRET_AUTO_REG = os.getenv("ZOOM_CLIENT_SECRET_AUTO_REG")
WEBINAR_ID = os.getenv("WEBINAR_ID")

# Check if all environment variables are set
if not all([ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID_AUTO_REG, ZOOM_CLIENT_SECRET_AUTO_REG, WEBINAR_ID]):
    raise RuntimeError("One or more required environment variables are missing. Please ensure ZOOM_ACCOUNT_ID, ZOOM_CLIENT_ID, ZOOM_CLIENT_SECRET, and WEBINAR_ID are set.")

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
if not access_token:
    raise RuntimeError("Failed to obtain access token from Zoom API.")

# FastAPI app
app = FastAPI()

# Pydantic model for GHL contact data
class Contact(BaseModel):
    name: str
    email: EmailStr
    phone: str

@app.post("/register")
async def register_contact(contact: Contact):
    """
    Register a contact for the Zoom webinar.
    """
    # Split the name into first and last name
    name_parts = contact.name.split()
    first_name = name_parts[0]
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

    # Webinar registrant endpoint
    url = f"https://api.zoom.us/v2/webinars/{WEBINAR_ID}/registrants"

    # Create payload for Zoom API
    payload = {
        "first_name": first_name,
        "last_name": last_name,
        "email": contact.email,
        "phone": contact.phone
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    try:
        # Send POST request to Zoom API
        response = requests.post(url, json=payload, headers=headers)

        if response.status_code == 201:
            # Registration successful
            response_json = response.json()
            return {
                "message": "Contact successfully registered for the webinar.",
                "registrant_id": response_json.get("registrant_id"),
                "join_url": response_json.get("join_url")
            }
        else:
            # Handle API errors
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to register contact. Zoom API responded with: {response.text}"
            )
    except requests.exceptions.RequestException as e:
        # Handle request errors
        raise HTTPException(
            status_code=500,
            detail=f"An error occurred while communicating with the Zoom API: {str(e)}"
        )