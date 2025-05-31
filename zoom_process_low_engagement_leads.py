import os
import requests
import time
import logging

# --- Configuration ---
# !!! SET THIS TO THE CURRENT WEBINAR WEEK NUMBER !!!
WEBINAR_WEEK_NUMBER = 23  # Example: Set this to the integer for the current week

# Tags
REGISTRATION_TAG_COMMON = "lead: amazon masterclass (swe)"
REGISTRATION_TAG_WEEK_PREFIX = "webinar week: "
WEBINAR_SPECIFIC_TAG = f"{REGISTRATION_TAG_WEEK_PREFIX}{WEBINAR_WEEK_NUMBER}"

EXCLUSION_TAGS = ["high engagement", "hot lead", "no-show"]
LOW_ENGAGEMENT_TAG = "low engagement" # The tag to add

# --- Logger Setup ---
log_file_name = 'ghl_low_engagement_tagging.log'
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.setLevel(logging.INFO)
    # File Handler
    fh = logging.FileHandler(log_file_name, mode='a')
    fh.setLevel(logging.INFO)
    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

# --- Environment Variable Loading ---
logger.info("Script started. Attempting to load environment variables...")
if os.getenv("RENDER") is None: # Simple check if not in a Render-like env
    try:
        from dotenv import load_dotenv
        if load_dotenv():
            logger.info("Successfully loaded environment variables from .env file.")
        else:
            logger.info("dotenv loaded, but no .env file found or it was empty.")
    except ImportError:
        logger.warning("dotenv library not found, skipping .env file loading.")
        pass
else:
    logger.info("Render-like environment detected, skipping .env file loading.")

# --- GHL API Configuration ---
GHL_API_TOKEN = os.getenv("GHL_API_TOKEN")
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID") # Required for contact search
GHL_API_VERSION = "2021-07-28"
GHL_BASE_URL = "https://services.leadconnectorhq.com"

if not GHL_API_TOKEN:
    logger.critical("GHL_API_TOKEN environment variable is missing.")
    raise ValueError("GHL_API_TOKEN is not set.")
if not GHL_LOCATION_ID:
    logger.critical("GHL_LOCATION_ID environment variable is missing.")
    raise ValueError("GHL_LOCATION_ID is not set.")

# --- Helper Function for GHL API Calls ---
def make_ghl_api_request(method, endpoint, headers=None, json_payload=None, params=None):
    """Makes a request to the GHL API and handles common errors."""
    base_headers = {
        "Authorization": f"Bearer {GHL_API_TOKEN}",
        "Version": GHL_API_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if headers:
        base_headers.update(headers)

    url = f"{GHL_BASE_URL}{endpoint}"
    try:
        if method.upper() == "POST":
            response = requests.post(url, headers=base_headers, json=json_payload, params=params)
        elif method.upper() == "GET": # Though not used here, good for future
            response = requests.get(url, headers=base_headers, params=params)
        else:
            logger.error(f"Unsupported HTTP method: {method}")
            return None

        response.raise_for_status() # Will raise an HTTPError for bad responses (4XX or 5XX)
        
        if response.status_code == 204: # No content, but successful
            return {} 
        return response.json()

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error for {method} {url}: {e.response.status_code} {e.response.reason}. Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Request Exception for {method} {url}: {e}")
    except ValueError: # Includes JSONDecodeError
        logger.error(f"JSON Decode Error for {method} {url}. Response text: {response.text if 'response' in locals() else 'N/A'}")
    return None

# --- Main Script Logic ---
def tag_low_engagement_contacts():
    logger.info(f"Starting low engagement tagging for webinar week: {WEBINAR_WEEK_NUMBER}")
    logger.info(f"Searching for contacts with tags: '{REGISTRATION_TAG_COMMON}' AND '{WEBINAR_SPECIFIC_TAG}'")
    logger.info(f"Will exclude if contact has any of: {EXCLUSION_TAGS}")
    logger.info(f"Will add tag: '{LOW_ENGAGEMENT_TAG}'")

    contacts_to_tag_low_engagement = []
    current_page = 1
    page_limit = 100 # GHL recommended max for pageLimit for some endpoints, adjust if needed
    total_fetched = 0
    
    while True:
        search_payload = {
            "locationId": GHL_LOCATION_ID,
            "page": current_page,
            "pageLimit": page_limit,
            "filters": [ # Default group is AND
                {
                    "field": "tags",
                    "operator": "contains",
                    "value": REGISTRATION_TAG_COMMON
                },
                {
                    "field": "tags",
                    "operator": "contains",
                    "value": WEBINAR_SPECIFIC_TAG
                }
            ]
            # Not adding sort for now, default sort is fine.
        }
        
        logger.info(f"Fetching page {current_page} of contacts...")
        response_data = make_ghl_api_request("POST", "/contacts/search", json_payload=search_payload)

        if not response_data or "contacts" not in response_data:
            logger.error("Failed to fetch contacts or received an unexpected response format.")
            break

        contacts_on_page = response_data.get("contacts", [])
        total_contacts_in_search = response_data.get("total", 0)
        
        if not contacts_on_page and total_fetched == 0 and current_page == 1:
            logger.info("No contacts found matching the initial registration tags.")
            break
        
        total_fetched += len(contacts_on_page)

        for contact in contacts_on_page:
            contact_id = contact.get("id")
            contact_email = contact.get("email", "N/A")
            existing_tags = [tag.lower() for tag in contact.get("tags", [])] # Normalize to lower for comparison

            # Check if any exclusion tag is present
            has_exclusion_tag = any(exc_tag.lower() in existing_tags for exc_tag in EXCLUSION_TAGS)
            
            # Check if already has low engagement tag
            has_low_engagement_tag = LOW_ENGAGEMENT_TAG.lower() in existing_tags

            if not has_exclusion_tag and not has_low_engagement_tag:
                contacts_to_tag_low_engagement.append(contact_id)
                logger.info(f"Identified contact for '{LOW_ENGAGEMENT_TAG}' tagging: ID {contact_id}, Email: {contact_email}")
            else:
                logger.debug(f"Skipping contact ID {contact_id}, Email: {contact_email}. Has exclusion tag: {has_exclusion_tag}, Has low engagement tag: {has_low_engagement_tag}")
        
        if total_fetched >= total_contacts_in_search or not contacts_on_page:
            logger.info(f"Finished fetching all contacts. Total fetched: {total_fetched} out of {total_contacts_in_search}.")
            break # Exit pagination loop
        
        current_page += 1
        time.sleep(1) # Be respectful to the API

    # Now tag the identified contacts
    if contacts_to_tag_low_engagement:
        logger.info(f"Found {len(contacts_to_tag_low_engagement)} contacts to tag as '{LOW_ENGAGEMENT_TAG}'.")
        for contact_id in contacts_to_tag_low_engagement:
            tag_payload = {"tags": [LOW_ENGAGEMENT_TAG]}
            logger.info(f"Adding tag '{LOW_ENGAGEMENT_TAG}' to contact ID: {contact_id}")
            tag_response = make_ghl_api_request("POST", f"/contacts/{contact_id}/tags", json_payload=tag_payload)
            
            if tag_response and isinstance(tag_response.get("tags"), list): # GHL returns list of all tags on success
                logger.info(f"Successfully tagged contact ID: {contact_id}. Current tags: {tag_response.get('tags')}")
            else:
                logger.error(f"Failed to tag contact ID: {contact_id}. Response: {tag_response}")
            time.sleep(1) # API rate limiting
    else:
        logger.info("No new contacts to tag as low engagement.")

    logger.info("Low engagement tagging process complete.")

if __name__ == "__main__":
    tag_low_engagement_contacts()