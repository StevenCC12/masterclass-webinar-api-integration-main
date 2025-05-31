# zoom_utils.py
import os
import requests
import time
import logging
import json

# --- Setup Logger ---
# Configure this once, and other scripts can get this logger
log_file_name = 'zoom_processing.log' # You might want separate logs later
logger = logging.getLogger(__name__) # Using __name__ (i.e., 'zoom_utils')

if not logger.handlers: # Prevent duplicate handlers
    logger.setLevel(logging.DEBUG)
    # File Handler
    fh = logging.FileHandler(log_file_name, mode='a')
    fh.setLevel(logging.DEBUG)
    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # Add Handlers
    logger.addHandler(fh)
    logger.addHandler(ch)

# --- Environment Variable Loading ---
# Encapsulate this if preferred, or load directly where needed in main scripts.
# For simplicity here, we'll assume main scripts handle specific checks after importing.
logger.info("zoom_utils.py loaded. Attempting to load environment variables if dotenv is available...")
if os.getenv("RENDER") is None:
    try:
        from dotenv import load_dotenv
        if load_dotenv():
            logger.info("Successfully loaded .env file variables.")
        else:
            logger.info(".env file not found or empty, or dotenv.load_dotenv() returned False.")
    except ImportError:
        logger.warning("dotenv library not found, .env file loading skipped.")
        pass
else:
    logger.info("RENDER environment detected, .env file loading skipped.")


# --- Configuration (Loaded from Environment) ---
ZOOM_API_TOKEN = os.getenv("ZOOM_API_TOKEN")
# WEBINAR_ID and OCCURRENCE_ID will be passed as arguments to functions
# from the main scripts, as they might change per run.
GHL_WEBHOOK_URL = os.getenv("GHL_WEBHOOK_URL_PYTHON_LIVE_PROCESS") # Ensure this is the correct GHL webhook

ZOOM_BASE_URL = "https://api.zoom.us/v2"

# --- Helper Functions ---

def make_zoom_api_request(endpoint, params=None, api_token_override=None):
    """
    Makes a GET request to the Zoom API, handling pagination.
    api_token_override can be used if token is managed outside global scope.
    """
    token_to_use = api_token_override if api_token_override else ZOOM_API_TOKEN
    if not token_to_use:
        logger.error("Zoom API token is not available for make_zoom_api_request.")
        return None

    headers = {
        "Authorization": f"Bearer {token_to_use}",
        "Content-Type": "application/json"
    }
    url = f"{ZOOM_BASE_URL}{endpoint}"
    all_items = []
    
    # Ensure params is a dict
    current_params = params.copy() if params else {}
    current_params.setdefault("page_size", 300) # Default page_size if not provided

    page_count = 0
    while True:
        page_count += 1
        logger.debug(f"Requesting Zoom API (Page {page_count}): {url} with params: {current_params}")
        try:
            response = requests.get(url, headers=headers, params=current_params, timeout=30)
            response.raise_for_status()
            data = response.json()

            items_key = None
            if "participants" in data: items_key = "participants"
            elif "registrants" in data: items_key = "registrants" # For absentees or registrants list
            # Add other potential keys if new endpoints are used

            if items_key and items_key in data:
                all_items.extend(data[items_key])
            elif isinstance(data, list) and not data.get("next_page_token"): # Fallback for simple list response without explicit items_key
                 all_items.extend(data)
            elif not data.get("next_page_token"): # No items key, no token, probably single page or error
                logger.warning(f"Unexpected response structure or no items key found for {url}. Data: {str(data)[:200]}...")
                if not all_items: # if we haven't collected any items yet from this response
                    if isinstance(data, dict) and not any(k in data for k in ["participants", "registrants"]): # if it's a dict but not with expected keys
                        logger.error(f"Response for {url} is a dictionary but does not contain expected item lists (participants/registrants).")

            next_page_token = data.get("next_page_token")
            if next_page_token:
                current_params["next_page_token"] = next_page_token
            else:
                break # No more pages
        
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error for {url} (Page {page_count}): {e.response.status_code} {e.response.reason}. Response: {e.response.text}")
            return None 
        except requests.exceptions.RequestException as e:
            logger.error(f"RequestException for {url} (Page {page_count}): {e}")
            return None
        except json.JSONDecodeError:
            logger.error(f"JSON decode error for API response from {url} (Page {page_count}). Response: {response.text if 'response' in locals() else 'N/A'}")
            return None
    return all_items

def parse_time_to_seconds(time_str, default_seconds=0):
    if not isinstance(time_str, str) or ':' not in time_str:
        return default_seconds
    try:
        parts = time_str.split(':')
        if len(parts) == 3:
            h, m, s = map(int, parts)
            return h * 3600 + m * 60 + s
        elif len(parts) == 2:
            m, s = map(int, parts)
            return m * 60 + s
        logger.warning(f"Time string '{time_str}' not in HH:MM:SS or MM:SS format. Defaulting to {default_seconds}s.")
        return default_seconds
    except ValueError:
        logger.error(f"ValueError converting time string '{time_str}' to seconds. Defaulting to {default_seconds}s.")
        return default_seconds

def determine_tag(registrant_mock_data):
    attended_live = str(registrant_mock_data.get("attended_live", "")).lower()
    time_live_str = registrant_mock_data.get("time_live", "00:00:00")

    if attended_live == "yes":
        total_seconds = parse_time_to_seconds(time_live_str)
        if total_seconds >= 5400:  # 1h 30min
            return "high engagement"
        else:
            return "low engagement" # This script might filter these out later
    else:
        return "no-show"

def determine_hot_lead(registrant_mock_data):
    attended_live = str(registrant_mock_data.get("attended_live", "")).lower()
    if attended_live != "yes":
        return 0
    time_live_str = registrant_mock_data.get("time_live", "00:00:00")
    total_seconds = parse_time_to_seconds(time_live_str)
    return 1 if total_seconds >= 7200 else 0  # 2 hours

def send_to_ghl(registrant_data, tag, purchased_status, hot_lead_status, webinar_id, occurrence_id=None, ghl_url_override=None):
    """ Send registrant data to GHL via inbound webhook. """
    ghl_target_url = ghl_url_override if ghl_url_override else GHL_WEBHOOK_URL
    if not ghl_target_url:
        logger.error(f"GHL Webhook URL is missing. Cannot send data for {registrant_data.get('email')}.")
        return

    payload = {
        "webinar_id": webinar_id, # Zoom's Webinar ID
        "schedule_id": occurrence_id, # Zoom's Occurrence ID (can be None)
        "first_name": registrant_data.get("first_name"),
        "last_name": registrant_data.get("last_name"),
        "email": registrant_data.get("email"),
        "phone": registrant_data.get("phone_number"), # Might be None
        "tag": tag,
        "purchased": purchased_status,
        "hot_lead": hot_lead_status,
        "time_live": registrant_data.get("time_live"),
        "attended_live_api_value": registrant_data.get("attended_live"), # "yes" or "no"
        "zoom_registrant_id": registrant_data.get("zoom_id") # Consistent field for Zoom's registrant ID
    }
    payload_cleaned = {k: v for k, v in payload.items() if v is not None} # Important for schedule_id

    try:
        response = requests.post(ghl_target_url, json=payload_cleaned, timeout=30)
        response.raise_for_status()
        logger.info(f"Successfully sent {registrant_data.get('email')} to GHL. Tag: '{tag}', Hot Lead: {hot_lead_status}.")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error sending {registrant_data.get('email')} to GHL: {e.response.status_code} {e.response.reason}. Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException sending {registrant_data.get('email')} to GHL: {e}")
    except Exception as e:
        logger.error(f"Unexpected error sending {registrant_data.get('email')} to GHL: {e}")
    
    time.sleep(1) # Reduced sleep from 2s to 1s, adjust as needed for GHL rate limits