# Filename: webinarjam_followup_processor.py
import os
import requests
import time
import logging
import json
from dotenv import load_dotenv # Keep dotenv import for local dev convenience

# --- Setup Logger ---
log_file_name = 'webinarjam_followup.log' # Specific log file
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    fh = logging.FileHandler(log_file_name, mode='a')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)

# --- Environment Variable Loading ---
logger.info("Script started (Follow-up Processor). Attempting to load environment variables...")
if os.getenv("RENDER") is None:
    if load_dotenv():
        logger.info("Successfully loaded environment variables from .env file.")
    else:
        logger.info("dotenv loaded, but no .env file found or it was empty.")
else:
    logger.info("RENDER environment detected, skipping .env file loading.")

# --- Configuration ---
WEBINARJAM_API_KEY = os.getenv("WEBINARJAM_API_KEY")
WEBINAR_ID = os.getenv("WEBINARJAM_WEBINAR_ID")
WEBINAR_SCHEDULE_ID = os.getenv("WEBINARJAM_WEBINAR_SCHEDULE_ID") # Specific session ID
REGISTRANTS_URL = "https://api.webinarjam.com/webinarjam/registrants"
GHL_WEBHOOK_URL = os.getenv("GHL_WEBHOOK_URL_PYTHON_LIVE_PROCESS")

# --- Environment Variable Check ---
logger.info("Checking for required environment variables...")
missing_vars = []
if not WEBINARJAM_API_KEY: missing_vars.append("WEBINARJAM_API_KEY")
if not WEBINAR_ID: missing_vars.append("WEBINARJAM_WEBINAR_ID")
if not WEBINAR_SCHEDULE_ID: missing_vars.append("WEBINARJAM_WEBINAR_SCHEDULE_ID")
if not GHL_WEBHOOK_URL: missing_vars.append("GHL_WEBHOOK_URL_PYTHON_LIVE_PROCESS")

if missing_vars:
    error_msg = f"Critical: Missing environment variables: {', '.join(missing_vars)}. Please set them."
    logger.critical(error_msg)
    raise RuntimeError(error_msg)
else:
    logger.info("All required environment variables are present.")

# --- Helper Functions (Identical to Script 1) ---
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
        logger.warning(f"Time string '{time_str}' not in HH:MM:SS or MM:SS. Defaulting to {default_seconds}s.")
        return default_seconds
    except ValueError:
        logger.error(f"ValueError converting '{time_str}' to seconds. Defaulting to {default_seconds}s.")
        return default_seconds

def determine_tag(registrant):
    attended_live = str(registrant.get("attended_live", "")).lower()
    time_live_str = registrant.get("time_live", "00:00:00")
    if attended_live == "yes":
        total_seconds = parse_time_to_seconds(time_live_str)
        return "high engagement" if total_seconds >= 5400 else "low engagement"
    return "no-show"

def determine_hot_lead(registrant): # Still useful to send correct hot_lead status (likely 0 for these groups)
    if str(registrant.get("attended_live", "")).lower() != "yes":
        return 0
    total_seconds = parse_time_to_seconds(registrant.get("time_live", "00:00:00"))
    return 1 if total_seconds >= 7200 else 0

def send_to_ghl(registrant, tag, purchased, hot_lead_status):
    payload = {
        "webinar_id": WEBINAR_ID,
        "schedule_id": WEBINAR_SCHEDULE_ID,
        "first_name": registrant.get("first_name"),
        "last_name": registrant.get("last_name"),
        "email": registrant.get("email"),
        "phone": registrant.get("phone_number") or registrant.get("phone"),
        "tag": tag,
        "purchased": purchased,
        "hot_lead": hot_lead_status,
        "time_live": registrant.get("time_live"),
        "attended_live_api_value": registrant.get("attended_live")
    }
    payload_cleaned = {k: v for k, v in payload.items() if v is not None}
    try:
        response = requests.post(GHL_WEBHOOK_URL, json=payload_cleaned, timeout=30)
        response.raise_for_status()
        logger.info(f"Successfully sent {registrant.get('email')} to GHL. Tag: '{tag}', Hot Lead: {hot_lead_status}.")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error for {registrant.get('email')}: {e.response.status_code}. Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException for {registrant.get('email')}: {e}")
    time.sleep(2) # Rate limiting

# --- Main Processing Logic for Low Engagement & No-Shows ---
def process_low_engagement_no_shows():
    logger.info(f"Starting processing for LOW ENGAGEMENT & NO-SHOWS: Webinar '{WEBINAR_ID}', Schedule '{WEBINAR_SCHEDULE_ID}'.")
    page = 1
    processed_count = 0
    api_fetched_count = 0

    while True:
        api_payload = {
            "api_key": WEBINARJAM_API_KEY,
            "webinar_id": WEBINAR_ID,
            "schedule": WEBINAR_SCHEDULE_ID,
            "date_range": 0,
            # No "attended_live" filter here, to fetch ALL registrants for the schedule
            "page": page
        }
        logger.debug(f"Fetching ALL registrants (Page {page}): {api_payload}")
        try:
            response = requests.post(REGISTRANTS_URL, data=api_payload, timeout=60)
            response.raise_for_status()
            response_json = response.json()

            if response_json.get("status") == "success":
                registrants_wrapper = response_json.get("registrants", {})
                if not isinstance(registrants_wrapper, dict):
                    logger.error(f"API Error: 'registrants' field invalid. Value: {registrants_wrapper}"); break
                
                data = registrants_wrapper.get("data", [])
                current_page_api = int(registrants_wrapper.get("current_page", 0))
                total_pages_api = int(registrants_wrapper.get("total_pages", 0))

                if not data and page == 1: logger.info("No registrants found for this schedule."); break
                if not data: logger.info(f"No more registrants. Last page: {page-1}."); break

                for registrant in data:
                    api_fetched_count += 1
                    email = registrant.get("email")
                    if not isinstance(registrant, dict) or not email:
                        logger.warning(f"Skipping invalid entry or missing email: {registrant}"); continue

                    tag = determine_tag(registrant)

                    if tag == "low engagement" or tag == "no-show":
                        logger.info(f"MATCH: {email} is '{tag}'. Sending to GHL.")
                        purchased = 1 if str(registrant.get("purchased_live", "")).lower() == "yes" else 0
                        is_hot_lead = determine_hot_lead(registrant) # Will be 0 for no-shows, likely 0 for low engagement
                        send_to_ghl(registrant, tag, purchased, is_hot_lead)
                        processed_count += 1
                    # else:
                        # logger.debug(f"NO MATCH: {email}. Tag: {tag}. Not sending for this script (likely high engagement).")
                
                if current_page_api >= total_pages_api or not data :
                    logger.info(f"End of registrant list. Current API page: {current_page_api}, Total: {total_pages_api}."); break
                page += 1
            elif response_json.get("status") == "error":
                logger.error(f"API Error: {response_json.get('message', 'Unknown error')}. Response: {json.dumps(response_json)}"); break
            else:
                logger.error(f"API Unexpected Status: {response_json.get('status')}. Response: {json.dumps(response_json)}"); break
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP Error (Page {page}): {e}. Response: {e.response.text if e.response else 'N/A'}"); break
        except requests.exceptions.RequestException as e:
            logger.error(f"Request Error (Page {page}): {e}"); break
        except json.JSONDecodeError:
            logger.error(f"JSON Decode Error (Page {page}). Response: {response.text if 'response' in locals() else 'N/A'}"); break
        except Exception as e:
            logger.critical(f"Critical Error (Page {page}): {e}", exc_info=True); break
            
    logger.info(f"Low Engagement & No-Show processing finished. API Fetched: {api_fetched_count}, Sent to GHL: {processed_count}.")

# --- Main Execution ---
if __name__ == "__main__":
    logger.info("--- Low Engagement & No-Show Processor Script Started ---")
    process_low_engagement_no_shows()
    logger.info("--- Low Engagement & No-Show Processor Script Finished ---")