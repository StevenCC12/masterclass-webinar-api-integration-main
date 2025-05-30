import os
import requests
import time
import logging # Import the logging module
import json # For logging complex objects like API responses if needed

# --- Setup Logger ---
log_file_name = 'webinarjam_live_processing.log'
logger = logging.getLogger(__name__) # Using __name__ is a common practice
logger.setLevel(logging.DEBUG) # Set the logger to capture all levels of messages

# Prevent duplicate handlers if script is run multiple times in same session (e.g. in a notebook)
if not logger.handlers:
    # File Handler (writes DEBUG and higher messages to log file)
    fh = logging.FileHandler(log_file_name, mode='a') # 'a' for append
    fh.setLevel(logging.DEBUG)

    # Console Handler (prints INFO and higher messages to console)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add Handlers to logger
    logger.addHandler(fh)
    logger.addHandler(ch)

# --- Environment Variable Loading ---
logger.info("Script started. Attempting to load environment variables...")
if os.getenv("RENDER") is None:  # Render sets this variable automatically
    try:
        from dotenv import load_dotenv
        if load_dotenv():
            logger.info("Successfully loaded environment variables from .env file.")
        else:
            logger.info("dotenv loaded, but no .env file found or it was empty.")
    except ImportError:
        logger.warning("dotenv library not found, skipping .env file loading.")
        pass # dotenv not installed, proceed without it
else:
    logger.info("RENDER environment detected, skipping .env file loading.")


# --- Configuration ---
WEBINARJAM_API_KEY = os.getenv("WEBINARJAM_API_KEY")
WEBINAR_ID = os.getenv("WEBINARJAM_WEBINAR_ID")
WEBINAR_SCHEDULE_ID = os.getenv("WEBINARJAM_WEBINAR_SCHEDULE_ID") # ID of a specific session/date
registrants_url = "https://api.webinarjam.com/webinarjam/registrants"
ghl_webhook_url = os.getenv("GHL_WEBHOOK_URL_PYTHON_LIVE_PROCESS")

# --- Environment Variable Check ---
logger.info("Checking for required environment variables...")
missing_vars = []
if not WEBINARJAM_API_KEY: missing_vars.append("WEBINARJAM_API_KEY")
if not WEBINAR_ID: missing_vars.append("WEBINARJAM_WEBINAR_ID")
if not WEBINAR_SCHEDULE_ID: missing_vars.append("WEBINARJAM_WEBINAR_SCHEDULE_ID")
if not ghl_webhook_url: missing_vars.append("GHL_WEBHOOK_URL_PYTHON_LIVE_PROCESS")

if missing_vars:
    error_msg = f"One or more required environment variables are missing: {', '.join(missing_vars)}. Please ensure they are set."
    logger.critical(error_msg)
    raise RuntimeError(error_msg)
else:
    logger.info("All required environment variables are present.")

# --- Helper Functions ---

def parse_time_to_seconds(time_str, default_seconds=0):
    """Converts HH:MM:SS string to seconds. Returns default_seconds on error."""
    if not isinstance(time_str, str) or ':' not in time_str:
        return default_seconds
    try:
        parts = time_str.split(':')
        if len(parts) == 3:
            h, m, s = map(int, parts)
            return h * 3600 + m * 60 + s
        elif len(parts) == 2: # Handle MM:SS
            m, s = map(int, parts)
            return m * 60 + s
        logger.warning(f"Time string '{time_str}' not in HH:MM:SS or MM:SS format. Defaulting to {default_seconds}s.")
        return default_seconds
    except ValueError:
        logger.error(f"ValueError converting time string '{time_str}' to seconds. Defaulting to {default_seconds}s.")
        return default_seconds


def determine_tag(registrant):
    """ Determine the tag for the registrant based on their attendance and engagement. """
    attended_live = str(registrant.get("attended_live", "")).lower()
    time_live_str = registrant.get("time_live", "00:00:00")

    if attended_live == "yes":
        total_seconds = parse_time_to_seconds(time_live_str)
        if total_seconds >= 5400:  # 1h 30min (5400 seconds)
            return "high engagement"
        else:
            return "low engagement"
    else:
        return "no-show"

def determine_hot_lead(registrant):
    """ Determine if the registrant is a hot lead based on their time spent in the live room. """
    attended_live = str(registrant.get("attended_live", "")).lower()
    if attended_live != "yes":
        return 0

    time_live_str = registrant.get("time_live", "00:00:00")
    total_seconds = parse_time_to_seconds(time_live_str)
    return 1 if total_seconds >= 7200 else 0  # 2 hours (7200 seconds)

def send_to_ghl(registrant, tag, purchased, hot_lead):
    """ Send registrant data to GHL via inbound webhook. """
    payload = {
        "webinar_id": WEBINAR_ID,
        "schedule_id": WEBINAR_SCHEDULE_ID,
        "first_name": registrant.get("first_name"),
        "last_name": registrant.get("last_name"),
        "email": registrant.get("email"),
        "phone": registrant.get("phone_number") or registrant.get("phone"),
        "tag": tag,
        "purchased": purchased,
        "hot_lead": hot_lead,
        "time_live": registrant.get("time_live"),
        "attended_live_api_value": registrant.get("attended_live")
    }
    payload_cleaned = {k: v for k, v in payload.items() if v is not None}

    try:
        response = requests.post(ghl_webhook_url, json=payload_cleaned, timeout=30)
        response.raise_for_status()
        logger.info(f"Successfully sent registrant {registrant.get('email')} to GHL. Tag: '{tag}', Purchased: {purchased}, Hot Lead: {hot_lead}.")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error sending {registrant.get('email')} to GHL: {e.response.status_code} {e.response.reason}. Response: {e.response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"RequestException sending {registrant.get('email')} to GHL: {e}")
    except Exception as e:
        logger.error(f"Unexpected error sending {registrant.get('email')} to GHL: {e}")

    time.sleep(2)

def process_registrants(fetch_attended_filter=None):
    """
    Fetch registrants from WebinarJam, determine their tags, purchased status,
    and hot lead status, then send them to GHL.
    :param fetch_attended_filter: Optional. See WebinarJam API docs for attended_live values.
                                  None will be treated as 0 (All registrants).
    """
    page = 1
    total_registrants_processed_successfully = 0
    total_api_registrants_fetched = 0

    # Determine attended_live value for API and logging
    api_attended_live_value = 0 # Default to "All registrants"
    log_status_text = "all registrants (attended_live=0)"
    if fetch_attended_filter is not None:
        api_attended_live_value = fetch_attended_filter
        if fetch_attended_filter == 1:
            log_status_text = "attended (attended_live=1)"
        elif fetch_attended_filter == 2:
            log_status_text = "no-shows (attended_live=2)"
        # Add other specific cases as needed from API docs (3, 4)
        else:
            log_status_text = f"custom filter (attended_live={fetch_attended_filter})"

    logger.info(f"Processing {log_status_text} for webinar_id '{WEBINAR_ID}' and schedule_id '{WEBINAR_SCHEDULE_ID}'.")

    while True:
        payload_api = {
            "api_key": WEBINARJAM_API_KEY,
            "webinar_id": WEBINAR_ID,
            "schedule": WEBINAR_SCHEDULE_ID,
            "date_range": 0, # All Time for this schedule
            "page": page,
            "attended_live": api_attended_live_value # Explicitly set based on logic above
        }

        logger.debug(f"Fetching registrants: Page {page}, Payload: {json.dumps(payload_api)}")

        try:
            response = requests.post(registrants_url, data=payload_api, timeout=60)
            response.raise_for_status()
            response_json = response.json()
            # logger.debug(f"API Response (Page {page}): {json.dumps(response_json, indent=2)[:500]}...")

            if response_json.get("status") == "success":
                registrants_data_wrapper = response_json.get("registrants", {})
                if not isinstance(registrants_data_wrapper, dict):
                    logger.error(f"API Error: 'registrants' field is not a dictionary. Value: {registrants_data_wrapper}")
                    break # Stop processing if response structure is unexpected

                data = registrants_data_wrapper.get("data", [])
                
                # Get current_page and total_pages from API, with fallbacks
                current_page_api_str = registrants_data_wrapper.get("current_page", str(page))
                try: current_page_api = int(current_page_api_str)
                except (ValueError, TypeError): current_page_api = page # Fallback to script's page
                
                total_pages_api_str = registrants_data_wrapper.get("total_pages", "0")
                try: total_pages_api = int(total_pages_api_str)
                except (ValueError, TypeError): total_pages_api = 0 # Fallback to 0 if parsing fails


                # If no data is returned for the current page request, we are done.
                if not data:
                    if page == 1:
                        logger.info("No registrants found matching the criteria on the first page.")
                    else:
                        logger.info(f"No more registrants found. Last page successfully processed was {page-1}.")
                    break # Exit the while loop

                # Process the data from the current page
                for registrant in data:
                    total_api_registrants_fetched += 1
                    if not isinstance(registrant, dict) or not registrant.get("email"):
                        logger.warning(f"Skipping invalid registrant entry or missing email: {registrant}")
                        continue

                    email = registrant.get("email")
                    # logger.debug(f"Processing: {email}, Attended: {registrant.get('attended_live')}, Time: {registrant.get('time_live')}")

                    tag = determine_tag(registrant)
                    purchased_val = str(registrant.get("purchased_live", "")).lower()
                    purchased = 1 if purchased_val == "yes" else 0
                    hot_lead = determine_hot_lead(registrant)

                    send_to_ghl(registrant, tag, purchased, hot_lead)
                    total_registrants_processed_successfully +=1

                # Decide whether to fetch another page
                # If API reports a positive total_pages and we've reached it, stop.
                if total_pages_api > 0 and current_page_api >= total_pages_api:
                    logger.info(f"Reached end of registrant list as per API pagination. Current API page: {current_page_api}, Total API pages: {total_pages_api}.")
                    break # Exit while loop
                
                # If total_pages_api is 0 (or less than current_page_api in a way that suggests an issue)
                # AND we received data (checked by 'if not data:' already),
                # we assume there might be more data on the next page.
                # The loop will terminate in the next iteration if 'if not data:' becomes true.

                page += 1 # Go to next page

            elif response_json.get("status") == "error":
                error_message = response_json.get("message", "Unknown API error")
                logger.error(f"WebinarJam API error: {error_message} (Full response: {json.dumps(response_json)})")
                break
            else:
                logger.error(f"WebinarJam API returned an unexpected status or malformed response. Status: {response_json.get('status')}. Response: {json.dumps(response_json)}")
                break

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching registrants (Page {page}): {e}. Response: {e.response.text if e.response else 'No response text'}")
            break
        except requests.exceptions.Timeout:
            logger.error(f"Timeout error fetching registrants (Page {page}).")
            time.sleep(5)
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching registrants (Page {page}): {e}")
            break
        except json.JSONDecodeError:
            logger.error(f"JSON decode error for API response (Page {page}). Response text: {response.text if 'response' in locals() and response else 'N/A'}")
            break
        except Exception as e:
            logger.critical(f"An critical unexpected error occurred in process_registrants (Page {page}): {e}", exc_info=True)
            break

    logger.info(f"Total registrants fetched from API: {total_api_registrants_fetched}")
    logger.info(f"Total registrants for whom GHL send was attempted: {total_registrants_processed_successfully}")
    logger.info("Registrant processing finished.")


# --- Main Execution ---
if __name__ == "__main__":
    logger.info("Script execution started via __main__.")

    # Option 1: Process ALL registrants (attended_live=0) for the specified webinar and schedule.
    # This will now explicitly send 'attended_live: 0' to the API.
    process_registrants(fetch_attended_filter=None) # None defaults to fetching all (API value 0)

    # Option 2: Process only "no-shows" (attended_live=2 according to docs).
    # process_registrants(fetch_attended_filter=2)

    # Option 3: Process only those who "attended" (attended_live=1).
    # process_registrants(fetch_attended_filter=1)

    logger.info("Script execution completed.")