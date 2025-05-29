import os
import requests
# import time # Not needed if not sending to GHL with delays
import json

# --- Environment Variable Loading ---
if os.getenv("RENDER") is None:
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("Loaded environment variables from .env file (if found).")
    except ImportError:
        print("dotenv library not found, skipping .env file loading.")
        pass

# --- Configuration ---
WEBINARJAM_API_KEY = os.getenv("WEBINARJAM_API_KEY")
WEBINAR_ID = os.getenv("WEBINARJAM_WEBINAR_ID")
WEBINAR_SCHEDULE_ID = os.getenv("WEBINARJAM_WEBINAR_SCHEDULE_ID")
registrants_url = "https://api.webinarjam.com/webinarjam/registrants"
# ghl_webhook_url is removed as we are not sending data

# --- Environment Variable Check ---
required_vars_check = {
    "WEBINARJAM_API_KEY": WEBINARJAM_API_KEY,
    "WEBINARJAM_WEBINAR_ID": WEBINAR_ID,
    "WEBINARJAM_WEBINAR_SCHEDULE_ID": WEBINAR_SCHEDULE_ID
}
missing_vars_list = [name for name, value in required_vars_check.items() if not value]
if missing_vars_list:
    error_message = f"ERROR: Required environment variables are missing or empty: {', '.join(missing_vars_list)}. Please set them."
    print(error_message)
    exit(1)

# --- Helper Functions ---

def parse_time_string(time_str):
    """ Safely parses HH:MM:SS strings into total seconds. Handles None or invalid formats. """
    if not time_str or ':' not in time_str: return 0
    try:
        cleaned_time_str = time_str.strip()
        hours, minutes, seconds = map(int, cleaned_time_str.split(':'))
        return hours * 3600 + minutes * 60 + seconds
    except (ValueError, TypeError, AttributeError):
        # print(f"Warning: Could not parse time string '{time_str}'. Defaulting to 0 seconds.")
        return 0

def determine_tag_replay(registrant):
    """ Determine tag based on 'attended_replay' field (expecting "Yes", "No", or other). """
    attended_status_val = registrant.get("attended_replay")

    if isinstance(attended_status_val, str):
        status_lower = attended_status_val.lower()
        if status_lower == 'yes':
            time_replay_str = registrant.get("time_replay", "00:00:00")
            total_seconds = parse_time_string(time_replay_str)
            HIGH_ENGAGEMENT_THRESHOLD_SECONDS = 5400 # 1h 30min
            if total_seconds >= HIGH_ENGAGEMENT_THRESHOLD_SECONDS:
                return "replay - high engagement"
            else:
                return "replay - low engagement" if total_seconds > 0 else "replay - watched 0 min"
        elif status_lower == 'no':
            return "replay - did not attend"
        else: # Unrecognized string value
            return f"replay - unknown string status ({attended_status_val})"
    elif attended_status_val is None: # Explicitly handle None
        return "replay - status missing"
    else: # Handle other types if necessary (e.g., integers if API is inconsistent)
        return f"replay - unexpected status type ({attended_status_val})"


def determine_hot_lead_replay(registrant):
    """ Determine hot lead status based on 'attended_replay' ("Yes"/"No") and time. """
    attended_status_val = registrant.get("attended_replay")

    if isinstance(attended_status_val, str) and attended_status_val.lower() == 'yes':
        time_replay_str = registrant.get("time_replay", "00:00:00")
        total_seconds = parse_time_string(time_replay_str)
        HOT_LEAD_THRESHOLD_SECONDS = 7200 # 2 hours
        return 1 if total_seconds >= HOT_LEAD_THRESHOLD_SECONDS else 0
    else:
        # Not a hot lead if they didn't attend replay or status is unknown/"No"
        return 0

# --- Main Processing Function ---
def process_all_registrants_for_replay_data_and_print():
    print(f"Processing ALL registrants (attended_replay=0) for webinar {WEBINAR_ID}, schedule {WEBINAR_SCHEDULE_ID} to analyze replay data...")
    page = 1
    total_registrants_processed = 0
    processed_emails = set()

    while True:
        print(f"\nFetching page {page}...")
        api_payload = {
            "api_key": WEBINARJAM_API_KEY,
            "webinar_id": WEBINAR_ID,
            "schedule": WEBINAR_SCHEDULE_ID,
            "date_range": 0,
            "attended_replay": 1, # Fetch everyone that attended the replay
            "page": page
        }
        try:
            response = requests.post(registrants_url, data=api_payload)
            response.raise_for_status()
            response_json = response.json()

            if response_json.get("status") == "success":
                registrants_data = response_json.get("registrants", {})
                data = registrants_data.get("data", [])
                if not data:
                    print(f"No more registrants found on page {page} or subsequent pages.")
                    break
                print(f"Found {len(data)} registrants on page {page}. Processing...")

                for registrant in data:
                    registrant_email = registrant.get("email")
                    if not registrant_email:
                        print(f"Skipping registrant with missing email.")
                        continue
                    if registrant_email in processed_emails:
                        print(f"Skipping already processed email: {registrant_email}")
                        continue
                    processed_emails.add(registrant_email)
                    total_registrants_processed += 1

                    # Determine purchased status (handles "Yes"/1 or "No"/0/None etc.)
                    purchased_value = registrant.get("purchased_replay")
                    purchased = 1 if (isinstance(purchased_value, str) and purchased_value.lower() == 'yes') or purchased_value == 1 else 0

                    # Determine tag and hot lead status based on replay data
                    tag = determine_tag_replay(registrant)
                    hot_lead = determine_hot_lead_replay(registrant)

                    # Construct the output payload dictionary
                    output_payload_to_print = {
                        "webinar_id": WEBINAR_ID,
                        "first_name": registrant.get("first_name"),
                        "last_name": registrant.get("last_name"),
                        "email": registrant_email,
                        "phone": registrant.get("phone"),
                        "tag": tag, # This is the segment tag
                        "purchased_replay": purchased,
                        "hot_lead_replay": hot_lead,
                        "attended_replay_api_value": registrant.get("attended_replay"), # Raw value from API
                        "time_replay": registrant.get("time_replay", "00:00:00")
                    }

                    # Print the constructed payload to the terminal
                    print("\n--- Registrant Info with Tag ---")
                    print(json.dumps(output_payload_to_print, indent=2))
                    # If you want a more concise output, you could do:
                    # print(f"Email: {registrant_email}, Tag: {tag}, Purchased: {purchased}, Hot Lead: {hot_lead}")

                page += 1 # Move to next page

            else: # Handle WebinarJam API error status
                api_error_message = response_json.get('message') or response_json.get('error', 'Unknown API error')
                print(f"WebinarJam API returned an error: Status='{response_json.get('status')}', Message='{api_error_message}'")
                break

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error contacting WebinarJam API: {http_err} (Status: {response.status_code if 'response' in locals() else 'N/A'})")
            if 'response' in locals(): print(f"Response: {response.text}")
            break
        except requests.exceptions.RequestException as req_err:
            print(f"Network error contacting WebinarJam API: {req_err}")
            break
        except json.JSONDecodeError as json_err:
             print(f"Failed to decode JSON response from WebinarJam API: {json_err}")
             if 'response' in locals(): print(f"Response text: {response.text}")
             break
        except Exception as e:
            print(f"An unexpected error occurred during page {page} processing ({type(e).__name__}): {e}")
            import traceback
            traceback.print_exc()
            break

    # --- Processing Summary ---
    print(f"\n--- Processing Complete ---")
    print(f"Total unique registrants processed and printed: {total_registrants_processed}")

# --- Run the Script ---
if __name__ == "__main__":
    process_all_registrants_for_replay_data_and_print()

print("\nScript execution finished.")