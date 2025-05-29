import os
import requests
import time

# Load environment variables from .env only in local development
if os.getenv("RENDER") is None:  # Render sets this variable automatically
    from dotenv import load_dotenv
    load_dotenv()

EVERWEBINAR_API_KEY = os.getenv("EVERWEBINAR_API_KEY")
EVERWEBINAR_ID = os.getenv("EVERWEBINAR_ID")
EVERWEBINAR_SCHEDULE_ID = os.getenv("EVERWEBINAR_SCHEDULE_ID")
registrants_url = "https://api.webinarjam.com/everwebinar/registrants"

# Check if all environment variables are set
if not all([EVERWEBINAR_API_KEY, EVERWEBINAR_ID, EVERWEBINAR_SCHEDULE_ID]):
    raise RuntimeError("One or more required environment variables are missing. Please ensure EVERWEBINAR_API_KEY, EVERWEBINAR_ID, and EVERWEBINAR_SCHEDULE_ID are set.")

def process_registrants():
    """
    Process registrants with attended_live=0 and display their segmentation in the terminal.
    """
    print("Processing EverWebinar registrants...")
    page = 1
    total_registrants = 0

    while True:
        # Prepare the payload for the current page
        payload = {
            "api_key": EVERWEBINAR_API_KEY,
            "webinar_id": EVERWEBINAR_ID,
            "schedule": EVERWEBINAR_SCHEDULE_ID,
            "date_range": 0,
            "attended_live": 0,
            "page": page
        }

        # Send the POST request to EverWebinar API
        response = requests.post(registrants_url, data=payload)

        if response.status_code == 200:
            response_json = response.json()

            # Check if the API's custom status field indicates success
            if response_json.get("status") == "success":
                registrants_data = response_json.get("registrants", {})
                current_page = registrants_data.get("current_page", 0)
                data = registrants_data.get("data", [])

                # Process each registrant
                for registrant in data:
                    tag = determine_tag(registrant)
                    hot_lead = determine_hot_lead(registrant)
                    display_to_terminal(registrant, tag, hot_lead)
                    total_registrants += 1

                # Check if there are more pages
                if len(data) == 0 or current_page == 0:
                    break  # No more data or invalid page

                page += 1  # Move to the next page
            else:
                raise RuntimeError(f"EverWebinar API error: {response_json.get('error', 'Unknown error')}")
        else:
            raise RuntimeError(f"HTTP error: {response.status_code}, {response.text}")

    print(f"Total registrants processed: {total_registrants}")

def determine_tag(registrant):
    """
    Determine the tag for the registrant based on their attendance and engagement.
    """
    attended_live = registrant.get("attended_live", "").lower()
    time_live = registrant.get("time_live", "00:00:00")

    if attended_live == "yes":
        # Convert time_live to seconds for comparison
        hours, minutes, seconds = map(int, time_live.split(":"))
        total_seconds = hours * 3600 + minutes * 60 + seconds

        if total_seconds >= 5400:  # 1h 30min in seconds
            return "high engagement"
        else:
            return "low engagement"
    else:
        return "no-show"

def determine_hot_lead(registrant):
    """
    Determine if the registrant is a hot lead based on their time spent in the live room.
    """
    time_live = registrant.get("time_live", "00:00:00")
    # Convert time_live to seconds for comparison
    hours, minutes, seconds = map(int, time_live.split(":"))
    total_seconds = hours * 3600 + minutes * 60 + seconds

    return 1 if total_seconds >= 7200 else 0  # 2 hours in seconds

def display_to_terminal(registrant, tag, hot_lead):
    """
    Display registrant data and segmentation in the terminal.
    """
    print(f"Registrant: {registrant.get('first_name')} {registrant.get('last_name')} ({registrant.get('email')})")
    print(f"  Tag: {tag}")
    print(f"  Hot Lead: {'Yes' if hot_lead else 'No'}")
    print(f"  Time Live: {registrant.get('time_live')}")
    print("-" * 40)

# Run the function to process registrants
process_registrants()