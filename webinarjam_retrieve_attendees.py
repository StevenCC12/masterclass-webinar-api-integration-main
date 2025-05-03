import os
import requests
import time

# Load environment variables from .env only in local development
if os.getenv("RENDER") is None:  # Render sets this variable automatically
    from dotenv import load_dotenv
    load_dotenv()

WEBINARJAM_API_KEY = os.getenv("WEBINARJAM_API_KEY")
WEBINAR_ID = os.getenv("WEBINARJAM_WEBINAR_ID")
WEBINAR_SCHEDULE_ID = os.getenv("WEBINARJAM_WEBINAR_SCHEDULE_ID")
registrants_url = "https://api.webinarjam.com/webinarjam/registrants"
ghl_webhook_url = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/VCs8Z5BuTAef953g81Pl"

# Check if all environment variables are set
if not all([WEBINARJAM_API_KEY, WEBINAR_ID, WEBINAR_SCHEDULE_ID]):
    raise RuntimeError("One or more required environment variables are missing. Please ensure WEBINARJAM_API_KEY, WEBINARJAM_WEBINAR_ID, and WEBINARJAM_WEBINAR_SCHEDULE_ID are set.")

def process_registrants():
    """
    Process registrants with attended_live=0 and send them to GHL with appropriate tags, purchased status, and hot lead status.
    """
    print("Processing registrants with attended_live=0...")
    page = 1
    total_registrants = 0

    while True:
        # Prepare the payload for the current page
        payload = {
            "api_key": WEBINARJAM_API_KEY,
            "webinar_id": WEBINAR_ID,
            "schedule": WEBINAR_SCHEDULE_ID,
            "date_range": 0,
            "attended_live": 0,
            "page": page
        }

        # Send the POST request to WebinarJam API
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
                    purchased = 1 if registrant.get("purchased_live", "").lower() == "yes" else 0
                    hot_lead = determine_hot_lead(registrant)
                    send_to_ghl(registrant, tag, purchased, hot_lead)
                    total_registrants += 1

                # Check if there are more pages
                if len(data) == 0 or current_page == 0:
                    break  # No more data or invalid page

                page += 1  # Move to the next page
            else:
                raise RuntimeError(f"WebinarJam API error: {response_json.get('error', 'Unknown error')}")
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

def send_to_ghl(registrant, tag, purchased, hot_lead):
    """
    Send registrant data to GHL via inbound webhook with the appropriate tag, purchased status, and hot lead status.
    """
    payload = {
        "webinar_id": WEBINAR_ID,
        "first_name": registrant.get("first_name"),
        "last_name": registrant.get("last_name"),
        "email": registrant.get("email"),
        "phone": registrant.get("phone_number"),
        "tag": tag,  # Add the appropriate tag for the registrant
        "purchased": purchased,  # Add the purchased status (1 for purchased, 0 for not purchased)
        "hot_lead": hot_lead,  # Add the hot lead status (1 for hot lead, 0 otherwise)
        "time_live": registrant.get("time_live")  # Add the time_live value
    }

    response = requests.post(ghl_webhook_url, json=payload)

    if response.status_code == 200:
        print(f"Successfully sent registrant {registrant.get('email')} to GHL with tag '{tag}', purchased={purchased}, hot_lead={hot_lead}, and time_live={registrant.get('time_live')}.")
    else:
        print(f"Failed to send registrant {registrant.get('email')} to GHL. HTTP {response.status_code}: {response.text}")

    # Add a 2-second delay between requests
    time.sleep(2)

# Run the function to process registrants
process_registrants()