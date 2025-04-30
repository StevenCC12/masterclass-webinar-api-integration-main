import os
import requests

# Load environment variables from .env only in local development
if os.getenv("RENDER") is None:  # Render sets this variable automatically
    from dotenv import load_dotenv
    load_dotenv()

WEBINARJAM_API_KEY = os.getenv("WEBINARJAM_API_KEY")
WEBINAR_ID = os.getenv("WEBINARJAM_WEBINAR_ID")
WEBINAR_SCHEDULE_ID = os.getenv("WEBINARJAM_WEBINAR_SCHEDULE_ID")
registrants_url = "https://api.webinarjam.com/webinarjam/registrants"

# Check if all environment variables are set
if not all([WEBINARJAM_API_KEY, WEBINAR_ID, WEBINAR_SCHEDULE_ID]):
    raise RuntimeError("One or more required environment variables are missing. Please ensure WEBINARJAM_API_KEY, WEBINARJAM_WEBINAR_ID, and WEBINARJAM_WEBINAR_SCHEDULE_ID are set.")

def count_registrants():
    """
    Count the total number of registrants with attended_live=0.
    """
    print("Counting registrants with attended_live=0...")
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

                # Count the registrants on this page
                total_registrants += len(data)

                # Check if there are more pages
                if len(data) == 0 or current_page == 0:
                    break  # No more data or invalid page

                page += 1  # Move to the next page
            else:
                raise RuntimeError(f"WebinarJam API error: {response_json.get('error', 'Unknown error')}")
        else:
            raise RuntimeError(f"HTTP error: {response.status_code}, {response.text}")

    print(f"Total registrants counted: {total_registrants}")

# Run the function to count registrants
count_registrants()