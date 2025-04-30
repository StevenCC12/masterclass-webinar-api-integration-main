import os
import requests

# Load environment variables from .env only in local development
if os.getenv("RENDER") is None:  # Render sets this variable automatically
    from dotenv import load_dotenv
    load_dotenv()

WEBINARJAM_API_KEY = os.getenv("WEBINARJAM_API_KEY")
webinars_url = "https://api.webinarjam.com/webinarjam/webinars"
details_url = "https://api.webinarjam.com/webinarjam/webinar"

# Prepare the payload
payload = {"api_key": WEBINARJAM_API_KEY}

# Make the POST request to fetch webinars
response = requests.post(webinars_url, data=payload)

# Check the response
if response.status_code == 200:
    data = response.json()
    if data.get("status") == "success":
        webinars = data.get("webinars", [])
        for webinar in webinars:
            webinar_id = webinar.get("webinar_id")
            webinar_title = webinar.get("title")
            
            # Fetch additional details from the webinar detail endpoint
            details_payload = {"api_key": WEBINARJAM_API_KEY, "webinar_id": webinar_id}
            details_response = requests.post(details_url, data=details_payload)
            
            if details_response.status_code == 200:
                details_data = details_response.json()
                if details_data.get("status") == "success":
                    webinar_details = details_data.get("webinar", {})
                    
                    # Print webinar details
                    print(f"Webinar Title: {webinar_title}")
                    print(f"Webinar ID: {webinar_id}")
                    print(f"Webinar Hash: {webinar_details.get('webinar_hash')}")
                    
                    # Iterate over the schedules array to extract schedule IDs
                    schedules = webinar_details.get("schedules", [])
                    for schedule in schedules:
                        print(f"Schedule ID: {schedule.get('schedule')}")
                        print(f"Date: {schedule.get('date')}")
                        print(f"Comment: {schedule.get('comment')}")
                    print("-" * 50)
                else:
                    print(f"Details API response status is not 'success' for webinar '{webinar_title}':", details_data)
            else:
                print(f"Error fetching details for webinar '{webinar_title}':", details_response.status_code, details_response.text)
    else:
        print("API response status is not 'success':", data)
else:
    print("Error:", response.status_code, response.text)