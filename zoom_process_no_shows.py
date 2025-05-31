# process_no_shows.py
import os

# Import shared utilities and specific variables/functions
from zoom_utils import (
    logger, 
    make_zoom_api_request, 
    send_to_ghl, # determine_tag and determine_hot_lead not strictly needed if always "no-show"
    ZOOM_API_TOKEN as Z_API_TOKEN,
    GHL_WEBHOOK_URL as GHL_URL
)

def process_absentees(webinar_id, occurrence_id=None, current_zoom_token=None, current_ghl_url=None):
    """
    Fetches webinar absentees and sends them to GHL with a "no-show" tag.
    """
    logger.info(f"NO-SHOWS SCRIPT: Processing for Zoom webinar ID: {webinar_id}" + (f", Occurrence ID: {occurrence_id}" if occurrence_id else ""))

    # --- Fetch No-Shows (Absentees) ---
    absentees_endpoint = f"/webinars/{webinar_id}/absentees"
    absentee_params = {} # page_size is handled by make_zoom_api_request
    if occurrence_id:
        absentee_params["occurrence_id"] = occurrence_id # Absentees endpoint uses this
    
    absentees_list = make_zoom_api_request(absentees_endpoint, absentee_params, api_token_override=current_zoom_token)

    if absentees_list is None:
        logger.error("NO-SHOWS SCRIPT: Failed to fetch absentees. Processing cannot continue.")
        return

    logger.info(f"NO-SHOWS SCRIPT: Fetched {len(absentees_list)} absentee records.")
    absentees_sent_count = 0
    for absentee in absentees_list:
        # Absentees records are usually full registrant objects
        absentee_registrant_id = absentee.get("id") # 'id' in absentee object is the registrant_id

        if not absentee_registrant_id:
            logger.warning(f"NO-SHOWS SCRIPT: Skipping absentee with no registrant ID: {absentee.get('email')}")
            continue

        registrant_mock = {
            "first_name": absentee.get("first_name"),
            "last_name": absentee.get("last_name"),
            "email": absentee.get("email"),
            "phone_number": absentee.get("phone"), # Phone number might be available here
            "attended_live": "no",
            "time_live": "00:00:00",
            "zoom_id": absentee_registrant_id, 
        }
        
        tag = "no-show" 
        hot_lead_status = 0 # No-shows are not hot leads in this context

        logger.info(f"NO-SHOWS SCRIPT: Processing {absentee.get('email')} (ID: {absentee_registrant_id}), Phone: {absentee.get('phone')}. Sending as no-show.")
        send_to_ghl(
            registrant_mock,
            tag,
            0, # Purchased default
            hot_lead_status,
            webinar_id,
            occurrence_id,
            ghl_url_override=current_ghl_url
        )
        absentees_sent_count += 1
        
    logger.info(f"NO-SHOWS SCRIPT: Finished. Sent {absentees_sent_count} no-shows to GHL.")

if __name__ == "__main__":
    logger.info("--- No-Shows Processing Script Started ---")

    WEBINAR_ID_TO_PROCESS = os.getenv("ZOOM_WEBINAR_ID")
    OCCURRENCE_ID_TO_PROCESS = os.getenv("ZOOM_OCCURRENCE_ID") # Optional

    if not Z_API_TOKEN:
        logger.critical("ZOOM_API_TOKEN environment variable is missing.")
    elif not WEBINAR_ID_TO_PROCESS:
        logger.critical("ZOOM_WEBINAR_ID environment variable is missing.")
    elif not GHL_URL:
        logger.critical("GHL_WEBHOOK_URL_PYTHON_LIVE_PROCESS environment variable is missing.")
    else:
        process_absentees(
            WEBINAR_ID_TO_PROCESS, 
            OCCURRENCE_ID_TO_PROCESS,
            current_zoom_token=Z_API_TOKEN,
            current_ghl_url=GHL_URL
        )
    logger.info("--- No-Shows Processing Script Finished ---")