# process_hot_leads.py
import os
import time # For time.strftime

# Import shared utilities and specific variables/functions
from zoom_utils import (
    logger, 
    make_zoom_api_request, 
    determine_tag, 
    determine_hot_lead, 
    send_to_ghl,
    ZOOM_API_TOKEN as Z_API_TOKEN, # Use an alias if preferred or ensure it's loaded
    GHL_WEBHOOK_URL as GHL_URL   # Ensure it's loaded
)

def process_high_engagement_attendees(webinar_id, occurrence_id=None, current_zoom_token=None, current_ghl_url=None):
    """
    Fetches webinar attendees, processes them, and sends only "high engagement"
    (which includes "hot leads") to GHL.
    """
    logger.info(f"HOT LEADS SCRIPT: Processing for Zoom webinar ID: {webinar_id}" + (f", Occurrence ID: {occurrence_id}" if occurrence_id else ""))

    # --- Fetch Attendees (Participants) ---
    participants_endpoint = f"/webinars/{webinar_id}/participants"
    # Request registrant_id to uniquely identify participants across sessions/rejoins
    participant_params = {"include_fields": "registrant_id"} 
    # Note: occurrence_id might not be directly used by /participants if webinarId is a UUID.
    # If webinar_id is a simple numeric ID, this endpoint usually gets the latest session.
    # For specific past instances, a UUID webinarId or a different report endpoint might be needed
    # if this doesn't take occurrence_id. For now, assuming webinar_id targets the correct one.

    all_participant_sessions = make_zoom_api_request(participants_endpoint, participant_params, api_token_override=current_zoom_token)
    
    if all_participant_sessions is None:
        logger.error("HOT LEADS SCRIPT: Failed to fetch attendees. Processing cannot continue.")
        return
    
    logger.info(f"HOT LEADS SCRIPT: Fetched {len(all_participant_sessions)} participant session entries.")
    
    aggregated_attendees = {}
    for session in all_participant_sessions:
        person_key = session.get("registrant_id")
        email_for_logging = session.get("user_email", "N/A")

        if not person_key or person_key.strip() == "":
            # Fallback to user_email only if registrant_id is truly absent or empty
            # This is less reliable for unique identification if user_email can be non-unique or empty
            person_key = session.get("user_email")
            if person_key and person_key.strip() != "":
                 logger.warning(f"HOT LEADS SCRIPT: Using user_email '{person_key}' as key for session ID {session.get('id', 'N/A')} due to missing/empty registrant_id.")
            else:
                logger.warning(f"HOT LEADS SCRIPT: Skipping participant session (ID: {session.get('id', 'N/A')}, Name: {session.get('name', 'N/A')}) due to missing key (registrant_id and user_email).")
                continue
        
        current_duration_seconds = session.get("duration", 0)

        if person_key not in aggregated_attendees:
            name_from_session = session.get("name", "")
            first_name = name_from_session.split(" ")[0] if name_from_session else ""
            last_name = " ".join(name_from_session.split(" ")[1:]) if " " in name_from_session else ""
            
            aggregated_attendees[person_key] = {
                "first_name": first_name,
                "last_name": last_name,
                "email": session.get("user_email", ""), 
                "registrant_id_used_as_key": person_key, # Store the key actually used
                "total_duration_seconds": current_duration_seconds,
                "attended_live": "yes", # All in this list attended
            }
        else:
            aggregated_attendees[person_key]["total_duration_seconds"] += current_duration_seconds
    
    logger.info(f"HOT LEADS SCRIPT: Aggregated into {len(aggregated_attendees)} unique attendees.")
    attendees_sent_count = 0
    for key_id, attendee_data in aggregated_attendees.items():
        time_live_seconds = attendee_data["total_duration_seconds"]
        time_live_str = time.strftime('%H:%M:%S', time.gmtime(time_live_seconds))

        registrant_mock = {
            "first_name": attendee_data["first_name"],
            "last_name": attendee_data["last_name"],
            "email": attendee_data["email"],
            "phone_number": None, # Not available from participants endpoint, not essential for this script
            "attended_live": "yes",
            "time_live": time_live_str,
            "zoom_id": attendee_data["registrant_id_used_as_key"], 
        }

        tag = determine_tag(registrant_mock)
        hot_lead = determine_hot_lead(registrant_mock)
        
        if tag == "high engagement": # This will include "hot leads"
            logger.info(f"HOT LEADS SCRIPT: QUALIFIED: {attendee_data['email']} (ID: {key_id}), Duration: {time_live_str}, Tag: {tag}, Hot Lead: {hot_lead}. Sending.")
            send_to_ghl(
                registrant_mock, 
                tag, 
                0, # Purchased default
                hot_lead,
                webinar_id, # Pass the main webinar_id
                occurrence_id, # Pass the occurrence_id (can be None)
                ghl_url_override=current_ghl_url
            )
            attendees_sent_count += 1
        # else:
            # logger.info(f"HOT LEADS SCRIPT: SKIPPED (not high engagement): {attendee_data['email']} (ID: {key_id}), Tag: {tag}.")

    logger.info(f"HOT LEADS SCRIPT: Finished. Sent {attendees_sent_count} high-engagement attendees to GHL.")

if __name__ == "__main__":
    logger.info("--- Hot Leads Processing Script Started ---")
    
    # Load required env vars for this script specifically
    WEBINAR_ID_TO_PROCESS = os.getenv("ZOOM_WEBINAR_ID")
    OCCURRENCE_ID_TO_PROCESS = os.getenv("ZOOM_OCCURRENCE_ID") # Optional, can be None

    # Check critical environment variables loaded by zoom_utils or directly
    if not Z_API_TOKEN:
        logger.critical("ZOOM_API_TOKEN environment variable is missing.")
    elif not WEBINAR_ID_TO_PROCESS:
        logger.critical("ZOOM_WEBINAR_ID environment variable is missing (for webinar_id_to_process).")
    elif not GHL_URL:
        logger.critical("GHL_WEBHOOK_URL_PYTHON_LIVE_PROCESS environment variable is missing.")
    else:
        process_high_engagement_attendees(
            WEBINAR_ID_TO_PROCESS, 
            OCCURRENCE_ID_TO_PROCESS,
            current_zoom_token=Z_API_TOKEN,
            current_ghl_url=GHL_URL
        )
    logger.info("--- Hot Leads Processing Script Finished ---")