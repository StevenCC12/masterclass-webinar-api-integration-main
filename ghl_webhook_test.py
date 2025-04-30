import requests
import logging
from datetime import datetime, timezone
import time

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# GHL Webhook URL
WEBHOOK_URL = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/35168aef-8200-4f8c-be75-ad53d38910e3"
WEBHOOK_URL_ENDTIME = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/272ae1b7-1dd2-4b6b-8b1e-41c382429ed9"

processed_participants = [
    {'name': 'Carl Helgesson', 'user_email': 'carl@rankonamazon.com', 'webinar_date': '2025-03-20', 'webinar_end_time': '2025-03-20 10:15 PM', 'duration': 9998},
    {'name': 'Test Testsson - low engagement', 'user_email': 'testlowengagement@example.com', 'webinar_date': '2025-03-20', 'duration': 3599},
    {'name': 'Test Testsson - high engagement', 'user_email': 'testhighengagement@example.com', 'webinar_date': '2025-03-20', 'duration': 3600},
    # {'name': 'Peter Adehill', 'user_email': 'test2@example.com', 'webinar_date': '2025-03-20', 'duration': 4340},
    # {'name': 'Marlen Garamy', 'user_email': 'test3@example.com', 'webinar_date': '2025-03-20', 'duration': 5970},
    # {'name': 'Mariana', 'user_email': 'test4@example.com', 'webinar_date': '2025-03-20', 'duration': 7867},
    # {'name': 'Frida Wingman', 'user_email': 'test5@example.com', 'webinar_date': '2025-03-20', 'duration': 8032},
    # {'name': 'Sejla B', 'user_email': 'test6@example.com', 'webinar_date': '2025-03-20', 'duration': 7043},
    # {'name': 'Maria S', 'user_email': 'test7@example.com', 'webinar_date': '2025-03-20', 'duration': 5315},
    # {'name': 'Tessan Levy', 'user_email': 'test8@example.com', 'webinar_date': '2025-03-20', 'duration': 6874},
    # {'name': 'Johanna S', 'user_email': 'test9@example.com', 'webinar_date': '2025-03-20', 'duration': 8145},
    # {'name': 'Olle,B', 'user_email': 'test10@example.com', 'webinar_date': '2025-03-20', 'duration': 9992},
    # {'name': 'Laila', 'user_email': 'test11@example.com', 'webinar_date': '2025-03-20', 'duration': 6217},
    # {'name': 'Emma', 'user_email': 'test12@example.com', 'webinar_date': '2025-03-20', 'duration': 9990},
    # {'name': 'vilje', 'user_email': 'test13@example.com', 'webinar_date': '2025-03-20', 'duration': 9488},
    # {'name': 'Ulla', 'user_email': 'test14@example.com', 'webinar_date': '2025-03-20', 'duration': 3735},
    # {'name': 'Håkan Sundmark', 'user_email': 'test15@example.com', 'webinar_date': '2025-03-20', 'duration': 7983},
    # {'name': 'Dag Adler', 'user_email': 'test16@example.com', 'webinar_date': '2025-03-20', 'duration': 9897},
    # {'name': 'Rannveig', 'user_email': 'test17@example.com', 'webinar_date': '2025-03-20', 'duration': 9987},
    # {'name': 'Annika', 'user_email': 'test18@example.com', 'webinar_date': '2025-03-20', 'duration': 9987},
    # {'name': 'iPhone', 'user_email': 'test19@example.com', 'webinar_date': '2025-03-20', 'duration': 2247},
    # {'name': 'Eleonor – iPhone', 'user_email': 'test20@example.com', 'webinar_date': '2025-03-20', 'duration': 7801},
    # {'name': 'Eva z', 'user_email': 'test21@example.com', 'webinar_date': '2025-03-20', 'duration': 2799},
    # {'name': 'Danny Nadler', 'user_email': 'test22@example.com', 'webinar_date': '2025-03-20', 'duration': 9124},
    # {'name': 'Edita Karlsson iPhone', 'user_email': 'test23@example.com', 'webinar_date': '2025-03-20', 'duration': 1413},
    # {'name': 'Caroline Holmgren', 'user_email': 'test24@example.com', 'webinar_date': '2025-03-20', 'duration': 9950},
    # {'name': 'tinatronestam', 'user_email': 'test25@example.com', 'webinar_date': '2025-03-20', 'duration': 9944},
    # {'name': 'Johan Thunqvist', 'user_email': 'test26@example.com', 'webinar_date': '2025-03-20', 'duration': 7870},
    # {'name': 'Sara Amberin Danielsson', 'user_email': 'test27@example.com', 'webinar_date': '2025-03-20', 'duration': 7325},
    # {'name': 'Mli', 'user_email': 'test28@example.com', 'webinar_date': '2025-03-20', 'duration': 9777},
    # {'name': 'iPhone', 'user_email': 'test29@example.com', 'webinar_date': '2025-03-20', 'duration': 974},
    # {'name': 'Caroline Park', 'user_email': 'test30@example.com', 'webinar_date': '2025-03-20', 'duration': 5825},
    # {'name': 'Johan Hallberg', 'user_email': 'test31@example.com', 'webinar_date': '2025-03-20', 'duration': 15},
    # {'name': 'Hassan Alarady', 'user_email': 'test32@example.com', 'webinar_date': '2025-03-20', 'duration': 894},
    # {'name': 'Marie', 'user_email': 'test34@example.com', 'webinar_date': '2025-03-20', 'duration': 9016},
    # {'name': 'Alyonas iPhone', 'user_email': 'test35@example.com', 'webinar_date': '2025-03-20', 'duration': 216},
    # {'name': 'Emira Zelenjakovic', 'user_email': 'test36@example.com', 'webinar_date': '2025-03-20', 'duration': 8061},
    # {'name': 'Hilma Gustafsson', 'user_email': 'test37@example.com', 'webinar_date': '2025-03-20', 'duration': 5291},
    # {'name': 'srisailam marupakula', 'user_email': 'test38@example.com', 'webinar_date': '2025-03-20', 'duration': 9732},
    # {'name': 'Emma', 'user_email': 'test39@example.com', 'webinar_date': '2025-03-20', 'duration': 9341},
    # {'name': 'Ilyas M Hussein', 'user_email': 'test40@example.com', 'webinar_date': '2025-03-20', 'duration': 2335}
]

def send_to_ghl(participant, webhook_url):
    """Send individual participant data to GHL webhook"""
    try:
        # Format name fields
        name_parts = participant['name'].split()
        first_name = name_parts[0]
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        # Extract webinar_date and optionally webinar_end_time
        webinar_date = participant['webinar_date']  # Already in 'YYYY-MM-DD'
        webinar_end_time = participant.get('webinar_end_time')  # Only present for Carl Helgesson

        # Prepare payload
        payload = {
            "email": participant['user_email'],
            "first_name": first_name,
            "last_name": last_name,
            "duration": participant['duration'],
            "_update": True,
            "custom_fields": {
                "last_webinar_attended": "Amazon Masterclass",
                "last_webinar_attended_date": webinar_date,  # Keep as 'YYYY-MM-DD'
                "webinar_attendance_count": "INCREMENT"
            }
        }

        # Conditionally add webinar_end_time if it exists
        if webinar_end_time:
            payload["webinar_end_time"] = webinar_end_time  # Keep as 'YYYY-MM-DD h:mm AM/PM'

        # Optional custom User-Agent
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "ZoomWebinarIntegration/1.0"
        }

        logger.info(f"Sending data for {participant['name']} to GHL")
        logger.info(f"Payload: {payload}")

        response = requests.post(
            webhook_url,
            headers=headers,
            json=payload,
            timeout=10
        )

        response.raise_for_status()
        logger.info(f"Successfully sent data for {participant['name']}. Status: {response.status_code}")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"Webhook failed for {participant['name']}: {str(e)}")
        return False

def main():
    logger.info("Starting GHL webhook test")
    success_count = 0

    # Separate Carl Helgesson from other participants
    if processed_participants and processed_participants[0]['name'] == 'Carl Helgesson':
        carl = processed_participants[0]
        other_participants = processed_participants[1:]
        
        # Send Carl to the first specific webhook
        logger.info(f"Sending Carl Helgesson to special webhook")
        if send_to_ghl(carl, WEBHOOK_URL_ENDTIME):
            success_count += 1
        
        # Short delay before processing other participants
        time.sleep(1.5)
        
        # Send other participants to the standard webhook
        for idx, participant in enumerate(other_participants, 1):
            logger.info(f"Sending participant {participant['name']} to standard webhook")
            if send_to_ghl(participant, WEBHOOK_URL):
                success_count += 1
            
            # Add a delay between requests to avoid rate limiting
            if idx < len(other_participants):
                time.sleep(1.5)
    
    else:
        # Fallback if no participants or Carl is not first
        logger.warning("No participants or Carl Helgesson not found as first entry")
        for idx, participant in enumerate(processed_participants, 1):
            if send_to_ghl(participant, WEBHOOK_URL):
                success_count += 1
            
            if idx < len(processed_participants):
                time.sleep(1.5)
    
    logger.info(f"Test completed. Successfully processed {success_count}/{len(processed_participants)} participants")

if __name__ == "__main__":
    main()