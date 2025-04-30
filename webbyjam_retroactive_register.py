import csv
import logging
import requests
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log format
    handlers=[
        logging.FileHandler("retroactive_register.log"),  # Log to a file
        logging.StreamHandler()  # Log to the console
    ]
)

# URL for the GHL Webhook
ghl_webhook_url = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/da1ef1c5-fd2d-4772-9745-8a0c5c14ee39"

# Function to send a POST request for each registration
def send_to_ghl_webhook(name, email, phone):
    payload = {
        "name": name,
        "email": email,
        "phone": phone
    }
    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(ghl_webhook_url, json=payload, headers=headers)
        if response.status_code == 200:
            logging.info(f"Successfully sent to GHL Webhook: {name} ({email})")
        else:
            logging.error(f"Failed to send to GHL Webhook: {name} ({email}). Status code: {response.status_code}. Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending to GHL Webhook {name} ({email}): {str(e)}")

# Main function to process the CSV file
def process_csv(file_path):
    with open(file_path, mode="r", encoding="utf-8") as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            name = row["name"]
            email = row["email"]
            phone = row["phone"]

            # Log the registration attempt
            logging.info(f"Processing registration for: {name} ({email})")

            # Send the data to the GHL Webhook
            send_to_ghl_webhook(name, email, phone)

            # Delay between requests
            time.sleep(3)

# Run the script
if __name__ == "__main__":
    csv_file_path = "registrations.csv"  # Replace with the path to your CSV file
    process_csv(csv_file_path)