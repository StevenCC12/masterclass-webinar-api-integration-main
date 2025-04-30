import csv
import requests
import time  # For adding delay between requests

# Input and output file paths
input_csv = "amazon_gung.csv"  # Replace with your input CSV file path
output_csv = "processed_amazon_gung.csv"  # Output file for preprocessed data

# GHL Webhook URL
ghl_webhook_url = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/93b43726-cb01-4ef3-a234-e317b92c2727"

def preprocess_csv(input_csv, output_csv):
    """
    Read the input CSV, preprocess the data, and write it to a new CSV.
    """
    with open(input_csv, mode="r", encoding="utf-8") as infile, open(output_csv, mode="w", encoding="utf-8", newline="") as outfile:
        reader = csv.DictReader(infile)
        # Define the expected fieldnames explicitly
        fieldnames = ["Email", "Name", "Attended", "On Amazon", "Phone Number if exists"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        writer.writeheader()  # Write the header to the output file
        
        for row in reader:
            # Proper case for the "Name" column
            if row.get("Name"):
                row["Name"] = row["Name"].title()
            
            # Convert "Attended" to 1 or 0
            if not row.get("Attended") or row["Attended"].strip().lower() == "no":
                row["Attended"] = 0
            elif row["Attended"].strip().lower() == "yes":
                row["Attended"] = 1
            
            # Convert "On Amazon" to 1 or 0
            if not row.get("On Amazon") or row["On Amazon"].strip().lower() == "no":
                row["On Amazon"] = 0
            elif row["On Amazon"].strip().lower() == "yes":
                row["On Amazon"] = 1
            
            # Handle missing phone numbers (leave blank or keep as is)
            if not row.get("Phone Number if exists"):
                row["Phone Number if exists"] = ""  # Leave blank
            
            # Remove the "Registered At" column from the row (if it exists)
            if "Registered At" in row:
                del row["Registered At"]
            
            # Write the preprocessed row to the output file
            writer.writerow(row)

def split_name(full_name):
    """
    Split a full name into first name and last name.
    """
    name_parts = full_name.split()
    first_name = name_parts[0] if len(name_parts) > 0 else ""
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    return first_name, last_name

def send_to_ghl_webhook(output_csv, webhook_url):
    """
    Read the processed CSV and send each row to the GHL Webhook.
    """
    with open(output_csv, mode="r", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            # Split the full name into first name and last name
            full_name = row.get("Name", "")
            first_name, last_name = split_name(full_name)
            
            # Prepare the payload for the webhook
            payload = {
                "email": row.get("Email", ""),
                "full_name": full_name,  # Include the full name in proper case
                "first_name": first_name,
                "last_name": last_name,
                "attended": row.get("Attended", ""),
                "on_amazon": row.get("On Amazon", ""),
                "phone": row.get("Phone Number if exists", "")
            }
            
            # Send the POST request to the webhook
            response = requests.post(webhook_url, json=payload)
            
            if response.status_code == 200:
                print(f"Successfully sent data for {row.get('Email')} to GHL.")
            else:
                print(f"Failed to send data for {row.get('Email')}. HTTP {response.status_code}: {response.text}")
            
            # Add a 1-second delay between requests
            time.sleep(1)

# Run the preprocessing function
preprocess_csv(input_csv, output_csv)

# Send the processed data to the GHL Webhook
send_to_ghl_webhook(output_csv, ghl_webhook_url)

print(f"Processed data has been sent to the GHL Webhook.")