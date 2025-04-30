import csv

# List of personal email domains
personal_email_domains = [
    "gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "live.com", "icloud.com",
    "aol.com", "protonmail.com", "yandex.com", "zoho.com", "mail.com", "me.com",
    "hotmail.co.uk", "live.se", "hotmail.se", "yahoo.se"
]

# Input and output file paths
input_csv = "all_contacts.csv"  # Replace with your input CSV file path
output_csv = "non_personal_emails.csv"  # Output file for non-personal emails

def is_personal_email(email):
    """
    Check if the email belongs to a personal email domain.
    """
    domain = email.split("@")[-1].lower()  # Extract the domain from the email
    return domain in personal_email_domains

def filter_non_personal_emails(input_csv, output_csv):
    """
    Read the input CSV, filter out non-personal emails, and write them to a new CSV.
    """
    with open(input_csv, mode="r", encoding="utf-8") as infile, open(output_csv, mode="w", encoding="utf-8", newline="") as outfile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames  # Preserve the original column headers
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        
        writer.writeheader()  # Write the header to the output file
        
        for row in reader:
            email = row.get("Email", "").strip()  # Get the email from the row
            if email and not is_personal_email(email):
                # Filter the row to include only the expected fields
                filtered_row = {key: row[key] for key in fieldnames if key in row}
                writer.writerow(filtered_row)  # Write the row to the output file if it's non-personal

# Run the filtering function
filter_non_personal_emails(input_csv, output_csv)

print(f"Non-personal emails have been exported to {output_csv}.")