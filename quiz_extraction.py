import csv
import requests
import json
import time
from urllib.parse import urlparse, parse_qs

# --- Configuration ---
csv_file = "quiz_data.csv"  # Path to your CSV file
webhook_url = "https://services.leadconnectorhq.com/hooks/kFKnF888dp7eKChjLxb9/webhook-trigger/829ad151-a671-4167-a712-3fd319cfbc82"
REQUEST_DELAY_SECONDS = 2 # Delay between GHL requests
ROWS_TO_PROCESS = 3 # Process only the first 2 data rows

# --- Helper Functions ---
def extract_cid_from_url(url):
    """ Extracts 'cid' parameter from URL, returns None if not found or on error. """
    if not url: return None
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        return query_params.get("cid", [None])[0]
    except Exception:
        # Optionally log the error here if needed: print(f"Error parsing URL '{url}': {e}")
        return None

# --- Main Processing Function ---
def process_first_n_rows_and_send(file_path, webhook_url, n):
    """
    Process the first 'n' data rows of the CSV file, and for qualifying rows,
    send data to the specified webhook with a delay.
    """
    print(f"Processing the first {n} data rows from {file_path}...")
    rows_considered = 0
    rows_sent = 0
    rows_skipped_no_cid = 0
    rows_skipped_no_score = 0
    rows_skipped_both_missing = 0

    try:
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=",")
            for row_number, row in enumerate(reader, 1):
                rows_considered += 1

                # Stop processing after reaching the desired number of rows
                if rows_considered > n:
                    print(f"\nFinished considering the first {n} rows.")
                    break

                print(f"\nProcessing Row {row_number} (Considered: {rows_considered}/{n})...")
                url = row.get("URL")
                # Get score, remove leading/trailing whitespace, remove trailing '%'
                overall_score_str = row.get("Overall Only", "").strip()
                if overall_score_str.endswith('%'):
                   overall_score_str = overall_score_str[:-1].strip()

                cid = extract_cid_from_url(url)

                # Check if row qualifies (has both cid and a non-empty score string)
                if cid and overall_score_str:
                    payload = {
                        "cid": cid,
                        "total_result": overall_score_str # Send cleaned score string
                    }
                    # Attempt to send data
                    try:
                        print(f"Row {row_number}: Preparing to send data for cid: {cid}...")
                        response = requests.post(webhook_url, json=payload)
                        response.raise_for_status()
                        print(f"Row {row_number}: Data sent successfully. Status: {response.status_code}")
                        rows_sent += 1
                    except requests.exceptions.HTTPError as http_err:
                        print(f"Row {row_number}: HTTP error sending data: {http_err}. Response: {response.text}")
                    except requests.exceptions.RequestException as req_err:
                        print(f"Row {row_number}: Request error sending data: {req_err}")
                    except Exception as e:
                        print(f"Row {row_number}: Unexpected error sending data: {e}")
                    finally:
                        # Delay after every attempt to send qualifying row
                        print(f"Waiting for {REQUEST_DELAY_SECONDS} seconds...")
                        time.sleep(REQUEST_DELAY_SECONDS)
                else:
                    # Log skipped rows
                    if not cid and not overall_score_str:
                        print(f"Row {row_number}: Skipped. Missing both cid and score.")
                        rows_skipped_both_missing +=1
                    elif not cid:
                        print(f"Row {row_number}: Skipped. Missing cid. Score: '{overall_score_str}'")
                        rows_skipped_no_cid += 1
                    else: # not overall_score_str (empty or only '%')
                        print(f"Row {row_number}: Skipped. Missing score. cid: '{cid}'")
                        rows_skipped_no_score +=1

        # --- Summary ---
        print("\n--- Processing Summary ---")
        # Adjust count if loop broke early
        final_rows_considered = rows_considered - 1 if rows_considered > n else rows_considered
        print(f"Rows considered: {final_rows_considered} (out of first {n})")
        print(f"Rows sent to webhook: {rows_sent}")
        print(f"Rows skipped (missing cid): {rows_skipped_no_cid}")
        print(f"Rows skipped (missing score): {rows_skipped_no_score}")
        print(f"Rows skipped (missing both): {rows_skipped_both_missing}")

    except FileNotFoundError:
        print(f"ERROR: File not found at path: {file_path}")
    except Exception as e:
        print(f"An unexpected error occurred during CSV processing: {e}")
        import traceback
        traceback.print_exc()

# --- Run the Script ---
if __name__ == "__main__":
    process_first_n_rows_and_send(csv_file, webhook_url, n=ROWS_TO_PROCESS)
    print("\nScript execution finished.")