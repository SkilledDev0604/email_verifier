import sys
import os
import glob
import requests
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
import time
# GMass API endpoint
GMASS_API_URL = "https://verify.gmass.co/verify"
i = 1
# Your GMass API Key
API_KEY = "04470b2e-2aa8-4e43-9a46-27cee7a48422"
GMASS_API_KEY = API_KEY

# Input and output directories
INPUT_FOLDER = sys.argv[1]
OUTPUT_FOLDER = sys.argv[2]

# Function to clean emails by removing junk data
def clean_email(email):
    if isinstance(email, str):
        cleaned_email = re.findall(r'[\w\.-]+@[\w\.-]+', email)
        if cleaned_email:
            return cleaned_email[0]  # Return the first extracted email
    return None  # Return None for non-string values or if no valid email is found


# Function to filter emails by removing verified emails
def filter_emails(emails_to_verify, verified_emails):

    return [obj for obj in emails_to_verify if all(obj['Email'] != obj2['Email'] for obj2 in verified_emails)]
 # Return the first extracted email

# Function to verify a single email using GMass API
def verify_email(email):
    global i
    batch_size = 4500
    if i >= batch_size:
        print('on sleep due to meet API limit')
        time.sleep(3600)  # Sleep for an hour
    try:
        # Build the API URL with the email and API key
        url = f"{GMASS_API_URL}?email={email['Email']}&key={GMASS_API_KEY}"
        response = requests.get(url)
        result = response.json()
        print("Count: ", i, " ", result)
        i += 1
        # Extract the verification status from the result
        # verification_status = "PASS" if result.get("Success", False) and result.get("Valid", False) else "FAIL"
        if result.get("Success", False):
            if result.get("Valid", False):
                verification_status = "Valid"
            else:
                verification_status = "Invalid"
        else:
            verification_status = "Could not verify"
        email['Verification Status'] = verification_status
        email['Response'] = result
        return email
    except Exception as e:
        i += 1
        print(f"Error {i} verifying {email}: {str(e)}")
        email['Verification Status'] = "ERROR"
        email['Response'] = ''
        return email

# Function to verify emails using multiple threads
def verify_emails_parallel(input_filename, output_filename):

    emails_to_verify = read_file(input_filename)
    verified_emails = read_file(output_filename)

    results = []
    print(len(emails_to_verify))

    
    # Clean the emails before verification
    cleaned_emails = [clean_email(email) for email in emails_to_verify]

    # Remove None values (invalid emails) from the list
    cleaned_emails = [email for email in cleaned_emails if email]

    filtered_emails = filter_emails(emails_to_verify, verified_emails)

    results = []
    print(len(filtered_emails))

    # valid_emails = re.findall(r'[\w\.-]+@[\w\.-]+', input_data["Email"])

    with ThreadPoolExecutor(max_workers=50) as executor:  # Adjust max_workers as needed
        results = list(executor.map(verify_email, filtered_emails))

    output_data = pd.DataFrame(verified_emails + results)
    output_data.to_csv(output_filename, index=False)

# Function to process new CSV and XLSX files
def process_new_files():
    print("in product fun")
    while True:
        supported_file_extensions = (".csv", ".xlsx")
        files_to_process = []

        for ext in supported_file_extensions:
            files_to_process.extend(glob.glob(os.path.join(INPUT_FOLDER, f"*{ext}")))

        print("Files: ", files_to_process)
        for file_to_process in files_to_process:
            historicalSize = -1
            while (historicalSize != os.path.getsize(file_to_process)):
                historicalSize = os.path.getsize(file_to_process)
                time.sleep(2)
            print(os.path.getsize(file_to_process))
            
            # Generate output filename
            output_file = os.path.join(OUTPUT_FOLDER, f"verified_{os.path.basename(file_to_process)}")

            # Verify emails using multiple threads and save results
            verify_emails_parallel(file_to_process, output_file)

            original_file = os.path.join(OUTPUT_FOLDER, os.path.basename(file_to_process))
            if os.path.exists(original_file):
                os.remove(original_file)  # Delete the existing file in the output folder

            # Move the processed file to another directory
            os.rename(file_to_process, original_file)
        break
        # time.sleep(3600)  # Sleep for an hour
def read_file(file_name):
    while True:
        try:
            if file_name.endswith(".csv"):
                input_data = pd.read_csv(file_name, encoding='latin-1')
            elif file_name.endswith(".xlsx"):
                input_data = pd.read_excel(file_name)
            else:
                print(f"Unsupported file format: {file_name}")
                return []
            df = pd.DataFrame(input_data)
            return df.to_dict(orient='list')
        except:
            time.sleep(5)

if __name__ == "__main__":
    process_new_files()
