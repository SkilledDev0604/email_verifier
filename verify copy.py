import sys
import os
import glob
import requests
import pandas as pd
import re
from concurrent.futures import ThreadPoolExecutor
import time
import asyncio
import aiohttp
import json
import random

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
    if isinstance(email["Email"], str):
        cleaned_email = re.findall(r"[\w\.-]+@[\w\.-]+", email["Email"])
        if cleaned_email[0]:
            email["Email"] = cleaned_email[0]
        else:
            email["Email"] = None  # Return the first extracted email
    return email  # Return None for non-string values or if no valid email is found


# Function to filter emails by removing verified emails
def filter_emails(emails_to_verify, verified_emails):
    return [
        obj
        for obj in emails_to_verify
        if all(obj["Email"] != obj2["Email"] for obj2 in verified_emails)
    ]


# Return the first extracted email


# make 2 fake emails
def make_fake_emails(original_email):
    fake_emails = []

    for _ in range(2):
        random_number = random.randint(1000, 9999)  # Generate a random 4-digit number
        fake_email = f'{original_email.split("@")[0]}{random_number}@{original_email.split("@")[1]}'
        fake_emails.append(fake_email)

    return fake_emails


async def get_response(email):
    # Build the API URL with the email and API key
    url = f"{GMASS_API_URL}?email={email}&key={GMASS_API_KEY}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                # code block to be executed
                result = json.loads(await response.text())
                return result
    except Exception as e:
        print(f"Error {i} verifying {email}: {str(e)}")
        return None


# Function to verify a single email using GMass API
async def verify_email(email):
    all_emails = [email] + make_fake_emails(email)
    tasks = []
    for email in all_emails:
        task = asyncio.ensure_future(get_response(email))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    if results[0] is not None:
        # Extract the verification status from the result
        # verification_status = "PASS" if result.get("Success", False)  and  result.get("Valid", False) else "FAIL"
        catch_all = False
        if results[0]["Success"] == True:
            if results[0]["Valid"] == True:
                verification_status = "Valid"
                if (
                    results[1]["Success"] == True
                    and results[2]["Success"] == True
                    and results[1]["Valid"] == True
                    and results[2]["Valid"] == True
                ):
                    catch_all = True
            else:
                verification_status = "Invalid"
        else:
            verification_status = "Could not verify"

        result_str_lower = str(results[0]).lower()
        filter = []
        if result_str_lower.find("Barracuda") > -1:
            filter.append("Barracuda")
        if result_str_lower.find("Cloudfilter") > -1:
            filter.append("Cloudfilter")

        service = []
        if result_str_lower.find("google") > -1:
            service.append("google")
        if result_str_lower.find("outlook") > -1:
            service.append("outlook")

        output_email = {}
        output_email["Email"] = email
        output_email["Verification Status"] = verification_status
        output_email["Response"] = results[0]
        output_email["Filter"] = ",".join(filter)
        output_email["Service"] = ",".join(service)
        output_email["Catch All"] = catch_all
        return output_email
    else:
        output_email = {}
        output_email["Email"] = email
        output_email["Verification Status"] = "ERROR"
        output_email["Response"] = ""
        output_email["Filter"] = ""
        output_email["Service"] = ""
        output_email["Catch All"] = False
        return output_email


# delete unverifed emails in verified emials
def remove_unverifed_emails(emails):
    return [
        email
        for email in emails
        if (
            email["Verification Status"] == "Invalid"
            and email["Verification Status"] == "Valid"
        )
    ]


# Function to verify emails using multiple threads
async def verify_emails_parallel(input_filename, output_filename):
    emails_to_verify = read_file(input_filename)

    # Check if file exists
    file_exists = os.path.exists(output_filename)
    verified_emails = []
    if file_exists:
        verified_emails = read_file(output_filename)
        verified_emails = remove_unverifed_emails(verified_emails)

    results = []

    # Clean the emails before verification
    cleaned_emails = [clean_email(email) for email in emails_to_verify]
    # Remove None values (invalid emails) from the list
    cleaned_emails = [email for email in cleaned_emails if email["Email"]]

    filtered_emails = filter_emails(cleaned_emails, verified_emails)
    filtered_emails = list(
        set(filtered_email["Email"] for filtered_email in filtered_emails)
    )

    print(f"{len(filtered_emails)} emails verifing...")

    results = []

    # valid_emails = re.findall(r'[\w\.-]+@[\w\.-]+', input_data["Email"])
    chunk_size = 100
    chunks = [
        filtered_emails[i : i + chunk_size]
        for i in range(0, len(filtered_emails), chunk_size)
    ]
    print(len(chunks))
    for index, chunk in enumerate(chunks):
        tasks = []
        for email_index, email in enumerate(chunk):
            task = asyncio.ensure_future(verify_email(email))
            tasks.append(task)
        results = await asyncio.gather(*tasks)
        print(chunk_size * (index + 1))
        verified_emails += results
        if index % 15 == 14:
            output_data = pd.DataFrame(verified_emails)
            output_data.to_csv(output_filename, index=False)
            time.sleep(3600)
            print("API LIMIT, sleeping...")
    output_data = pd.DataFrame(verified_emails)
    output_data.to_csv(output_filename, index=False)


# Function to process new CSV and XLSX files
async def process_new_files():
    while True:
        supported_file_extensions = (".csv", ".xlsx")
        files_to_process = []
        for ext in supported_file_extensions:
            files_to_process.extend(glob.glob(os.path.join(INPUT_FOLDER, f"*{ext}")))

        print("Files: ", files_to_process)
        for file_to_process in files_to_process:
            # historicalSize = -1
            # while historicalSize != os.path.getsize(file_to_process):
            #     historicalSize = os.path.getsize(file_to_process)
            #     time.sleep(2)
            # # print(os.path.getsize(file_to_process))

            # Generate output filename
            output_file = os.path.join(OUTPUT_FOLDER, f"verified_emails.csv")

            # Verify emails using multiple threads and save results
            await verify_emails_parallel(file_to_process, output_file)

            original_file = os.path.join(
                OUTPUT_FOLDER, "verified", os.path.basename(file_to_process)
            )
            if os.path.exists(original_file):
                os.remove(
                    original_file
                )  # Delete the existing file in the output folder

            # Move the processed file to another directory
            os.rename(file_to_process, original_file)
        break
        # time.sleep(3600)  # Sleep for an hour


def read_file(file_name):
    while True:
        try:
            if file_name.endswith(".csv"):
                input_data = pd.read_csv(file_name, encoding="latin-1")
            elif file_name.endswith(".xlsx"):
                input_data = pd.read_excel(file_name)
            else:
                print(f"Unsupported file format: {file_name}")
                return []
            df = pd.DataFrame(input_data)
            return df.to_dict(orient="records")
        except:
            time.sleep(5)


async def main():
    print("verifying...")
    await process_new_files()
    print("verified...")


asyncio.run(main())
