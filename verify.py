import sys
import os
import glob
import requests
import pandas as pd
import re
import asyncio
import aiohttp
import json
import random
from typing import List, Dict, Union
from datetime import datetime, timedelta

GMASS_API_URL = "https://verify.gmass.co/verify"
API_KEY = "04470b2e-2aa8-4e43-9a46-27cee7a48422"
GMASS_API_KEY = API_KEY

INPUT_FOLDER = sys.argv[1]
OUTPUT_FOLDER = sys.argv[2]

def read_string_file(datetime_file):
    with open(datetime_file, "r") as file:
        datetime_string = file.read().strip()
    return datetime_string

def save_string_file(datetime_file, datetime_string):
    with open(datetime_file, "w") as file:
        file.write(datetime_string)

def clean_email(email: Dict[str, Union[str, None]]) -> Dict[str, Union[str, None]]:
    if isinstance(email["Email"], str):
        cleaned_email = re.findall(r"[\w\.-]+@[\w\.-]+", email["Email"])
        email["Email"] = cleaned_email[0] if cleaned_email else None
    return email

def filter_emails(emails_to_verify: List[Dict[str, Union[str, None]]],
                  verified_emails: List[Dict[str, Union[str, None]]]) -> List[Dict[str, Union[str, None]]]:
    return [obj for obj in emails_to_verify if all(obj["Email"] != obj2["Email"] for obj2 in verified_emails)]

def make_fake_emails(original_email: str) -> List[str]:
    fake_emails = []
    for _ in range(2):
        random_number = random.randint(1000, 9999)
        fake_email = f'{original_email.split("@")[0]}{random_number}@{original_email.split("@")[1]}'
        fake_emails.append(fake_email)
    return fake_emails

async def get_response(email: str) -> Dict:
    url = f"{GMASS_API_URL}?email={email}&key={GMASS_API_KEY}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                result = json.loads(await response.text())
                return result
    except Exception as e:
        print(f"Error verifying {email}: {str(e)}")
        return None

async def verify_email(email: str) -> Dict:
    all_emails = [email] + make_fake_emails(email)
    tasks = [asyncio.ensure_future(get_response(email)) for email in all_emails]
    results = await asyncio.gather(*tasks)
    if results[0] is not None:
        verification_status = "Could not verify"
        catch_all = False
        if results[0]["Success"] and results[0]["Valid"]:
            verification_status = "Valid"
            if all(result["Success"] and result["Valid"] for result in results[1:]):
                catch_all = True
        elif results[0]["Success"]:
            verification_status = "Invalid"
        result_str_lower = str(results[0]).lower()
        filter_list = [filter_name for filter_name in ["Barracuda", "Cloudfilter"] if filter_name.lower() in result_str_lower]
        service_list = [service_name for service_name in ["google", "outlook"] if service_name.lower() in result_str_lower]
        output_email = {
            "Email": email,
            "Verification Status": verification_status,
            "Response": results[0],
            "Filter": ",".join(filter_list),
            "Service": ",".join(service_list),
            "Catch All": catch_all,
        }
        return output_email
    else:
        output_email = {
            "Email": email,
            "Verification Status": "ERROR",
            "Response": "",
            "Filter": "",
            "Service": "",
            "Catch All": False,
        }
        return output_email

def remove_unverified_emails(emails: List[Dict[str, Union[str, None]]]) -> List[Dict[str, Union[str, None]]]:
    return [email for email in emails if email["Verification Status"] not in ["Invalid", "Valid"]]

async def verify_emails_parallel(input_filename: str, output_filename: str) -> None:
    emails_to_verify = await read_file(input_filename)
    verified_emails = []
    if os.path.exists(output_filename):
        verified_emails = await read_file(output_filename)
        verified_emails = remove_unverified_emails(verified_emails)
    cleaned_emails = [clean_email(email) for email in emails_to_verify if isinstance(email["Email"], str)]
    filtered_emails = filter_emails(cleaned_emails, verified_emails)
    filtered_emails = list(set(filtered_email["Email"] for filtered_email in filtered_emails))
    print(f"{len(filtered_emails)} emails verifying...")

    verified_emails = []
    chunk_size = 100
    chunks = [filtered_emails[i : i + chunk_size] for i in range(0, len(filtered_emails), chunk_size)]
    print(len(chunks))

    for index, chunk in enumerate(chunks):
        tasks = [asyncio.ensure_future(verify_email(email)) for email in chunk]
        results = await asyncio.gather(*tasks)
        print(chunk_size * (index + 1))
        verified_emails += results
        if index % 15 == 14:
            output_data = pd.DataFrame(verified_emails)
            output_data.to_csv(output_filename, index=False)
            print("API LIMIT reached, sleeping for 1 hour")
            await asyncio.sleep(3600)

    output_data = pd.DataFrame(verified_emails)
    output_data.to_csv(output_filename, index=False)
    print("API LIMIT reached, sleeping for 1 hour")
    await asyncio.sleep(3600)

async def read_file(file_name: str) -> List[Dict[str, Union[str, None]]]:
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
            await asyncio.sleep(5)

async def process_new_files() -> None:
    while True:
        supported_file_extensions = (".csv", ".xlsx")
        files_to_process = []
        for ext in supported_file_extensions:
            files_to_process.extend(glob.glob(os.path.join(INPUT_FOLDER, f"*{ext}")))
        print("Files: ", files_to_process)
        for file_to_process in files_to_process:
            output_file = os.path.join(OUTPUT_FOLDER, "verified_emails.csv")
            await verify_emails_parallel(file_to_process, output_file)
            original_file = os.path.join(
                OUTPUT_FOLDER,
                "verified",
                "verified_" + os.path.basename(file_to_process),
            )
            if os.path.exists(original_file):
                os.remove(original_file)
            os.rename(file_to_process, original_file)
        break

async def main() -> None:
    print("Verifying emails...")
    await process_new_files()
    print("Verification completed.")

asyncio.run(main())