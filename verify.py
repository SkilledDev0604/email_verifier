import sys
import os
import glob
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


async def get_response(email: str) -> Dict:
    url = f"{GMASS_API_URL}?email={email}&key={GMASS_API_KEY}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                result = await response.json()
                return result
    except aiohttp.ClientError as e:
        print(f"Error verifying {email}: {str(e)}")
        return None


async def verify_email(email: str) -> Dict:
    print("*", end="")
    all_emails = [email] + make_fake_emails(email)
    tasks = [asyncio.ensure_future(get_response(email)) for email in all_emails]
    results = await asyncio.gather(*tasks)
    if results[0] is not None:
        verification_status = "Could not verify"
        catch_all = False
        if results[0]["Success"] and results[0]["Valid"]:
            verification_status = "Valid"
            if all(result is not None for result in results[1:3]):
                if all(
                    result["Success"] and result["Valid"] for result in results[1:3]
                ):
                    catch_all = True
        elif results[0]["Success"]:
            verification_status = "Invalid"
        filter_list = [
            filter_name
            for filter_name in ["Barracuda", "Cloudfilter"]
            if filter_name.lower() in str(results[0]).lower()
        ]
        service_list = [
            service_name
            for service_name in ["google", "outlook"]
            if service_name.lower() in str(results[0]).lower()
        ]
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


def make_fake_emails(original_email: str) -> List[str]:
    fake_emails = []
    for _ in range(2):
        random_number = random.randint(1000, 9999)
        fake_email = f'{original_email.split("@")[0]}{random_number}@{original_email.split("@")[1]}'
        fake_emails.append(fake_email)
    return fake_emails


def clean_email(email: Dict[str, Union[str, None]]) -> Dict[str, Union[str, None]]:
    if isinstance(email["Email"], str):
        cleaned_email = re.findall(r"[\w\.-]+@[\w\.-]+", email["Email"])
        email["Email"] = cleaned_email[0] if cleaned_email else None
    return email


def filter_emails(
    emails_to_verify: List[Dict[str, Union[str, None]]],
    verified_emails: List[Dict[str, Union[str, None]]],
) -> List[Dict[str, Union[str, None]]]:
    return [
        obj
        for obj in emails_to_verify
        if all(obj["Email"] != obj2["Email"] for obj2 in verified_emails)
    ]


def remove_unverified_emails(
    emails: List[Dict[str, Union[str, None]]]
) -> List[Dict[str, Union[str, None]]]:
    return [
        email
        for email in emails
        if email["Verification Status"] not in ["Invalid", "Valid"]
    ]

async def verify_emails_parallel(
    input_filenames: List[str], output_filename: str
) -> None:
    emails_to_verify = [
        element
        for input_filename in input_filenames
        for element in read_file(input_filename)
    ]
    verified_emails = (
        read_file(output_filename) if os.path.exists(output_filename) else []
    )
    verified_emails = remove_unverified_emails(verified_emails)

    cleaned_emails = [
        clean_email(email)
        for email in emails_to_verify
        if isinstance(email["Email"], str)
    ]
    filtered_emails = filter_emails(cleaned_emails, verified_emails)
    filtered_emails = list(
        set(filtered_email["Email"] for filtered_email in filtered_emails)
    )

    print(f"{len(filtered_emails)} emails verifying...")

    verified_emails = []
    chunk_size = 100
    chunks = [
        filtered_emails[i : i + chunk_size]
        for i in range(0, len(filtered_emails), chunk_size)
    ]

    for index, chunk in enumerate(chunks):
        print(
            f"{index * chunk_size + 1} - {(index + 1) * chunk_size} emails verifying ...."
        )
        tasks = [asyncio.ensure_future(verify_email(email)) for email in chunk]
        results = await asyncio.gather(*tasks)
        verified_emails += results

        if index % 15 == 14:
            output_data = pd.DataFrame(verified_emails)
            with open(output_filename, "w") as file:
                output_data.to_csv(file, index=False)
            print("\nAPI LIMIT reached, sleeping for 1 hour")
            await asyncio.sleep(3600)

    output_data = pd.DataFrame(verified_emails)
    with open(output_filename, "w") as file:
        output_data.to_csv(file, index=False)
    return verified_emails


def read_file(file_name: str) -> List[Dict[str, Union[str, None]]]:
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
    except (FileNotFoundError, PermissionError) as e:
        print(f"Error reading file {file_name}: {str(e)}")
        return []


def get_folder_size(folder_path):
    total_size = 0
    for path, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(path, file)
            total_size += os.path.getsize(file_path)
    return total_size


async def process_new_files() -> None:
    supported_file_extensions = (".csv", ".xlsx")
    files_to_process = []

    print("Files: ", files_to_process)

    history_size = -1
    while history_size != get_folder_size(INPUT_FOLDER):
        history_size = get_folder_size(INPUT_FOLDER)
        await asyncio.sleep(2)
    files_to_process = [
        file
        for ext in supported_file_extensions
        for file in glob.glob(os.path.join(INPUT_FOLDER, f"*{ext}"))
    ]
    output_file = os.path.join(OUTPUT_FOLDER, "verified_emails.csv")

    verified_emails = await verify_emails_parallel(files_to_process, output_file)

    for file_to_process in files_to_process:
        emails = read_file(file_to_process)
        new_emails = []
        for input_email in emails:
            for verified_email in verified_emails:
                if input_email['Email'] == verified_email['Email']:
                    new_emails.append({**input_email, **verified_email})
                    break
        output_data = pd.DataFrame(new_emails)
        output_filename = os.path.join(
            OUTPUT_FOLDER,
            "verified",
            f"verified_output_{os.path.basename(file_to_process)}",
        )
        with open(output_filename, "w") as file:
            output_data.to_csv(file, index=False)
        original_file = os.path.join(
            OUTPUT_FOLDER,
            "verified",
            "verified_input_" + os.path.basename(file_to_process),
        )
        if os.path.exists(original_file):
            os.remove(original_file)
        os.rename(file_to_process, original_file)

    print(f"\nSuccess! Because of API LIMIT, sleeping for 1 hour")
    await asyncio.sleep(3600)


async def main() -> None:
    print("Verifying emails...")
    await process_new_files()
    print("Verification completed.")


asyncio.run(main())
