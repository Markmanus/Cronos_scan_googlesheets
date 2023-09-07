import json
import pprint
import os
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# Google API Details
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT_FILE = './manecity.json'
SPREADSHEET_ID = '1NCEZBA66B6SZO0oCJC8IZcU6M4lQ5wYI-OMkjm44TyE'

creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

def get_latest_block():
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="norm_tx!A2:A").execute()
    values = result.get('values', [])
    if values:
        max_block = max([int(row[0]) for row in values])
        return max_block
    return 0

def get_latest_internal_block():
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range="int_tx!A2:A").execute()
    values = result.get('values', [])
    if values:
        max_block = max([int(row[0]) for row in values])
        return max_block
    return 0


def append_to_sheet(rows):
    body = {'values': [list(row.values()) for row in rows]}
    result = sheet.values().append(spreadsheetId=SPREADSHEET_ID, range="norm_tx!A2", body=body, valueInputOption="RAW").execute()
    print(f"{result.get('updates').get('updatedRows')} rows appended in Normal transaction sheet.")

def append_to_internal_sheet(rows):
    body = {'values': [list(row.values()) for row in rows]}
    result = sheet.values().append(spreadsheetId=SPREADSHEET_ID, range="int_tx!A2", body=body, valueInputOption="RAW").execute()
    print(f"{result.get('updates').get('updatedRows')} rows appended for internal transactions.")


def fetch_and_save_transactions():
    latest_block = get_latest_block()

    url = "https://api.cronoscan.com/api"
    API_KEY = os.environ['API_KEY']
    params = {
        "module": "account",
        "action": "txlist",
        "address": "0x0ca35bdf10f0f548857fe222760bf47761bbaf50",
        "startblock": latest_block + 1,
        "endblock": 'latest',
        "page": 1,
        "offset": 10000,
        "sort": "asc",
        "apikey": API_KEY
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        new_results = []

        for result in data.get("result", []):
            filtered_result = {key: result[key] for key in ["blockNumber", "from", "timeStamp", "value"] if
                               key in result}
            new_results.append(filtered_result)

        if new_results:
            formatted_results = []
            for result in new_results:
                formatted_result = {
                    "blockNumber": result["blockNumber"],
                    "from": result["from"],
                    "timeStamp": pd.to_datetime(int(result["timeStamp"]), unit='s').strftime('%d/%m/%Y %H:%M'),
                    "value": round(float(result["value"]) / 10 ** 18, 2)
                }
                formatted_results.append(formatted_result)
            append_to_sheet(formatted_results)
        else:
            print("No new transactions.")


def fetch_and_save_internal_transactions():
    latest_block = get_latest_internal_block()

    url = "https://api.cronoscan.com/api"
    API_KEY = os.environ['API_KEY']
    params = {
        "module": "account",
        "action": "txlistinternal",
        "address": "0x0ca35bdf10f0f548857fe222760bf47761bbaf50",
        "startblock": latest_block + 1,
        "endblock": 'latest',
        "page": 1,
        "offset": 10000,
        "sort": "asc",
        "apikey": API_KEY
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        new_results = []

        for result in data.get("result", []):
            # Exclude the specific "to" address from being appended
            if result.get("to") == "0x1caf6d213f8210c17e3c92f879c5ef4bb1d940da":
                continue
            # Adapt this based on the actual keys in the "result"
            filtered_result = {key: result[key] for key in ["blockNumber", "to", "timeStamp", "value"] if
                               key in result}
            new_results.append(filtered_result)

        if new_results:
            formatted_results = [] # You can reuse the same formatting logic
            for result in new_results:
                formatted_result = {
                    "blockNumber": result["blockNumber"],
                    "to": result["to"],
                    "timeStamp": pd.to_datetime(int(result["timeStamp"]), unit='s').strftime('%d/%m/%Y %H:%M'),
                    "value": round(float(result["value"]) / 10 ** 18, 2)
                }
                formatted_results.append(formatted_result)
            append_to_internal_sheet(formatted_results)
        else:
            print("No new transactions.")


if __name__ == "__main__":
    fetch_and_save_transactions()
    fetch_and_save_internal_transactions()