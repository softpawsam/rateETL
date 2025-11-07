import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
import os
import schedule
import time

def etl_rate():

    load_dotenv()

    api_key = os.getenv("API_KEY")
    url = "https://api.currencybeacon.com/v1/latest"
    params = {"base" : "IDR","api_key" : api_key }

    response = requests.get(url, params=params)

    if response.status_code != 200:
        print("ERROR: ", response.status_code,response.text)
        return False

    data = response.json()
    print("API called successfully")


    response_data=data["response"]
    rates = response_data["rates"]
    base =  response_data["base"]
    timestamp = response_data["date"]

    