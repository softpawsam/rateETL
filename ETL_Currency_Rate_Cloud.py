import requests, pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
import os
import schedule
import time
from datetime import datetime

load_dotenv()  # This automatically finds and loads the .env file

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

api_key = os.getenv("API_KEY")


def etl_currency_rates():
    url = "https://api.currencybeacon.com/v1/latest"

    params = {
        "base": "IDR",
        "api_key": api_key
    }

    response = requests.get(url, params=params)

    # Convert to JSON
    data = response.json()

    # Print the result
    print(data)

    if response.status_code == 200:
        data = response.json()
        print("Success:", data)
    else:
        print("Error:", response.status_code, response.text)

    response_data = data["response"]
    rates = response_data["rates"]
    base = response_data["base"]
    timestamp = response_data["date"]

    # Convert to DataFrame
    df = pd.DataFrame(list(rates.items()), columns=["Currency", "Rate"])

    # Add extra columns
    df["Base"] = base
    df["Last_Updated"] = timestamp

    # Optional: sort and round
    df["Rate"] = df["Rate"].round(6)
    df = df.sort_values(by="Rate", ascending=False)
    df = df.reset_index(drop=True)

    df.head()

    connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    engine = create_engine(connection_string)

    df.to_sql("currency_rates", engine,schema="public", if_exists="replace", index=False)

    print("Data successfully loaded into PostgreSQL!")
    print(f"✓ Data loaded to: {db_name}.public.currency_rates")


etl_currency_rates()