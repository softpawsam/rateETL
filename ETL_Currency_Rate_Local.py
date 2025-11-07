import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
import os
import schedule
import time
from datetime import datetime, timedelta, timezone

def load_currency_data():
    try:
        # Load environment variables
        load_dotenv()
        
        # Extract
        api_key = os.getenv("API_KEY")
        url = "https://api.currencybeacon.com/v1/latest"
        params = {"base": "USD", "api_key": api_key}
        
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            return False
        
        data = response.json()
        print("API call successful")
        
        # Transform
        response_data = data["response"]
        rates = response_data["rates"]
        base = response_data["base"]
        timestamp = response_data["date"]
        utc_dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        wib_dt = utc_dt + timedelta(hours=7)

        date = wib_dt.strftime("%Y-%m-%d")
        time = wib_dt.strftime("%H:%M:%S")

        df = pd.DataFrame(list(rates.items()), columns=["Currency", "Rate"])
        df["Base"] = base
        
        df["Rate"] = df["Rate"].round(6)

        df["Rate"] = pd.to_numeric(df["Rate"], errors="coerce")  # ensure numeric
        df = df.dropna(subset=["Rate"]) 
        df = df[df["Rate"] > 0] 

        df["USD_per_Currency"] = (1 / df["Rate"]).round(6)
        df = df.dropna(subset=["USD_per_Currency"])
        df = df[df["USD_per_Currency"] > 0] 

        df["Updated_Time"] = time
        df["Updated_Date"] = date

        df = df.sort_values(by="Rate", ascending=False).reset_index(drop=True)

        
        print(f"Transformed {len(df)} records")
        
        # Load
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")

        connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

        try:
            df.to_sql("currency_rates", engine, if_exists="append", index=False, method="multi")
            print("Data successfully appended to PostgreSQL!")
        except Exception as db_error:
            print(f"Database upload error: {db_error}") 
        return True
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return False

# Run once initially
load_currency_data()

def scheduled_etl():
    print(f"\n{'='*50}")
    print(f"Running scheduled job at {datetime.now()}")
    print(f"{'='*50}")
    load_currency_data()

# Schedule every hour
schedule.every(1).hour.do(scheduled_etl)

# Keep running
while True:
    schedule.run_pending()
    time.sleep(60)
