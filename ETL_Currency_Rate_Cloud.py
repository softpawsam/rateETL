import requests
import pandas as pd
from sqlalchemy import create_engine, inspect
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os

def load_currency_data():
    try:
        # Load environment variables
        load_dotenv()
        
        # === Extract ===
        api_key = os.getenv("API_KEY")
        url = "https://api.currencybeacon.com/v1/latest"
        params = {"base": "USD", "api_key": api_key}
        
        response = requests.get(url, params=params)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text)
            return False
        
        data = response.json()
        print("✅ API call successful")
        
        # === Transform ===
        response_data = data["response"]
        rates = response_data["rates"]
        base = response_data["base"]
        timestamp = response_data["date"]

        utc_dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        wib_dt = utc_dt + timedelta(hours=7)
        date_str = wib_dt.strftime("%Y-%m-%d")
        time_str = wib_dt.strftime("%H:%M:%S")

        df = pd.DataFrame(list(rates.items()), columns=["Currency", "Rate"])
        df["Base"] = base
        df["Rate"] = pd.to_numeric(df["Rate"], errors="coerce").round(6)
        df = df.dropna(subset=["Rate"])
        df = df[df["Rate"] > 0]

        df["USD_per_Currency"] = (1 / df["Rate"]).round(6)
        df = df.dropna(subset=["USD_per_Currency"])
        df = df[df["USD_per_Currency"] > 0]

        df["Updated_Date"] = date_str
        df["Updated_Time"] = time_str
        df = df.sort_values(by="Rate", ascending=False).reset_index(drop=True)

        print(f"🔁 Transformed {len(df)} records")

        # === Load ===
        db_host = os.getenv("CDB_HOST")
        db_port = os.getenv("CDB_PORT")
        db_name = os.getenv("CDB_NAME")
        db_user = os.getenv("CDB_USER")
        db_password = os.getenv("CDB_PASSWORD")

        connection_string = (
            f"postgresql+psycopg2://{db_user}:{db_password}"
            f"@{db_host}:{db_port}/{db_name}?sslmode=require"
        )
        engine = create_engine(connection_string)

        inspector = inspect(engine)
        table_name = "currency_rates"

        # Create table if it doesn’t exist
        if not inspector.has_table(table_name):
            print("🧱 Table not found — creating table...")
            df.head(0).to_sql(table_name, engine, index=False)
            print("✅ Table created successfully!")

        # Append new data
        df.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
        print("📦 Data successfully appended to Supabase PostgreSQL!")

        return True

    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        return False

# Run once
if __name__ == "__main__":
    load_currency_data()
