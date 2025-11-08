# Currency ETL Pipeline

This project extracts live currency exchange rates from the **CurrencyBeacon API**, transforms the data, and loads it into a **Supabase PostgreSQL database**. 
ETL_Currency_Rate_Cloud.py uses a cloud database.
ETL_Currency_Rate_Local.py uses a local database.

---

## Features

* Fetches live currency rates using `requests`
* Converts timestamp from UTC to WIB (UTC+7)
* Calculates currency value inversely (`USD_per_Currency`)
* Stores data into Supabase PostgreSQL using SQLAlchemy
* Automatically creates table if it doesn't exist
* Environment variables managed with `.env`
* Automated and scheduled execution using GitHub Actions

---

## Tech Stack

| Component | Technology                                                                |
| --------- | ------------------------------------------------------------------------- |
| Language  | Python                                                                    |
| API       | CurrencyBeacon ([https://currencybeacon.com](https://currencybeacon.com)) |
| Database  | Supabase PostgreSQL                                                       |
| Libraries | `requests`, `pandas`, `sqlalchemy`, `dotenv`                              |

---

## Installation

1. Clone the repository:

```sh
git clone <your_repo_url>
cd <project_folder>
```

2. Install dependencies:

```sh
pip install -r requirements.txt
```

3. Create a `.env` file and fill in your credentials:

```env
API_KEY=your_currencybeacon_api_key

# Supabase Connection
CDB_HOST=your_host.supabase.co
CDB_PORT=5432
CDB_NAME=postgres
CDB_USER=postgres
CDB_PASSWORD=your_password
```

---

## Running the ETL Script

To execute the script manually:

```sh
python <filename>.py
```

You should see logs:

```
 API call successful
🔁 Transformed <X> records
📦 Data successfully appended to Supabase PostgreSQL!
```

---

## Code Overview

* **Extract** → Calls CurrencyBeacon API
* **Transform** → Converts timestamp, cleans negative/invalid data, calculates inverse rate
* **Load** → Inserts into Supabase PostgreSQL table (`currency_rates`)

---

## Table Schema

| Column           | Type  | Description                    |
| ---------------- | ----- | ------------------------------ |
| Currency         | TEXT  | Currency code (IDR, EUR, etc)  |
| Rate             | FLOAT | Value in USD → target currency |
| Base             | TEXT  | Base currency (always `USD`)   |
| USD_per_Currency | FLOAT | Inverse value (1 / rate)       |
| Updated_Date     | TEXT  | WIB converted date             |
| Updated_Time     | TEXT  | WIB converted time             |

---

## Automation

You can automate this ETL using GitHub Actions and run on schedule (cron) to update daily.

---


### Author

Developed by **Samuel Adrian**
