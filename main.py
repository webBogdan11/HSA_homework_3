import requests
from dotenv import load_dotenv
import os
import json
import uuid
import schedule
import time
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("background_worker.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()

MEASUREMENT_ID = os.getenv("MEASUREMENT_ID")
API_SECRET = os.getenv("API_SECRET")
CLIENT_ID = os.getenv("CLIENT_ID", str(uuid.uuid4()))

def get_uah_usd_rate():
    """
    Fetches the USD to UAH exchange rate from the National Bank of Ukraine's public API.
    """
    url = "https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Find USD currency in the response
        for item in data:
            if item.get("cc") == "USD":
                rate = item.get("rate")
                logging.info(f"Fetched USD/UAH rate: {rate}")
                return rate
        logging.warning("USD rate not found in the response.")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching USD/UAH rate: {e}")
        return None

def send_event_to_ga4(rate):
    """
    Sends the fetched exchange rate as an event to Google Analytics 4.
    """
    logging.info(f"Sending event to GA4 with rate: {rate}")
    endpoint = f"https://www.google-analytics.com/mp/collect?measurement_id={MEASUREMENT_ID}&api_secret={API_SECRET}"
    
    # Generate a consistent session_id or retrieve it if maintaining sessions
    session_id = str(uuid.uuid4())  # For demonstration; implement proper session management as needed
    
    # Calculate timestamp in microseconds since epoch
    timestamp_micros = int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000)
    
    payload = {
        "client_id": CLIENT_ID,
        "timestamp_micros": timestamp_micros,
        "events": [
            {
                "name": "uah_usd_rate",
                "params": {
                    "rate": rate,
                    "engagement_time_msec": "100",  # Example engagement time
                    "session_id": session_id,
                }
            }
        ],
  
    }
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(endpoint, data=json.dumps(payload), headers=headers, timeout=10)
        print(response.content)
        logging.info(f"Response: {response.status_code}, {response.text}")
        if response.status_code == 204:
            logging.info("Event sent successfully.")
        else:
            logging.error(f"Failed to send event. Status Code: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error sending event to GA4: {e}")

def job():
    """
    The scheduled job that fetches the rate and sends the event.
    """
    logging.info("Starting scheduled job.")
    rate = get_uah_usd_rate()
    if rate is not None:
        send_event_to_ga4(rate)
    else:
        logging.error("Could not fetch USD/UAH rate. Event not sent.")

def main():
    """
    Sets up the scheduler to run the job every hour.
    """
    schedule.every().hour.do(job)
    logging.info("Background worker started. Scheduling job to run every hour.")

    job()

    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except KeyboardInterrupt:
            logging.info("Background worker stopped by user.")
            break
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            time.sleep(60) 

if __name__ == "__main__":
    main()
