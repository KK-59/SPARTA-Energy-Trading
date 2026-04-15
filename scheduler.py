import schedule
import time
from ingest import fetch_and_store

zones = ['FR', 'DE_LU', 'NO_2']

def run_ingestion():
    print(f"Running ingestion...")
    for zone in zones:
        fetch_and_store(zone)

# run immediately on start, then every hour
run_ingestion()
schedule.every(1).hours.do(run_ingestion)

while True:
    schedule.run_pending()
    time.sleep(60)