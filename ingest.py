from entsoe import EntsoePandasClient
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import os

load_dotenv()

client = EntsoePandasClient(api_key=os.getenv('ENTSO_API_KEY'))
engine = create_engine(os.getenv('DB_URL'))

def fetch_and_store(zone_code):
    # fetch last 2 hours to ensure we always capture the latest
    end = pd.Timestamp.now(tz='Europe/Brussels')
    start = end - pd.Timedelta(hours=2)
    
    try:
        prices = client.query_day_ahead_prices(zone_code, start=start, end=end)
        df = prices.reset_index()
        df.columns = ['timestamp', 'price_eur_mwh']
        df['zone'] = zone_code
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        # use ON CONFLICT to avoid duplicate rows
        with engine.connect() as conn:
            for _, row in df.iterrows():
                conn.execute(text("""
                    INSERT INTO prices (timestamp, zone, price_eur_mwh)
                    VALUES (:timestamp, :zone, :price_eur_mwh)
                    ON CONFLICT (timestamp, zone) DO NOTHING;
                """), row.to_dict())
            conn.commit()
        print(f"Stored {len(df)} rows for {zone_code}")
    except Exception as e:
        print(f"Failed for {zone_code}: {e}")

zones = ['FR', 'DE_LU', 'NO_2', 'NL', 'BE']
for zone in zones:
    fetch_and_store(zone)