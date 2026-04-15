from entsoe import EntsoePandasClient
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import os

load_dotenv('api.env')

client = EntsoePandasClient(api_key=os.getenv('ENTSO_API_KEY'))
engine = create_engine(os.getenv('DB_URL'))

def setup_db(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS prices (
                timestamp TIMESTAMPTZ NOT NULL,
                zone TEXT NOT NULL,
                price_eur_mwh DOUBLE PRECISION,
                PRIMARY KEY (timestamp, zone)
            );
        """))
        conn.commit()

def fetch_and_store(zone_code, start, end):
    print(f"Fetching {zone_code}...")
    try:
        prices = client.query_day_ahead_prices(zone_code, start=start, end=end)
        df = prices.reset_index()
        df.columns = ['timestamp', 'price_eur_mwh']
        df['zone'] = zone_code
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        df.to_sql('prices', engine, if_exists='append', index=False,
                  method='multi')
        print(f"  Stored {len(df)} rows for {zone_code}")
    except Exception as e:
        print(f"  Failed for {zone_code}: {e}")

setup_db(engine)

start = pd.Timestamp('20260401', tz='Europe/Brussels')
end   = pd.Timestamp('20260410', tz='Europe/Brussels')

zones = ['FR', 'DE_LU', 'NO_2']
for zone in zones:
    fetch_and_store(zone, start, end)