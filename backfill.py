from entsoe import EntsoePandasClient
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import os
import time

load_dotenv()

client = EntsoePandasClient(api_key=os.getenv('ENTSO_API_KEY'))
engine = create_engine(os.getenv('DB_URL'))

def backfill_zone(zone_code, start_date, end_date):
    # fetch one month at a time to avoid API timeouts
    current = start_date
    while current < end_date:
        chunk_end = min(current + pd.DateOffset(months=1), end_date)
        print(f"  Fetching {zone_code}: {current.date()} to {chunk_end.date()}")
        try:
            prices = client.query_day_ahead_prices(zone_code, start=current, end=chunk_end)
            df = prices.reset_index()
            df.columns = ['timestamp', 'price_eur_mwh']
            df['zone'] = zone_code
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

            with engine.connect() as conn:
                for _, row in df.iterrows():
                    conn.execute(text("""
                        INSERT INTO prices (timestamp, zone, price_eur_mwh)
                        VALUES (:timestamp, :zone, :price_eur_mwh)
                        ON CONFLICT (timestamp, zone) DO NOTHING;
                    """), row.to_dict())
                conn.commit()
            print(f"    Stored {len(df)} rows")
        except Exception as e:
            print(f"    Failed: {e}")

        current = chunk_end
        time.sleep(2)  # be polite to the API

zones = ['FR', 'DE_LU', 'NO_2', 'NL', 'BE']
start = pd.Timestamp('20240101', tz='Europe/Brussels')
end   = pd.Timestamp.now(tz='Europe/Brussels')

for zone in zones:
    print(f"\nBackfilling {zone}...")
    backfill_zone(zone, start, end)

print("\nDone.")