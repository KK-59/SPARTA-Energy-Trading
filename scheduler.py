import schedule
import time
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv('api.env')
engine = create_engine(os.getenv('DB_URL'))

from ingest import fetch_and_store
from signalsnippets import (
    add_spreads, add_spread_statistics, add_arbitrage_candidate_flags,
    add_persistence_flags, add_best_opportunity, add_divergence,
    add_volatility, add_negative_price_flag, add_price_spike_flag,
    DEFAULT_ZONES, SPREAD_PAIRS, Z_SCORE_THRESHOLD
)
from run_signals import store_signals

zones = ['FR', 'DE_LU', 'NO_2', 'NL', 'BE']

def load_recent_prices(engine, zones, hours=48):
    zone_list = ','.join([repr(z) for z in zones])
    df = pd.read_sql(
        f"""
        SELECT timestamp, zone, price_eur_mwh 
        FROM prices 
        WHERE zone IN ({zone_list})
        AND timestamp >= NOW() - INTERVAL '{hours} hours'
        ORDER BY timestamp
        """,
        engine
    )
    return df.pivot(index='timestamp', columns='zone', values='price_eur_mwh')

def run_all():
    print(f"Running pipeline at {pd.Timestamp.now()}...")

    # step 1: ingest latest prices
    print("Ingesting prices...")
    for zone in zones:
        fetch_and_store(zone)

    # step 2: recompute signals on last 48 hours only
    print("Computing signals...")
    try:
        pivot = load_recent_prices(engine, DEFAULT_ZONES, hours=48)
        pivot = add_spreads(pivot)
        pivot = add_spread_statistics(pivot)
        pivot = add_arbitrage_candidate_flags(pivot)
        pivot = add_persistence_flags(pivot)
        pivot = add_best_opportunity(pivot)
        pivot = add_divergence(pivot, DEFAULT_ZONES)
        pivot = add_volatility(pivot, DEFAULT_ZONES)
        pivot = add_negative_price_flag(pivot, DEFAULT_ZONES)
        pivot = add_price_spike_flag(pivot, DEFAULT_ZONES)
        store_signals(pivot, engine)
    except Exception as e:
        print(f"Signal computation failed: {e}")

    print(f"Pipeline complete at {pd.Timestamp.now()}")

# run immediately on startup then every 15 minutes
run_all()
schedule.every(15).minutes.do(run_all)

while True:
    schedule.run_pending()
    time.sleep(15)