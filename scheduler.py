import schedule
import time
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL') or os.getenv('DB_URL'))

from ingest import fetch_and_store
from signalsnippets import (
    add_spreads, add_spread_statistics, add_arbitrage_candidate_flags,
    add_persistence_flags, add_best_opportunity, add_divergence,
    add_volatility, add_negative_price_flag, add_price_spike_flag,
    DEFAULT_ZONES, SPREAD_PAIRS, Z_SCORE_THRESHOLD
)
from run_signals import store_signals
from zone_stats import compute_zone_stats, store_zone_stats, load_prices as load_prices_for_stats
from setup_db import setup_prices_table, setup_signals_table
from zone_stats import setup_zone_stats_table

ZONES = ['FR', 'DE_LU', 'NO_2', 'NL', 'BE']

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
    for zone in ZONES:
        fetch_and_store(zone)

    # step 2: recompute signals on last 48 hours
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
        import traceback
        print(f"Signal computation failed: {e}")
        traceback.print_exc()

    # step 3: recompute zone stats on last 48 hours
    print("Computing zone stats...")
    try:
        df = load_prices_for_stats(engine, ZONES)
        # only last 48 hours for efficiency
        cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=48)
        df = df[df['timestamp'] >= cutoff]
        for zone in ZONES:
            stats = compute_zone_stats(df, zone)
            if not stats.empty:
                store_zone_stats(stats, engine)
    except Exception as e:
        import traceback
        print(f"Zone stats computation failed: {e}")
        traceback.print_exc()

    print(f"Pipeline complete at {pd.Timestamp.now()}")

# ── STARTUP ───────────────────────────────────────────────────────────────────
print("Setting up tables...")
setup_prices_table(engine)
setup_signals_table(engine)
setup_zone_stats_table(engine)

# run immediately then every 15 minutes
run_all()
schedule.every(15).minutes.do(run_all)

while True:
    schedule.run_pending()
    time.sleep(60)