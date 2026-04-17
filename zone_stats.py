import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv('DB_URL'))

ZONES = ['FR', 'DE_LU', 'NO_2', 'NL', 'BE']
PEAK_HOURS = range(8, 20)  # 08:00 - 20:00

# ── SETUP ─────────────────────────────────────────────────────────────────────
def setup_zone_stats_table(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS zone_stats (
                timestamp TIMESTAMPTZ NOT NULL,
                zone TEXT NOT NULL,
                daily_min DOUBLE PRECISION,
                daily_max DOUBLE PRECISION,
                daily_range DOUBLE PRECISION,
                daily_avg_min_7d DOUBLE PRECISION,
                daily_avg_max_7d DOUBLE PRECISION,
                daily_avg_range_7d DOUBLE PRECISION,
                weekly_mean DOUBLE PRECISION,
                weekly_std DOUBLE PRECISION,
                wow_change DOUBLE PRECISION,
                price_momentum_4h DOUBLE PRECISION,
                price_percentile_rank DOUBLE PRECISION,
                peak_offpeak_ratio DOUBLE PRECISION,
                PRIMARY KEY (timestamp, zone)
            );
        """))
        conn.commit()
        print("zone_stats table created.")

# ── LOAD ──────────────────────────────────────────────────────────────────────
def load_prices(engine, zones):
    zone_list = ','.join([repr(z) for z in zones])
    df = pd.read_sql(
        f"""
        SELECT timestamp, zone, price_eur_mwh
        FROM prices
        WHERE zone IN ({zone_list})
        ORDER BY timestamp
        """,
        engine
    )
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    return df

# ── COMPUTE ───────────────────────────────────────────────────────────────────
def compute_zone_stats(df, zone):
    s = df[df['zone'] == zone].set_index('timestamp')['price_eur_mwh'].sort_index()

    if s.empty:
        return pd.DataFrame()

    stats = pd.DataFrame(index=s.index)

    # ── daily min / max / range ───────────────────────────────────────────────
    # resample to daily, then forward-fill back to 15-min resolution
    daily_min = s.resample('D').min().reindex(s.index, method='ffill')
    daily_max = s.resample('D').max().reindex(s.index, method='ffill')
    daily_range = daily_max - daily_min

    stats['daily_min']   = daily_min
    stats['daily_max']   = daily_max
    stats['daily_range'] = daily_range

    # ── rolling 7-day average of daily min / max / range ─────────────────────
    # compute on daily series first, then reindex
    daily_avg_min_7d = (
        s.resample('D').min()
        .rolling(window=7, min_periods=1).mean()
        .reindex(s.index, method='ffill')
    )
    daily_avg_max_7d = (
        s.resample('D').max()
        .rolling(window=7, min_periods=1).mean()
        .reindex(s.index, method='ffill')
    )
    daily_avg_range_7d = (
        s.resample('D').apply(lambda x: x.max() - x.min())
        .rolling(window=7, min_periods=1).mean()
        .reindex(s.index, method='ffill')
    )

    stats['daily_avg_min_7d']   = daily_avg_min_7d
    stats['daily_avg_max_7d']   = daily_avg_max_7d
    stats['daily_avg_range_7d'] = daily_avg_range_7d

    # ── weekly mean / std / week-over-week change ─────────────────────────────
    weekly_mean = (
        s.resample('W').mean()
        .reindex(s.index, method='ffill')
    )
    weekly_std = (
        s.resample('W').std()
        .reindex(s.index, method='ffill')
    )
    weekly_mean_shifted = (
        s.resample('W').mean()
        .shift(1)
        .reindex(s.index, method='ffill')
    )
    wow_change = weekly_mean - weekly_mean_shifted

    stats['weekly_mean'] = weekly_mean
    stats['weekly_std']  = weekly_std
    stats['wow_change']  = wow_change

    # ── 4-hour price momentum ─────────────────────────────────────────────────
    # 4h = 16 × 15-min periods
    stats['price_momentum_4h'] = s - s.shift(16)

    # ── price percentile rank over last 24h ───────────────────────────────────
    # rolling 96-period window (96 × 15min = 24h)
    def percentile_rank(window):
        if len(window) < 2:
            return np.nan
        return (window < window.iloc[-1]).mean() * 100

    stats['price_percentile_rank'] = (
        s.rolling(window=96, min_periods=4)
        .apply(percentile_rank, raw=False)
    )

    # ── peak / off-peak ratio (rolling 7-day) ─────────────────────────────────
    is_peak = s.index.hour.isin(PEAK_HOURS)
    peak_series    = s.where(is_peak)
    offpeak_series = s.where(~is_peak)

    rolling_peak_mean    = peak_series.rolling(window=96*7, min_periods=48).mean()
    rolling_offpeak_mean = offpeak_series.rolling(window=96*7, min_periods=48).mean()

    # avoid division by zero
    stats['peak_offpeak_ratio'] = rolling_peak_mean / rolling_offpeak_mean.replace(0, np.nan)

    stats['zone'] = zone
    stats = stats.reset_index().rename(columns={'timestamp': 'timestamp'})
    return stats

# ── STORE ─────────────────────────────────────────────────────────────────────
def store_zone_stats(df, engine):
    with engine.connect() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO zone_stats (
                    timestamp, zone, daily_min, daily_max, daily_range,
                    daily_avg_min_7d, daily_avg_max_7d, daily_avg_range_7d,
                    weekly_mean, weekly_std, wow_change,
                    price_momentum_4h, price_percentile_rank, peak_offpeak_ratio
                ) VALUES (
                    :timestamp, :zone, :daily_min, :daily_max, :daily_range,
                    :daily_avg_min_7d, :daily_avg_max_7d, :daily_avg_range_7d,
                    :weekly_mean, :weekly_std, :wow_change,
                    :price_momentum_4h, :price_percentile_rank, :peak_offpeak_ratio
                ) ON CONFLICT (timestamp, zone) DO NOTHING;
            """), row.to_dict())
        conn.commit()

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Setting up table...")
    setup_zone_stats_table(engine)

    print("Loading prices...")
    df = load_prices(engine, ZONES)

    for zone in ZONES:
        print(f"Computing stats for {zone}...")
        stats = compute_zone_stats(df, zone)
        if stats.empty:
            print(f"  No data for {zone}, skipping")
            continue
        print(f"  Storing {len(stats)} rows...")
        store_zone_stats(stats, engine)
        print(f"  Done.")

    print("\nAll zone stats computed and stored.")