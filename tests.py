import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from sqlalchemy import create_engine
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.stattools import adfuller, acf

load_dotenv()
engine = create_engine(os.getenv('DATABASE_URL') or os.getenv('DB_URL'))

# ── CONFIG ────────────────────────────────────────────────────────────────────
PAIR = ('DE_LU', 'FR')  # start with just this one
CRISIS_END = '2023-01-01'  # pre/post crisis split

# ── LOAD ──────────────────────────────────────────────────────────────────────
def load_spread(engine, zone_a, zone_b):
    df = pd.read_sql(
        f"""
        SELECT timestamp, zone, price_eur_mwh
        FROM prices
        WHERE zone IN ('{zone_a}', '{zone_b}')
        ORDER BY timestamp
        """,
        engine
    )
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
    pivot = df.pivot(index='timestamp', columns='zone', values='price_eur_mwh')
    pivot['spread'] = pivot[zone_a] - pivot[zone_b]
    return pivot['spread'].dropna()

# ── ANALYSIS ──────────────────────────────────────────────────────────────────
def analyse_spread(spread, pair_name):
    print(f"\n{'='*60}")
    print(f"SPREAD ANALYSIS: {pair_name}")
    print(f"{'='*60}")
    print(f"Total periods:     {len(spread)}")
    print(f"Date range:        {spread.index[0].date()} to {spread.index[-1].date()}")
    print(f"Mean:              {spread.mean():.2f} EUR/MWh")
    print(f"Std:               {spread.std():.2f} EUR/MWh")
    print(f"Min:               {spread.min():.2f} EUR/MWh")
    print(f"Max:               {spread.max():.2f} EUR/MWh")
    print(f"% above €20:       {(spread > 20).mean():.1%}")
    print(f"% negative:        {(spread < 0).mean():.1%}")

    # pre/post crisis
    crisis_end = pd.Timestamp(CRISIS_END, tz='UTC')
    pre  = spread[spread.index < crisis_end]
    post = spread[spread.index >= crisis_end]
    print(f"\nPre-crisis  mean:  {pre.mean():.2f}  std: {pre.std():.2f}")
    print(f"Post-crisis mean:  {post.mean():.2f}  std: {post.std():.2f}")

    # augmented dickey-fuller — is the spread stationary or trending?
    adf_result = adfuller(spread.dropna())
    print(f"\nADF test p-value:  {adf_result[1]:.4f} "
          f"({'stationary' if adf_result[1] < 0.05 else 'non-stationary'})")

    # autocorrelation at key lags
    acf_vals = acf(spread.dropna(), nlags=96, fft=True)
    print(f"\nAutocorrelation:")
    print(f"  lag 1  (15min):  {acf_vals[1]:.3f}")
    print(f"  lag 4  (1h):     {acf_vals[4]:.3f}")
    print(f"  lag 16 (4h):     {acf_vals[16]:.3f}")
    print(f"  lag 96 (24h):    {acf_vals[96]:.3f}")

    # predictability at your target horizons
    print(f"\nPredictability (correlation with future spread):")
    for h, label in [(16, '4h ahead'), (96, 'day ahead')]:
        corr = spread.corr(spread.shift(-h))
        print(f"  {label}: {corr:.3f}")

    # time of day pattern
    hourly = spread.groupby(spread.index.hour).mean()
    print(f"\nHourly mean spread (top 3 hours):")
    for hour, val in hourly.nlargest(3).items():
        print(f"  {hour:02d}:00 → {val:.2f} EUR/MWh")

    return acf_vals

def plot_spread(spread, pair_name):
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))

    # time series
    axes[0].plot(spread.index, spread.values, linewidth=0.5, alpha=0.8)
    axes[0].axhline(20, color='red', linestyle='--', label='€20 threshold')
    axes[0].axhline(0, color='gray', linestyle='-', alpha=0.3)
    axes[0].axvline(pd.Timestamp(CRISIS_END, tz='UTC'), color='orange',
                    linestyle='--', label='Crisis end')
    axes[0].set_title(f'{pair_name} Spread (EUR/MWh)')
    axes[0].legend()

    # ACF
    plot_acf(spread.dropna(), lags=96, ax=axes[1], title='Autocorrelation (up to 24h)')

    # hourly distribution
    spread.groupby(spread.index.hour).mean().plot(
        kind='bar', ax=axes[2], title='Mean spread by hour of day'
    )
    axes[2].set_xlabel('Hour')
    axes[2].set_ylabel('Mean spread (EUR/MWh)')

    plt.tight_layout()
    plt.savefig(f'spread_analysis_{pair_name.replace("/", "_")}.png', dpi=150)
    print(f"\nPlot saved to spread_analysis_{pair_name.replace('/', '_')}.png")

# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    zone_a, zone_b = PAIR
    pair_name = f"{zone_a}/{zone_b}"

    print(f"Loading {pair_name} spread...")
    spread = load_spread(engine, zone_a, zone_b)

    acf_vals = analyse_spread(spread, pair_name)
    plot_spread(spread, pair_name)