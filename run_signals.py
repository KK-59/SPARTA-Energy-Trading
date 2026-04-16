from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pandas as pd
import os
from signalsnippets import (
    add_spreads, add_spread_statistics, add_arbitrage_candidate_flags,
    add_persistence_flags, add_best_opportunity, get_trade_direction,
    add_divergence, add_volatility, add_negative_price_flag,
    add_price_spike_flag, DEFAULT_ZONES, SPREAD_PAIRS, Z_SCORE_THRESHOLD
)

load_dotenv()
engine = create_engine(os.getenv('DB_URL'))

def load_prices(engine, zones):
    zone_list = ','.join([repr(z) for z in zones])
    df = pd.read_sql(
        f"SELECT timestamp, zone, price_eur_mwh FROM prices WHERE zone IN ({zone_list}) ORDER BY timestamp",
        engine
    )
    return df.pivot(index='timestamp', columns='zone', values='price_eur_mwh')

def store_signals(pivot, engine):
    spread_cols = [
        c for c in pivot.columns
        if c.startswith("spread_")
        and not c.endswith(("_roll_mean", "_roll_std", "_z", "_abs_flag", "_z_flag", "_arb_candidate", "_arb_candidate_persistent"))
    ]
    rows = []
    for ts, row in pivot.iterrows():
        for col in spread_cols:
            pair = col.replace("spread_", "", 1)
            spread_val = row.get(col)
            rows.append({
                'timestamp': ts,
                'spread_pair': pair,
                'spread_value': spread_val,
                'roll_mean': row.get(f"{col}_roll_mean"),
                'roll_std': row.get(f"{col}_roll_std"),
                'z_score': row.get(f"{col}_z"),
                'arb_candidate': bool(row.get(f"{col}_arb_candidate")),
                'persistent': bool(row.get(f"{col}_arb_candidate_persistent")),
                'trade_direction': get_trade_direction(col, spread_val),
                'divergence': row.get('divergence'),
                'divergence_z': row.get('divergence_z'),
                'negative_price_flag': bool(row.get('negative_price_flag')),
                'price_spike_flag': bool(row.get('price_spike_flag')),
                'best_opportunity_pair': row.get('best_opportunity_pair'),
                'best_opportunity_z': row.get('best_opportunity_z'),
            })
    df_out = pd.DataFrame(rows)
    with engine.connect() as conn:
        for _, r in df_out.iterrows():
            conn.execute(text("""
                INSERT INTO signals (
                    timestamp, spread_pair, spread_value, roll_mean, roll_std,
                    z_score, arb_candidate, persistent, trade_direction,
                    divergence, divergence_z, negative_price_flag,
                    price_spike_flag, best_opportunity_pair, best_opportunity_z
                ) VALUES (
                    :timestamp, :spread_pair, :spread_value, :roll_mean, :roll_std,
                    :z_score, :arb_candidate, :persistent, :trade_direction,
                    :divergence, :divergence_z, :negative_price_flag,
                    :price_spike_flag, :best_opportunity_pair, :best_opportunity_z
                ) ON CONFLICT (timestamp, spread_pair) DO NOTHING;
            """), r.to_dict())
        conn.commit()
    print(f"Stored {len(df_out)} signal rows")

if __name__ == "__main__":
    print("Loading prices...")
    pivot = load_prices(engine, DEFAULT_ZONES)

    print("Computing signals...")
    pivot = add_spreads(pivot)
    pivot = add_spread_statistics(pivot)
    pivot = add_arbitrage_candidate_flags(pivot)
    pivot = add_persistence_flags(pivot)
    pivot = add_best_opportunity(pivot)
    pivot = add_divergence(pivot, DEFAULT_ZONES)
    pivot = add_volatility(pivot, DEFAULT_ZONES)
    pivot = add_negative_price_flag(pivot, DEFAULT_ZONES)
    pivot = add_price_spike_flag(pivot, DEFAULT_ZONES)

    print("Storing signals...")
    store_signals(pivot, engine)
    print("Done.")