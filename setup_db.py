from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import os

load_dotenv('api.env')
engine = create_engine(os.getenv('DB_URL'))

def setup_signals_table(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS signals (
                timestamp TIMESTAMPTZ NOT NULL,
                spread_pair TEXT NOT NULL,
                spread_value DOUBLE PRECISION,
                roll_mean DOUBLE PRECISION,
                roll_std DOUBLE PRECISION,
                z_score DOUBLE PRECISION,
                arb_candidate BOOLEAN,
                persistent BOOLEAN,
                trade_direction TEXT,
                divergence DOUBLE PRECISION,
                divergence_z DOUBLE PRECISION,
                negative_price_flag BOOLEAN,
                price_spike_flag BOOLEAN,
                best_opportunity_pair TEXT,
                best_opportunity_z DOUBLE PRECISION,
                PRIMARY KEY (timestamp, spread_pair)
            );
        """))
        conn.commit()
        print("Signals table created.")

setup_signals_table(engine)