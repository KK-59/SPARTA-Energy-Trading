from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import os

load_dotenv()
engine = create_engine(os.getenv('DB_URL'))

def calculate_spreads(zone_a, zone_b):
    query = """
        SELECT timestamp, zone, price_eur_mwh
        FROM prices
        WHERE zone IN :zones
        ORDER BY timestamp
    """
    df = pd.read_sql(
    f"SELECT timestamp, zone, price_eur_mwh FROM prices WHERE zone IN ('{zone_a}', '{zone_b}') ORDER BY timestamp",
    engine
)
    
    # pivot so each zone is a column
    df_pivot = df.pivot(index='timestamp', columns='zone', values='price_eur_mwh')
    
    # calculate spread
    spread_col = f"{zone_a}_minus_{zone_b}"
    df_pivot[spread_col] = df_pivot[zone_a] - df_pivot[zone_b]
    
    return df_pivot

spreads = calculate_spreads('DE_LU', 'FR')
print(spreads.tail(20))