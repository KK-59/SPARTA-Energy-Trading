#setup includes imports and config
import pandas as pd
from typing import Iterable

# CONFIG
DEFAULT_ZONES = ["DE_LU", "FR", "NL", "BE", "NO_2"]
SPREAD_PAIRS = [
    ("DE_LU", "FR"),
    ("DE_LU", "NL"),
    ("DE_LU", "BE"),
    ("FR", "BE"),
    ("FR", "NL"),
    ("NO_2", "DE_LU"),
]

ABS_SPREAD_THRESHOLD = 20.0
Z_SCORE_THRESHOLD = 2.0
MIN_PERSISTENCE_PERIODS = 2

ROLLING_WINDOW = 24
VOLATILITY_WINDOW = 24
SPIKE_WINDOW = 24

SPIKE_MULTIPLIER = 1.5
NEGATIVE_PRICE_THRESHOLD = 0.0

#spreads
def add_spreads(pivot: pd.DataFrame) -> pd.DataFrame:
    """
    Add spread columns for all configured spread pairs.
    Spread A_B = price(A) - price(B)
    """
    pivot = pivot.copy()

    for zone_a, zone_b in SPREAD_PAIRS:
        if {zone_a, zone_b}.issubset(pivot.columns):
            col_name = f"spread_{zone_a}_{zone_b}"
            pivot[col_name] = pivot[zone_a] - pivot[zone_b]

    return pivot

# rolling mean, rolling std, z-score

def add_spread_statistics(pivot: pd.DataFrame) -> pd.DataFrame:
    pivot = pivot.copy()
    spread_cols = [c for c in pivot.columns if c.startswith("spread_")]

    for col in spread_cols:
        mean_col = f"{col}_roll_mean"
        std_col = f"{col}_roll_std"
        z_col = f"{col}_z"

        # rolling mean

        pivot[mean_col] = pivot[col].rolling(
            window=ROLLING_WINDOW,
            min_periods=max(4, ROLLING_WINDOW // 4),
        ).mean()

        # rolling std
        
        pivot[std_col] = pivot[col].rolling(
            window=ROLLING_WINDOW,
            min_periods=max(4, ROLLING_WINDOW // 4),
        ).std()
        
        # handles divide by 0 error
        std_nonzero = pivot[std_col].replace(0, pd.NA)
        
        # z-scores
        pivot[z_col] = (pivot[col] - pivot[mean_col]) / std_nonzero

    return pivot

#arbitrage candidate flags
def add_arbitrage_candidate_flags(pivot: pd.DataFrame) -> pd.DataFrame:
    """
    Add candidate arbitrage flags for each spread.
    """
    pivot = pivot.copy()

    spread_cols = [
        c for c in pivot.columns
        if c.startswith("spread_")
        and not c.endswith("_roll_mean")
        and not c.endswith("_roll_std")
        and not c.endswith("_z")
    ]

    for col in spread_cols:
        z_col = f"{col}_z"
        abs_flag_col = f"{col}_abs_flag"
        z_flag_col = f"{col}_z_flag"
        candidate_col = f"{col}_arb_candidate"

        # 1. Raw spread size condition
        pivot[abs_flag_col] = pivot[col].abs() > ABS_SPREAD_THRESHOLD

        # 2. Statistical significance condition
        pivot[z_flag_col] = pivot[z_col].abs() > Z_SCORE_THRESHOLD

        # 3. Final arbitrage candidate = BOTH conditions satisfied
        pivot[candidate_col] = pivot[abs_flag_col] & pivot[z_flag_col]

    return pivot

#persistence filter
def add_persistence_flags(pivot: pd.DataFrame) -> pd.DataFrame:
    """
    For each arbitrage candidate column, add a persistence flag.
    """
    pivot = pivot.copy()
    candidate_cols = [c for c in pivot.columns if c.endswith("_arb_candidate")]

    for col in candidate_cols:
        persistence_col = f"{col}_persistent"

        pivot[persistence_col] = (
            pivot[col]
            .astype(int)
            .rolling(window=MIN_PERSISTENCE_PERIODS, min_periods=MIN_PERSISTENCE_PERIODS)
            .sum()
            >= MIN_PERSISTENCE_PERIODS
        )

    return pivot

#best opportunity per timestamp
def add_best_opportunity(pivot: pd.DataFrame) -> pd.DataFrame:
    pivot = pivot.copy()

    spread_cols = [
        c for c in pivot.columns
        if c.startswith("spread_")
        and not c.endswith("_roll_mean")
        and not c.endswith("_roll_std")
        and not c.endswith("_z")
    ]

    def best_pair_for_row(row):
        best_pair = None
        best_z = None

        for col in spread_cols:
            z_val = row.get(f"{col}_z")
            if pd.notna(z_val):
                if best_z is None or abs(z_val) > abs(best_z):
                    best_z = z_val
                    best_pair = col

        return best_pair

    pivot["best_opportunity_pair"] = pivot.apply(best_pair_for_row, axis=1)

    pivot["best_opportunity_spread"] = pivot.apply(
        lambda row: row[row["best_opportunity_pair"]]
        if pd.notna(row["best_opportunity_pair"]) else None,
        axis=1
    )

    pivot["best_opportunity_z"] = pivot.apply(
        lambda row: row[f"{row['best_opportunity_pair']}_z"]
        if pd.notna(row["best_opportunity_pair"]) else None,
        axis=1
    )

    return pivot

#trade direction
def get_trade_direction(spread_name: str, spread_value: float) -> str | None:
    """
    Convert a spread name and value into a trade direction string.

    For spread A_B = price(A) - price(B):
    - if spread > 0, A is more expensive, so buy B and sell A
    - if spread < 0, B is more expensive, so buy A and sell B
    """
    if pd.isna(spread_value):
        return None

    pair = spread_name.replace("spread_", "", 1)

    for zone_a, zone_b in SPREAD_PAIRS:
        expected = f"{zone_a}_{zone_b}"
        if pair == expected:
            if spread_value > 0:
                return f"Buy {zone_b}, Sell {zone_a}"
            elif spread_value < 0:
                return f"Buy {zone_a}, Sell {zone_b}"
            else:
                return "No directional edge"

    return None

#divergence
def add_divergence(pivot: pd.DataFrame, zones: Iterable[str]) -> pd.DataFrame:
    """
    Add market-wide divergence and its rolling statistics.
    """
    pivot = pivot.copy()
    available_zones = [z for z in zones if z in pivot.columns]

    if not available_zones:
        pivot["divergence"] = pd.NA
        pivot["divergence_roll_mean"] = pd.NA
        pivot["divergence_roll_std"] = pd.NA
        pivot["divergence_z"] = pd.NA
        return pivot

    pivot["divergence"] = (
        pivot[available_zones].max(axis=1)
        - pivot[available_zones].min(axis=1)
    )

    pivot["divergence_roll_mean"] = pivot["divergence"].rolling(
        window=ROLLING_WINDOW,
        min_periods=max(4, ROLLING_WINDOW // 4),
    ).mean()

    pivot["divergence_roll_std"] = pivot["divergence"].rolling(
        window=ROLLING_WINDOW,
        min_periods=max(4, ROLLING_WINDOW // 4),
    ).std()

    std_nonzero = pivot["divergence_roll_std"].replace(0, pd.NA)

    pivot["divergence_z"] = (
        pivot["divergence"] - pivot["divergence_roll_mean"]
    ) / std_nonzero

    return pivot

#volatility
def add_volatility(pivot: pd.DataFrame, zones: Iterable[str]) -> pd.DataFrame:
    """
    Add rolling volatility for each available zone.
    """
    pivot = pivot.copy()
    available_zones = [z for z in zones if z in pivot.columns]

    for zone in available_zones:
        pivot[f"{zone}_rolling_vol"] = pivot[zone].rolling(
            window=VOLATILITY_WINDOW,
            min_periods=max(4, VOLATILITY_WINDOW // 4),
        ).std()

    return pivot

#negative price flag
def add_negative_price_flag(pivot: pd.DataFrame, zones: Iterable[str]) -> pd.DataFrame:
    """
    Add a flag for negative price events.

    This detects if any zone has a price below zero at a given timestamp,
    which typically indicates oversupply or extreme market imbalance.
    """
    pivot = pivot.copy()
    available_zones = [z for z in zones if z in pivot.columns]

    if not available_zones:
        pivot["negative_price_flag"] = False
        return pivot

    pivot["negative_price_flag"] = (
        pivot[available_zones] < NEGATIVE_PRICE_THRESHOLD
    ).any(axis=1)

    return pivot

#price spike flag
def add_price_spike_flag(pivot: pd.DataFrame, zones: Iterable[str]) -> pd.DataFrame:
    """
    Add a flag for price spike events.

    This detects when prices in any zone rise significantly above their
    recent average, indicating potential market stress or supply shortages.
    """
    pivot = pivot.copy()
    available_zones = [z for z in zones if z in pivot.columns]

    if not available_zones:
        pivot["price_spike_flag"] = False
        return pivot

    # Compute rolling mean for each zone
    rolling_mean = pivot[available_zones].rolling(
        window=SPIKE_WINDOW,
        min_periods=max(4, SPIKE_WINDOW // 4),
    ).mean()

    # Detect spikes relative to rolling average
    pivot["price_spike_flag"] = (
        pivot[available_zones] > rolling_mean * SPIKE_MULTIPLIER
    ).any(axis=1)

    return pivot

#simple alert system
def print_active_signals(pivot: pd.DataFrame):
    signals = pivot[pivot["best_opportunity_z"].abs() > Z_SCORE_THRESHOLD]

    if not signals.empty:
        latest = signals.iloc[-1]

        print("\n SIGNAL DETECTED ")
        print(f"Time: {latest.name}")
        print(f"Pair: {latest['best_opportunity_pair']}")
        print(f"Z-score: {latest['best_opportunity_z']:.2f}")
        print(f"Direction: {latest['trade_direction']}")
