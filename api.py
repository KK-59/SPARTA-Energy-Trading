from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import json
import os
import math

load_dotenv()
engine = create_engine(os.getenv("DB_URL"))

ZONES = ["FR", "DE_LU", "NO_2", "NL", "BE"]

SPREAD_PAIRS_MAP = {
    "DE_LU_FR":   ["DE_LU", "FR"],
    "DE_LU_NL":   ["DE_LU", "NL"],
    "DE_LU_BE":   ["DE_LU", "BE"],
    "FR_BE":      ["FR",    "BE"],
    "FR_NL":      ["FR",    "NL"],
    "NO_2_DE_LU": ["NO_2",  "DE_LU"],
}


def pairs_for_zones(selected_zones):
    return [
        pair for pair, members in SPREAD_PAIRS_MAP.items()
        if any(z in selected_zones for z in members)
    ]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"[{self.address_string()}] {format % args}")

    def send_json(self, data, status=200):
        def clean(obj):
            if isinstance(obj, float) and math.isnan(obj):
                return None
            if isinstance(obj, dict):
                return {k: clean(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [clean(v) for v in obj]
            return obj
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.send_header("ngrok-skip-browser-warning", "true")
        self.end_headers()
        self.wfile.write(body)
        
        body = json.dumps(clean(data), default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        path = parsed.path

        try:
            if path == "/api/zones":
                self.send_json(ZONES)

            elif path == "/api/zone_stats":
                zones = params.get("zones", ZONES)
                with engine.connect() as conn:
                    rows = conn.execute(
                        text("""
                            SELECT DISTINCT ON (zone)
                                timestamp, zone, daily_min, daily_max, daily_range,
                                daily_avg_min_7d, daily_avg_max_7d, daily_avg_range_7d,
                                weekly_mean, weekly_std, wow_change,
                                price_momentum_4h, price_percentile_rank, peak_offpeak_ratio
                            FROM zone_stats
                            WHERE zone = ANY(:zones)
                            ORDER BY zone, timestamp DESC
                        """),
                        {"zones": zones},
                    ).mappings().all()
                self.send_json([dict(r) for r in rows])

            elif path == "/api/signals":
                zones = params.get("zones", ZONES)
                arb_only = params.get("arb_only", ["false"])[0].lower() == "true"
                limit = min(int(params.get("limit", [200])[0]), 500)
                relevant_pairs = pairs_for_zones(zones)
                if not relevant_pairs:
                    return self.send_json([])

                arb_clause = "AND arb_candidate = TRUE" if arb_only else ""
                with engine.connect() as conn:
                    rows = conn.execute(
                        text(f"""
                            SELECT
                                timestamp, spread_pair, spread_value, roll_mean, roll_std, z_score,
                                arb_candidate, persistent, trade_direction,
                                divergence, divergence_z,
                                negative_price_flag, price_spike_flag,
                                best_opportunity_pair, best_opportunity_z
                            FROM signals
                            WHERE timestamp >= NOW() - INTERVAL '48 hours'
                              AND spread_pair = ANY(:pairs)
                              {arb_clause}
                            ORDER BY timestamp DESC
                            LIMIT :limit
                        """),
                        {"pairs": relevant_pairs, "limit": limit},
                    ).mappings().all()
                self.send_json([dict(r) for r in rows])

            elif path == "/api/latest_prices":
                zones = params.get("zones", ZONES)
                with engine.connect() as conn:
                    rows = conn.execute(
                        text("""
                            SELECT DISTINCT ON (zone)
                                timestamp, zone, price_eur_mwh
                            FROM prices
                            WHERE zone = ANY(:zones)
                            ORDER BY zone, timestamp DESC
                        """),
                        {"zones": zones},
                    ).mappings().all()
                self.send_json([dict(r) for r in rows])

            elif path in ("/", "/dashboard"):
                here = os.path.dirname(os.path.abspath(__file__))
                html_path = os.path.join(here, "dashboard.html")
                with open(html_path, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", len(body))
                self.end_headers()
                self.wfile.write(body)

            else:
                self.send_json({"error": "not found"}, status=404)

        except Exception as e:
            self.send_json({"error": str(e)}, status=500)


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", 5050), Handler)
    print("API running at http://54.66.235.120:5050")
    server.serve_forever()
