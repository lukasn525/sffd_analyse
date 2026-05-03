"""
Downloads SFFD Fire Calls for Service from SF Open Data (Socrata API)
and saves the result as a Parquet file in data/raw/.

Run once before basic_stats.py:
    .\\venv\\Scripts\\python.exe scripts\\download_data.py
"""

import requests
import pandas as pd
from pathlib import Path

BASE_URL = "https://data.sfgov.org/resource/wr8u-xric.json"
LIMIT = 50_000
OUT_PATH = Path(__file__).parent.parent / "data" / "raw" / "fire_incidents.parquet"


def download() -> None:
    session = requests.Session()
    all_data = []
    offset = 0

    print("Downloading SFFD Fire Calls for Service …")

    while True:
        response = session.get(
            BASE_URL,
            params={"$limit": LIMIT, "$offset": offset, "$order": ":id"},
            timeout=60,
        )
        response.raise_for_status()
        batch = response.json()

        if not batch:
            break

        all_data.extend(batch)
        offset += LIMIT
        print(f"  fetched {len(all_data):>9,} rows …")

    print(f"\nTotal rows downloaded: {len(all_data):,}")

    df = pd.DataFrame(all_data)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"Saved to: {OUT_PATH}")


if __name__ == "__main__":
    download()
