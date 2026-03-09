#scrpaing only team record bc not working in original fie

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

START_YEAR = 2000
END_YEAR   = 2026
DELAY      = 3
DATA_DIR   = "data"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

os.makedirs(DATA_DIR, exist_ok=True)


def fetch(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, "html.parser")
        else:
            print(f"  HTTP {resp.status_code} for {url}")
            return None
    except Exception as e:
        print(f"  error fetching {url}: {e}")
        return None


def scrape_team_records(year):
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"
    print(f"  team records: {year}")
    soup = fetch(url)
    if not soup:
        return None
    time.sleep(DELAY)

        # debug: print all table ids on the page
    all_tables = soup.find_all("table")
    print(f"  tables found: {[t.get('id') for t in all_tables]}")

    rows = []
    for table_id in ["confs_standings_E", "confs_standings_W", "divs_standings_E", "divs_standings_W"]:
        table = soup.find("table", {"id": table_id})
        if not table:
            continue
        for tr in table.find("tbody").find_all("tr"):
            team_cell   = tr.find(["th", "td"], {"data-stat": "team_name"})
            wins_cell   = tr.find("td", {"data-stat": "wins"})
            losses_cell = tr.find("td", {"data-stat": "losses"})
            pct_cell    = tr.find("td", {"data-stat": "win_loss_pct"})
            if team_cell and wins_cell and losses_cell:
                rows.append({
                    "Team":   team_cell.get_text(strip=True).replace("*", ""),
                    "W":      wins_cell.get_text(strip=True),
                    "L":      losses_cell.get_text(strip=True),
                    "W/L%":   pct_cell.get_text(strip=True) if pct_cell else None,
                    "season": year,
                })

    all_tables = soup.find_all("table")
    print(f"  tables found: {[t.get('id') for t in all_tables]}")

    if not rows:
        print(f"  warning: no team records found for {year}")
        return None

    return pd.DataFrame(rows)


if __name__ == "__main__":
    all_records = []
    seasons = range(START_YEAR, END_YEAR + 1)
    total = len(list(seasons))

    for i, year in enumerate(seasons, 1):
        print(f"[{i}/{total}] season {year}")
        rec = scrape_team_records(year)
        if rec is not None:
            all_records.append(rec)

    if all_records:
        df = pd.concat(all_records, ignore_index=True)
        df.to_csv(f"{DATA_DIR}/team_records.csv", index=False)
        print(f"\nsaved team_records.csv ({len(df):,} rows)")
    else:
        print("\nnothing saved -- check table IDs")