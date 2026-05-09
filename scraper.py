# scraper.py
# scrapes Basketball-Reference for per-game stats, advanced stats,
# award winners, and team records from 2000 to 2026.
# saves everything as CSVs in the /data folder.
#
# usage:
#   pip install requests beautifulsoup4 pandas
#   python scraper.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

# config

START_YEAR = 2000
END_YEAR   = 2026
DELAY      = 3          # seconds between requests to avoid getting blocked
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


def parse_table(soup, table_id):
    table = soup.find("table", {"id": table_id})
    if not table:
        return None

    for tag in table.find_all("tr", class_="thead"):
        tag.decompose()

    rows = []
    headers = [th.get_text(strip=True) for th in table.find("thead").find_all("th")]

    for tr in table.find("tbody").find_all("tr"):
        if tr.get("class") and "partial_table" not in tr.get("class", []):
            if "thead" in tr.get("class", []):
                continue
        cells = [td.get_text(strip=True) for td in tr.find_all(["th", "td"])]
        if cells:
            rows.append(cells)

    if not rows:
        return None2

    df = pd.DataFrame(rows, columns=headers[:len(rows[0])] if headers else None)
    return df


def scrape_per_game(year):
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html"
    print(f"  per-game: {year}")
    soup = fetch(url)
    if not soup:
        return None
    time.sleep(DELAY)

    df = parse_table(soup, "per_game")
    if df is None:
        df = parse_table(soup, "per_game_stats")
    if df is None:
        return None

    df["season"] = year
    df = df[df["Player"].notna() & (df["Player"] != "Player") & (df["Player"] != "")]
    if "Tm" in df.columns:
        df = df[~((df.duplicated(subset=["Player", "season"], keep=False)) & (df["Tm"] != "TOT"))]
    return df


def scrape_advanced(year):
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_advanced.html"
    print(f"  advanced: {year}")
    soup = fetch(url)
    if not soup:
        return None
    time.sleep(DELAY)

    df = parse_table(soup, "advanced")
    if df is None:
        df = parse_table(soup, "advanced_stats")
    if df is None:
        return None

    df["season"] = year
    df = df[df["Player"].notna() & (df["Player"] != "Player") & (df["Player"] != "")]
    if "Tm" in df.columns:
        df = df[~((df.duplicated(subset=["Player", "season"], keep=False)) & (df["Tm"] != "TOT"))]
    return df


def scrape_awards(year):
    url = f"https://www.basketball-reference.com/awards/awards_{year}.html"
    print(f"  awards: {year}")
    soup = fetch(url)
    if not soup:
        return None
    time.sleep(DELAY)

    winners = {"season": year}
    award_table_map = {
        "mvp":  "mvp",
        "dpoy": "dpoy",
        "roy":  "roy",
        "smoy": "smoy",
        "mip":  "mip",
    }

    for award_key, table_id in award_table_map.items():
        table = soup.find("table", {"id": table_id})
        if not table:
            winners[award_key] = None
            continue
        first_row = table.find("tbody").find("tr")
        if not first_row:
            winners[award_key] = None
            continue
        player_cell = first_row.find("td", {"data-stat": "player"})
        winners[award_key] = player_cell.get_text(strip=True) if player_cell else None

    return winners


def scrape_team_records(year):
    url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"
    print(f"  team records: {year}")
    soup = fetch(url)
    if not soup:
        return None
    time.sleep(DELAY)

    rows = []
    seen = set()
    for table_id in ["confs_standings_E", "confs_standings_W", "divs_standings_E", "divs_standings_W"]:
        table = soup.find("table", {"id": table_id})
        if not table:
            continue
        for tr in table.find("tbody").find_all("tr"):
            if tr.get("class") and "full_table" not in tr.get("class", []):
                continue
            team_cell   = tr.find(["th", "td"], {"data-stat": "team_name"})
            wins_cell   = tr.find("td", {"data-stat": "wins"})
            losses_cell = tr.find("td", {"data-stat": "losses"})
            pct_cell    = tr.find("td", {"data-stat": "win_loss_pct"})
            if not (team_cell and wins_cell and losses_cell):
                continue
            link = team_cell.find("a")
            abbr = link["href"].split("/")[2] if link else None
            team_name = team_cell.get_text(strip=True).replace("*", "")
            if team_name in seen:
                continue
            seen.add(team_name)
            rows.append({
                "Team":         team_name,
                "W":            wins_cell.get_text(strip=True),
                "L":            losses_cell.get_text(strip=True),
                "W/L%":         pct_cell.get_text(strip=True) if pct_cell else None,
                "season":       year,
                "Abbreviation": abbr,
            })

    if not rows:
        print(f"  warning: no team records found for {year}")
        return None

    return pd.DataFrame(rows)



if __name__ == "__main__":
    all_per_game = []
    all_advanced = []
    all_awards   = []
    all_records  = []

    seasons = range(START_YEAR, END_YEAR + 1)
    total = len(list(seasons))

    for i, year in enumerate(seasons, 1):
        print(f"\n[{i}/{total}] season {year}")

        pg = scrape_per_game(year)
        if pg is not None:
            all_per_game.append(pg)

        adv = scrape_advanced(year)
        if adv is not None:
            all_advanced.append(adv)

        if year < 2026:
            aw = scrape_awards(year)
            if aw:
                all_awards.append(aw)

        rec = scrape_team_records(year)
        if rec is not None:
            all_records.append(rec)

    print("\nsaving data...")

    if all_per_game:
        df_pg = pd.concat(all_per_game, ignore_index=True)
        df_pg.to_csv(f"{DATA_DIR}/per_game_stats.csv", index=False)
        print(f"  saved per_game_stats.csv ({len(df_pg):,} rows)")

    if all_advanced:
        df_adv = pd.concat(all_advanced, ignore_index=True)
        df_adv.to_csv(f"{DATA_DIR}/advanced_stats.csv", index=False)
        print(f"  saved advanced_stats.csv ({len(df_adv):,} rows)")

    if all_awards:
        df_aw = pd.DataFrame(all_awards)
        df_aw.to_csv(f"{DATA_DIR}/award_winners.csv", index=False)
        print(f"  saved award_winners.csv ({len(df_aw):,} rows)")

    if all_records:
        df_rec = pd.concat(all_records, ignore_index=True)
        df_rec.to_csv(f"{DATA_DIR}/team_records.csv", index=False)
        print(f"  saved team_records.csv ({len(df_rec):,} rows)")

    print("\ndone. run train.py next.")