import csv
import json
import os
import time
import datetime
import sys
from typing import Optional

import requests

API_KEY  = os.environ.get("LICHESS_API_KEY", "")
if not API_KEY:
    print("ERROR: LICHESS_API_KEY not set.")
    sys.exit(1)

TEAM_ID  = "taktikspektakel"
DRY_RUN  = os.environ.get("DRY_RUN", "false").lower() == "true"
BASE_URL = "https://lichess.org/api"
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}
OUT_FILE = "data/tactics_history.csv"

FIELDNAMES = [
    "timestamp",
    "username",
    "bullet_rating",
    "blitz_rating",
    "rapid_rating",
    "avg_bullet_blitz_rapid",
    "puzzle_rating",
    "puzzle_rating_deviation",
    "puzzle_rating_progress",
    "puzzles_solved_total",
    "storm_best_score",
    "racer_best_score",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_with_retry(url, headers, method="GET", data=None, retries=3):
    for attempt in range(retries):
        try:
            if method == "POST":
                resp = requests.post(url, headers=headers, data=data, timeout=30)
            else:
                resp = requests.get(url, headers=headers, timeout=15)

            if resp.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
                continue

            resp.raise_for_status()
            return resp

        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))


def get_team_members(team_id: str) -> list:
    url = f"{BASE_URL}/team/{team_id}/users"
    resp = fetch_with_retry(
        url,
        {**HEADERS, "Accept": "application/x-ndjson"}
    )

    usernames = []
    for line in resp.iter_lines():
        if not line:
            continue
        try:
            obj = json.loads(line)
            username = obj.get("username") or obj.get("id")
            if username:
                usernames.append(username)
        except:
            continue

    print(f"[INFO] Found {len(usernames)} members.")
    return usernames



def safe_get(d: dict, *keys, default=None):
    for key in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(key, default)
        if d is None:
            return default
    return d


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    now = datetime.datetime.now(datetime.timezone.utc)
    timestamp = now.strftime("%Y-%m-%d %H:%M UTC")
    current_hour = now.strftime("%Y-%m-%d %H")

    os.makedirs("data", exist_ok=True)
    file_exists = os.path.isfile(OUT_FILE) and os.path.getsize(OUT_FILE) > 0

    already_recorded = set()
    last_totals = {}

    if file_exists:
        with open(OUT_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                row_hour = row.get("timestamp", "")[:13]
                if row_hour == current_hour:
                    already_recorded.add(row["username"].lower())
                last_totals[row["username"]] = row.get("puzzles_solved_total")

    members = get_team_members(TEAM_ID)

    rows = []

    def process_user(user):
        """Extract a row dict from a Lichess user object."""
        username = user.get("username")
        if not username or username in already_recorded:
            return None

        perfs = user.get("perfs", {})

        bullet = safe_get(perfs, "bullet", "rating")
        blitz  = safe_get(perfs, "blitz",  "rating")
        rapid  = safe_get(perfs, "rapid",  "rating")

        available = [r for r in [bullet, blitz, rapid] if r is not None]
        avg = round(sum(available) / len(available), 1) if available else None

        puzzle      = perfs.get("puzzle", {})
        puzzle_r    = puzzle.get("rating")
        puzzle_rd   = puzzle.get("rd")
        puzzle_prog = puzzle.get("prog")
        puzzle_total= puzzle.get("games")

        storm = safe_get(perfs, "storm", "score")
        racer = safe_get(perfs, "racer", "score")

        return {
            "timestamp":               timestamp,
            "username":                username,
            "bullet_rating":           bullet,
            "blitz_rating":            blitz,
            "rapid_rating":            rapid,
            "avg_bullet_blitz_rapid":  avg,
            "puzzle_rating":           puzzle_r,
            "puzzle_rating_deviation": puzzle_rd,
            "puzzle_rating_progress":  puzzle_prog,
            "puzzles_solved_total":    puzzle_total,
            "storm_best_score":        storm,
            "racer_best_score":        racer,
        }

    # ── Bulk fetch (max 300 per request) ────────────────────────────────────
    # Einzelabruf für jeden Spieler (zuverlässiger als Bulk-API)
    to_fetch = [m for m in members if m.lower() not in already_recorded]
    print(f"[INFO] Rufe {len(to_fetch)} Spieler ab ({len(already_recorded)} bereits diese Stunde erfasst)...")
    for username in to_fetch:
        try:
            resp = fetch_with_retry(f"{BASE_URL}/user/{username}", HEADERS)
            row  = process_user(resp.json())
            if row:
                rows.append(row)
                print(f"[OK]   {row['username']}: puzzles={row['puzzles_solved_total']}, rating={row['puzzle_rating']}")
            time.sleep(0.3)
        except Exception as e:
            print(f"[WARN] Konnte {username} nicht abrufen: {e}")

    # ── Write results ────────────────────────────────────────────────────────
    if rows:
        if DRY_RUN:
            print(f"\n[DRY RUN] Würde {len(rows)} Zeilen schreiben — kein Schreibzugriff.")
            print(f"{'USERNAME':<25} {'AUFGABEN':>10} {'WERTUNG':>8}")
            print("-" * 45)
            for r in rows:
                print(f"{r['username']:<25} {str(r['puzzles_solved_total']):>10} {str(r['puzzle_rating']):>8}")
        else:
            with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
                if not file_exists:
                    writer.writeheader()
                writer.writerows(rows)

    skipped = len(already_recorded)
    mode = "[DRY RUN] " if DRY_RUN else ""
    print(f"\n{mode}Fertig. {len(rows)} Zeilen verarbeitet, {skipped} übersprungen.")


if __name__ == "__main__":
    main()
