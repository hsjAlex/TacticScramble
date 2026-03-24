"""
Lichess Tactics & Rating Tracker
Team: hessische-schachjugend

Fetches for every team member:
  - Ratings: Bullet, Blitz, Rapid (and their average)
  - Puzzle rating, rating deviation, rating progress, total solved
  - Puzzle Storm best score, Puzzle Racer best score

Results are appended to data/tactics_history.csv daily.
Run via GitHub Actions (see .github/workflows/lichess_tracker.yml).
"""

import csv
import json
import os
import time
import datetime
import sys
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_KEY  = os.environ.get("LICHESS_API_KEY", "")
if not API_KEY:
    print("ERROR: LICHESS_API_KEY environment variable is not set.")
    sys.exit(1)

TEAM_ID  = "hessische-schachjugend"
BASE_URL = "https://lichess.org/api"
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}
OUT_FILE = "data/tactics_history.csv"

FIELDNAMES = [
    "date",
    "username",
    # Game ratings
    "bullet_rating",
    "blitz_rating",
    "rapid_rating",
    "avg_bullet_blitz_rapid",
    # Puzzle stats (all available from /api/user/<username>)
    "puzzle_rating",
    "puzzle_rating_deviation",   # rd: lower = more reliable rating
    "puzzle_rating_progress",    # prog: change over last 12 games (+/-)
    "puzzles_solved_total",      # games: all-time count
    # Bonus: best scores from timed puzzle modes
    "storm_best_score",          # Puzzle Storm: most puzzles in 3 min
    "racer_best_score",          # Puzzle Racer: best finish position
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_team_members(team_id: str) -> list:
    """Return a list of usernames for every member of the team."""
    url = f"{BASE_URL}/team/{team_id}/users"
    resp = requests.get(
        url,
        headers={**HEADERS, "Accept": "application/x-ndjson"},
        stream=True,
        timeout=60,
    )
    resp.raise_for_status()
    usernames = []
    for line in resp.iter_lines():
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        username = obj.get("username") or obj.get("id")
        if username:
            usernames.append(username)
        else:
            print(f"  [WARN] Skipping unexpected line shape: {list(obj.keys())}")
    print(f"Found {len(usernames)} members in team '{team_id}'.")
    return usernames


def get_user_data(username: str) -> dict:
    """Fetch public user data (ratings, puzzle stats)."""
    url = f"{BASE_URL}/user/{username}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code == 404:
        print(f"  [WARN] User '{username}' not found - skipping.")
        return {}
    resp.raise_for_status()
    return resp.json()


def safe_get(d: dict, *keys, default=None):
    """Safely traverse nested dicts."""
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
    today = datetime.date.today().isoformat()

    os.makedirs("data", exist_ok=True)
    file_exists = os.path.isfile(OUT_FILE) and os.path.getsize(OUT_FILE) > 0

    # Load usernames already recorded today to prevent duplicate rows
    already_recorded = set()
    if file_exists:
        with open(OUT_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("date") == today:
                    already_recorded.add(row["username"])
    if already_recorded:
        print(f"Skipping {len(already_recorded)} users already recorded today.")

    members = get_team_members(TEAM_ID)
    if not members:
        print("No members found - nothing to do.")
        sys.exit(0)

    rows = []
    for username in members:
        if username in already_recorded:
            print(f"  Skipping {username} (already recorded today).")
            continue
        print(f"  Processing {username} ...")
        user = get_user_data(username)
        if not user:
            continue

        perfs = user.get("perfs", {})

        bullet_r = safe_get(perfs, "bullet", "rating")
        blitz_r  = safe_get(perfs, "blitz",  "rating")
        rapid_r  = safe_get(perfs, "rapid",  "rating")

        available  = [r for r in [bullet_r, blitz_r, rapid_r] if r is not None]
        avg_rating = round(sum(available) / len(available), 1) if available else None

        puzzle      = perfs.get("puzzle", {})
        puzzle_r    = puzzle.get("rating")
        puzzle_rd   = puzzle.get("rd")
        puzzle_prog = puzzle.get("prog")
        puzzle_total= puzzle.get("games", 0)

        storm_score = safe_get(perfs, "storm", "score")
        racer_score = safe_get(perfs, "racer", "score")

        row = {
            "date":                    today,
            "username":                username,
            "bullet_rating":           bullet_r,
            "blitz_rating":            blitz_r,
            "rapid_rating":            rapid_r,
            "avg_bullet_blitz_rapid":  avg_rating,
            "puzzle_rating":           puzzle_r,
            "puzzle_rating_deviation": puzzle_rd,
            "puzzle_rating_progress":  puzzle_prog,
            "puzzles_solved_total":    puzzle_total,
            "storm_best_score":        storm_score,
            "racer_best_score":        racer_score,
        }
        rows.append(row)
        prog_str = f"{puzzle_prog:+d}" if puzzle_prog is not None else "?"
        print(
            f"    -> puzzle: {puzzle_r} (rd={puzzle_rd}, prog={prog_str}), "
            f"total solved: {puzzle_total}, storm: {storm_score}, racer: {racer_score}"
        )

        time.sleep(0.5)

    with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone. Wrote {len(rows)} rows to '{OUT_FILE}'.")


if __name__ == "__main__":
    main()
