"""
Join-Detection für TaktikSpektakel
====================================
Läuft alle 10 Minuten via GitHub Actions (siehe lichess_tracker.yml).

Was dieser Script macht:
  1. Holt alle aktuellen Team-Mitglieder von Lichess
  2. Vergleicht mit den bereits bekannten Spielern in tactics_history.csv
  3. Für jeden NEUEN Spieler: sofort Stats abrufen und als Baseline speichern

Dadurch beginnt das Tracking eines neuen Mitglieds innerhalb von max. 10 Minuten
nach dem Beitritt — der erste Snapshot ist direkt eine gültige Startbasis.

Leichtgewichtig: ruft nur neue User ab, nicht alle.
"""

import csv
import json
import os
import sys
import time
import datetime
from typing import Optional

import requests

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY  = os.environ.get("LICHESS_API_KEY", "")
if not API_KEY:
    print("ERROR: LICHESS_API_KEY nicht gesetzt.")
    sys.exit(1)

TEAM_ID  = "taktikspektakel"
BASE_URL = "https://lichess.org/api"
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}
OUT_FILE = "data/tactics_history.csv"

FIELDNAMES = [
    "timestamp", "username",
    "bullet_rating", "blitz_rating", "rapid_rating", "avg_bullet_blitz_rapid",
    "puzzle_rating", "puzzle_rating_deviation", "puzzle_rating_progress",
    "puzzles_solved_total", "storm_best_score", "racer_best_score",
]

# ── Helpers ─────────────────────────────────────────────────────────────────

def fetch_with_retry(url, headers, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"  [429] Rate limit — warte {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def get_team_members() -> set:
    url  = f"{BASE_URL}/team/{TEAM_ID}/users"
    resp = fetch_with_retry(url, {**HEADERS, "Accept": "application/x-ndjson"})
    members = set()
    for line in resp.iter_lines():
        if not line:
            continue
        try:
            obj      = json.loads(line)
            username = obj.get("username") or obj.get("id")
            if username:
                members.add(username.lower())  # normalisiert für Vergleich
        except json.JSONDecodeError:
            continue
    return members


def get_known_members() -> set:
    """Gibt alle bekannten Usernamen in Kleinbuchstaben zurück (case-insensitive)."""
    if not os.path.isfile(OUT_FILE):
        return set()
    known = set()
    with open(OUT_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("username"):
                known.add(row["username"].lower())
    return known


def safe_get(d, *keys, default=None):
    for key in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(key, default)
        if d is None:
            return default
    return d


def build_row(user: dict, timestamp: str) -> Optional[dict]:
    username = user.get("username")
    if not username:
        return None
    perfs    = user.get("perfs", {})
    bullet   = safe_get(perfs, "bullet", "rating")
    blitz    = safe_get(perfs, "blitz",  "rating")
    rapid    = safe_get(perfs, "rapid",  "rating")
    available = [r for r in [bullet, blitz, rapid] if r is not None]
    avg      = round(sum(available) / len(available), 1) if available else None
    puzzle   = perfs.get("puzzle", {})
    return {
        "timestamp":               timestamp,
        "username":                username,
        "bullet_rating":           bullet,
        "blitz_rating":            blitz,
        "rapid_rating":            rapid,
        "avg_bullet_blitz_rapid":  avg,
        "puzzle_rating":           puzzle.get("rating"),
        "puzzle_rating_deviation": puzzle.get("rd"),
        "puzzle_rating_progress":  puzzle.get("prog"),
        "puzzles_solved_total":    puzzle.get("games"),
        "storm_best_score":        safe_get(perfs, "storm", "score"),
        "racer_best_score":        safe_get(perfs, "racer", "score"),
    }


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    timestamp  = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    file_exists = os.path.isfile(OUT_FILE) and os.path.getsize(OUT_FILE) > 0

    os.makedirs("data", exist_ok=True)
    print(f"[{timestamp}] Join-Detection gestartet...")

    team_members  = get_team_members()
    known_members = get_known_members()
    new_members   = team_members - known_members

    print(f"  Team: {len(team_members)} | Bekannt: {len(known_members)} | Neu: {len(new_members)}")

    if not new_members:
        print("  Keine neuen Mitglieder. Fertig.")
        return

    print(f"  Neue Mitglieder: {', '.join(sorted(new_members))}")

    new_rows = []
    for username in sorted(new_members):
        print(f"  → Baseline für '{username}'...")
        try:
            resp = fetch_with_retry(f"{BASE_URL}/user/{username}", HEADERS)
            user_data = resp.json()
            # Wichtig: nochmal prüfen ob der User wirklich noch nicht bekannt ist
            # (mit korrektem API-Username, nicht dem lowercase-Key)
            canonical = user_data.get("username", username)
            row  = build_row(user_data, timestamp)
            if row:
                new_rows.append(row)
                print(f"    ✓ Aufgaben gesamt: {row['puzzles_solved_total']}, Wertung: {row['puzzle_rating']}")
        except Exception as e:
            print(f"    ✗ Fehler: {e}")
        time.sleep(0.3)

    if new_rows:
        with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            if not file_exists:
                writer.writeheader()
            writer.writerows(new_rows)
        print(f"\n  {len(new_rows)} Baseline(s) gespeichert.")
    else:
        print("  Nichts zu speichern.")


if __name__ == "__main__":
    main()
