import csv
import os
import sys
import datetime

IN_FILE  = "data/tactics_history.csv"
OUT_FILE = "data/leaderboard.csv"

LEADERBOARD_FIELDS = [
    "rank",
    "username",
    "puzzles_since_tracking",
    "puzzles_total_now",
    "puzzle_rating_now",
    "puzzle_rating_progress",
    "avg_bullet_blitz_rapid",
    "storm_best_score",
    "racer_best_score",
    "first_seen",
    "last_seen",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_timestamp(row):
    return row.get("timestamp") or row.get("date") or ""


def parse_ts(ts):
    try:
        return datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M UTC")
    except:
        return None


def safe_int(value, default=None):
    try:
        return int(value) if value not in (None, "", "None") else default
    except:
        return default


def safe_float(value, default=None):
    try:
        return float(value) if value not in (None, "", "None") else default
    except:
        return default


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_history(path: str) -> dict:
    if not os.path.isfile(path):
        print(f"ERROR: {path} not found.")
        sys.exit(1)

    users = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            username = row["username"]
            users.setdefault(username, []).append(row)

    # sort per user
    for username in users:
        users[username].sort(key=lambda r: get_timestamp(r))

    return users


# ---------------------------------------------------------------------------
# Build leaderboard
# ---------------------------------------------------------------------------

def build_leaderboard(users: dict) -> list:
    entries = []

    for username, rows in users.items():
        # Deduplicate (latest per timestamp wins)
        by_ts = {}
        for row in rows:
            ts = get_timestamp(row)
            by_ts[ts] = row

        deduped = sorted(by_ts.values(), key=lambda r: get_timestamp(r))

        if not deduped:
            continue

        oldest = deduped[0]
        newest = deduped[-1]

        total_now   = safe_int(newest.get("puzzles_solved_total"), 0)
        total_start = safe_int(oldest.get("puzzles_solved_total"), 0)

        oldest_ts = get_timestamp(oldest)
        newest_ts = get_timestamp(newest)

        dt_old = parse_ts(oldest_ts)
        dt_new = parse_ts(newest_ts)

# replacement code
solved_since = total_now - total_start

        entries.append({
            "username": username,
            "puzzles_since_tracking": solved_since,
            "puzzles_total_now": total_now,
            "puzzle_rating_now": safe_int(newest.get("puzzle_rating")),
            "puzzle_rating_progress": safe_int(newest.get("puzzle_rating_progress")),
            "avg_bullet_blitz_rapid": safe_float(newest.get("avg_bullet_blitz_rapid")),
            "storm_best_score": safe_int(newest.get("storm_best_score")),
            "racer_best_score": safe_int(newest.get("racer_best_score")),
            "first_seen": oldest_ts,
            "last_seen": newest_ts,
        })

    # ✅ Improved sorting
    entries.sort(
        key=lambda e: (
            e["puzzles_since_tracking"] is None,
            -(e["puzzles_since_tracking"] or 0),
            -(e["puzzles_total_now"] or 0),
        )
    )

    # ✅ Stable ranking (ties handled)
    rank = 1
    prev = None
    for i, e in enumerate(entries):
        current = (e["puzzles_since_tracking"], e["puzzles_total_now"])
        if current != prev:
            rank = i + 1
        e["rank"] = rank
        prev = current

    return entries


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_leaderboard(entries: list):
    print(f"\n{'RANK':<5} {'USERNAME':<20} {'SOLVED':<10} {'RATING':<8} {'PROG':<6}")
    print("-" * 60)

    for e in entries[:20]:
        solved = e["puzzles_since_tracking"]
        solved_str = f"{solved:,}" if solved is not None else "—"

        prog = e["puzzle_rating_progress"]
        prog_str = f"{prog:+d}" if prog is not None else "?"

        print(
            f"{e['rank']:<5} {e['username']:<20} "
            f"{solved_str:<10} "
            f"{str(e['puzzle_rating_now'] or '?'):<8} "
            f"{prog_str:<6}"
        )


def main():
    users   = load_history(IN_FILE)
    entries = build_leaderboard(users)

    os.makedirs("data", exist_ok=True)

    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEADERBOARD_FIELDS)
        writer.writeheader()
        writer.writerows(entries)

    print_leaderboard(entries)
    print(f"\nLeaderboard written to '{OUT_FILE}' — {len(entries)} players ranked.")


if __name__ == "__main__":
    main()
