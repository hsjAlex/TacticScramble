"""
One-time cleanup: deduplicate data/tactics_history.csv.

If fetch_lichess_stats.py was run multiple times on the same day,
each user will have duplicate rows for that date. This script keeps
only the LAST row per (username, date) pair and rewrites the file.

Run once from your repo root:
    python cleanup_history.py
"""

import csv
import os
import shutil
import datetime

IN_FILE  = "data/tactics_history.csv"
BAK_FILE = f"data/tactics_history_backup_{datetime.date.today().isoformat()}.csv"

if not os.path.isfile(IN_FILE):
    print(f"Nothing to do — {IN_FILE} not found.")
    exit(0)

# Read all rows
with open(IN_FILE, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    rows = list(reader)

# Keep last row per (username, date) — preserves order
seen = {}
for row in rows:
    key = (row["username"], row.get("timestamp", row.get("date",""))[:13])
    seen[key] = row   # later rows overwrite earlier ones

deduped = list(seen.values())

# Backup original
shutil.copy(IN_FILE, BAK_FILE)
print(f"Backup saved to {BAK_FILE}")
print(f"Rows before: {len(rows)}  →  after deduplication: {len(deduped)}")

# Rewrite
with open(IN_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(deduped)

print("Done. Run generate_leaderboard.py to rebuild the leaderboard.")
