"""
historical_scraper.py

Run this ONCE to populate your Supabase database with all historical TOTO results.
It loops from the earliest draw up to the current draw, skipping any that already exist.

Usage:
    pip install requests beautifulsoup4 supabase python-dotenv
    python historical_scraper.py

Set these environment variables (or put them in a .env file):
    SUPABASE_URL=https://xxxx.supabase.co
    SUPABASE_KEY=your-service-role-key
"""

import os
import time
from dotenv import load_dotenv
from supabase import create_client
from scraper import fetch_draw

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# Current draw as of March 2026 — update this if you run it later
# The script will stop automatically when draws return no data
LATEST_DRAW = 4165

# Earliest draw with data on Singapore Pools website
# They typically have a few years of history. Start lower if unsure — it'll skip gracefully.
EARLIEST_DRAW = 1200

# Delay between requests (seconds) — be polite to their servers
REQUEST_DELAY = 1.5

# How many consecutive empty draws before we assume we've hit the end
MAX_CONSECUTIVE_EMPTY = 10


def upsert_draw(supabase, draw: dict):
    """Insert a draw into Supabase. Skip if draw_no already exists."""
    draw_no = draw["draw_no"]

    # Check if already exists
    existing = (
        supabase.table("toto_draws")
        .select("draw_no")
        .eq("draw_no", draw_no)
        .execute()
    )
    if existing.data:
        print(f"  Draw {draw_no} already in DB — skipping")
        return False

    # Insert main draw record
    supabase.table("toto_draws").insert({
        "draw_no":      draw["draw_no"],
        "draw_date":    draw["draw_date"],
        "n1":           draw["n1"],
        "n2":           draw["n2"],
        "n3":           draw["n3"],
        "n4":           draw["n4"],
        "n5":           draw["n5"],
        "n6":           draw["n6"],
        "additional":   draw["additional"],
        "group1_prize": draw["group1_prize"],
    }).execute()

    # Insert prize breakdown
    if draw.get("prize_details"):
        prize_rows = [
            {
                "draw_no":       draw_no,
                "prize_group":   p["prize_group"],
                "share_amount":  p["share_amount"],
                "winning_shares": p["winning_shares"],
            }
            for p in draw["prize_details"]
        ]
        supabase.table("toto_prize_details").insert(prize_rows).execute()

    return True


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"Connected to Supabase. Scraping draws {EARLIEST_DRAW} to {LATEST_DRAW}...\n")

    inserted = 0
    skipped = 0
    failed = 0
    consecutive_empty = 0

    for draw_no in range(EARLIEST_DRAW, LATEST_DRAW + 1):
        print(f"Fetching draw {draw_no}...", end=" ")

        draw = fetch_draw(draw_no)

        if draw is None:
            print("No data found")
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                print(f"\nHit {MAX_CONSECUTIVE_EMPTY} consecutive empty draws — stopping early.")
                break
            time.sleep(REQUEST_DELAY)
            continue

        consecutive_empty = 0
        print(f"Draw {draw['draw_no']} | {draw['draw_date']} | {draw['n1']}-{draw['n2']}-{draw['n3']}-{draw['n4']}-{draw['n5']}-{draw['n6']} +{draw['additional']}", end=" ")

        try:
            was_inserted = upsert_draw(supabase, draw)
            if was_inserted:
                inserted += 1
                print("✓ Inserted")
            else:
                skipped += 1
        except Exception as e:
            failed += 1
            print(f"✗ DB error: {e}")

        time.sleep(REQUEST_DELAY)

    print(f"\n{'='*50}")
    print(f"Done! Inserted: {inserted} | Skipped: {skipped} | Failed: {failed}")


if __name__ == "__main__":
    main()
