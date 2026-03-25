"""
fourd_historical.py

Run this ONCE to populate all historical 4D results from Nestia.
Loops through all available draws, skipping any already in DB.

Usage:
    source venv/bin/activate
    python fourd_historical.py
"""

import os
import time
from dotenv import load_dotenv
from supabase import create_client
from fourd_scraper import fetch_draw, fetch_draw_list_from_nestia

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
REQUEST_DELAY = 1.5


def draw_exists(supabase, draw_no):
    result = supabase.table("fourd_draws").select("draw_no").eq("draw_no", draw_no).execute()
    return bool(result.data)


def insert_draw(supabase, draw):
    supabase.table("fourd_draws").insert({
        "draw_no":   draw["draw_no"],
        "draw_date": draw["draw_date"],
        "prize_1st": draw["prize_1st"],
        "prize_2nd": draw["prize_2nd"],
        "prize_3rd": draw["prize_3rd"],
    }).execute()

    prize_rows = (
        [{"draw_no": draw["draw_no"], "category": "starter", "number": n} for n in draw["starters"]] +
        [{"draw_no": draw["draw_no"], "category": "consolation", "number": n} for n in draw["consolations"]]
    )
    if prize_rows:
        supabase.table("fourd_prizes").insert(prize_rows).execute()


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("Fetching draw list from Nestia...")
    draw_list = fetch_draw_list_from_nestia()

    if not draw_list:
        print("No draws found. Exiting.")
        return

    print(f"Found {len(draw_list)} draws.")
    print(f"Latest: Draw {draw_list[0]['draw_no']} ({draw_list[0]['date']})")
    print(f"Oldest: Draw {draw_list[-1]['draw_no']} ({draw_list[-1]['date']})\n")

    inserted = skipped = failed = 0

    for d in draw_list:
        draw_no = d["draw_no"]
        date_str = d["date"]

        if draw_exists(supabase, draw_no):
            print(f"Draw {draw_no} already in DB — skipping")
            skipped += 1
            continue

        print(f"Fetching draw {draw_no} ({date_str})...", end=" ")
        draw = fetch_draw(date_str, draw_no)

        if draw is None:
            print("No data")
            failed += 1
            time.sleep(REQUEST_DELAY)
            continue

        try:
            insert_draw(supabase, draw)
            print(f"1st:{draw['prize_1st']} 2nd:{draw['prize_2nd']} 3rd:{draw['prize_3rd']} ✓")
            inserted += 1
        except Exception as e:
            print(f"✗ DB error: {e}")
            failed += 1

        time.sleep(REQUEST_DELAY)

    print(f"\n{'='*50}")
    print(f"Done! Inserted: {inserted} | Skipped: {skipped} | Failed: {failed}")


if __name__ == "__main__":
    main()
