"""
fourd_historical.py - Run once to populate all historical 4D results.
"""

import os
import time
from dotenv import load_dotenv
from supabase import create_client
from fourd_scraper import fetch_draw

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
LATEST_DRAW = 5460
EARLIEST_DRAW = 1
REQUEST_DELAY = 1.5
MAX_CONSECUTIVE_EMPTY = 10

def upsert_draw(supabase, draw):
    draw_no = draw["draw_no"]
    existing = supabase.table("fourd_draws").select("draw_no").eq("draw_no", draw_no).execute()
    if existing.data:
        print(f"  Draw {draw_no} already in DB — skipping")
        return False

    supabase.table("fourd_draws").insert({
        "draw_no":    draw["draw_no"],
        "draw_date":  draw["draw_date"],
        "prize_1st":  draw["prize_1st"],
        "prize_2nd":  draw["prize_2nd"],
        "prize_3rd":  draw["prize_3rd"],
    }).execute()

    prize_rows = (
        [{"draw_no": draw_no, "category": "starter", "number": n} for n in draw["starters"]] +
        [{"draw_no": draw_no, "category": "consolation", "number": n} for n in draw["consolations"]]
    )
    if prize_rows:
        supabase.table("fourd_prizes").insert(prize_rows).execute()

    return True

def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    print(f"Connected. Scraping draws {EARLIEST_DRAW} to {LATEST_DRAW}...\n")

    inserted = skipped = failed = 0
    consecutive_empty = 0

    for draw_no in range(EARLIEST_DRAW, LATEST_DRAW + 1):
        print(f"Fetching draw {draw_no}...", end=" ")
        draw = fetch_draw(draw_no)

        if draw is None:
            print("No data")
            consecutive_empty += 1
            if consecutive_empty >= MAX_CONSECUTIVE_EMPTY:
                print(f"\nHit {MAX_CONSECUTIVE_EMPTY} consecutive empty — stopping.")
                break
            time.sleep(REQUEST_DELAY)
            continue

        consecutive_empty = 0
        print(f"{draw['draw_date']} | 1st:{draw['prize_1st']} 2nd:{draw['prize_2nd']} 3rd:{draw['prize_3rd']}", end=" ")

        try:
            if upsert_draw(supabase, draw):
                inserted += 1
                print("✓")
            else:
                skipped += 1
        except Exception as e:
            failed += 1
            print(f"✗ {e}")

        time.sleep(REQUEST_DELAY)

    print(f"\nDone! Inserted: {inserted} | Skipped: {skipped} | Failed: {failed}")

if __name__ == "__main__":
    main()
