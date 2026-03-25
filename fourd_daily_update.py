"""
fourd_daily_update.py - Daily update for 4D results.
"""

import os
from dotenv import load_dotenv
from supabase import create_client
from fourd_scraper import fetch_latest_draws

load_dotenv()

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
    supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
    draws = fetch_latest_draws()
    print(f"Fetched {len(draws)} draws from archive")

    new_count = 0
    for draw in draws:
        if draw_exists(supabase, draw["draw_no"]):
            print(f"  Draw {draw['draw_no']} already exists — skipping")
            continue
        try:
            insert_draw(supabase, draw)
            print(f"  ✓ Inserted Draw {draw['draw_no']} | {draw['draw_date']} | 1st: {draw['prize_1st']}")
            new_count += 1
        except Exception as e:
            print(f"  ✗ Error inserting draw {draw['draw_no']}: {e}")

    print(f"\nDone! Added {new_count} new draw(s).")

if __name__ == "__main__":
    main()
