"""
daily_update.py

Runs every day via GitHub Actions.
Fetches the latest TOTO results and adds any new draws to Supabase.
Safe to run daily — it checks for existence before inserting.

Environment variables required (set as GitHub Actions secrets):
    SUPABASE_URL
    SUPABASE_KEY
"""

import os
from supabase import create_client
from scraper import fetch_draw, fetch_recent_draws


def get_latest_draw_no(supabase) -> int:
    """Get the highest draw number already in the database."""
    result = (
        supabase.table("toto_draws")
        .select("draw_no")
        .order("draw_no", desc=True)
        .limit(1)
        .execute()
    )
    if result.data:
        return result.data[0]["draw_no"]
    return 0


def draw_exists(supabase, draw_no: int) -> bool:
    result = (
        supabase.table("toto_draws")
        .select("draw_no")
        .eq("draw_no", draw_no)
        .execute()
    )
    return bool(result.data)


def insert_draw(supabase, draw: dict):
    """Insert a new draw and its prize details."""
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

    if draw.get("prize_details"):
        prize_rows = [
            {
                "draw_no":        draw["draw_no"],
                "prize_group":    p["prize_group"],
                "share_amount":   p["share_amount"],
                "winning_shares": p["winning_shares"],
            }
            for p in draw["prize_details"]
        ]
        supabase.table("toto_prize_details").insert(prize_rows).execute()


def main():
    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_KEY"]
    supabase = create_client(supabase_url, supabase_key)

    latest_in_db = get_latest_draw_no(supabase)
    print(f"Latest draw in DB: {latest_in_db}")

    # Try fetching the next few draws after what we have
    # (handles special draws and catching up after downtime)
    new_count = 0
    consecutive_empty = 0
    check_draw = latest_in_db + 1

    while consecutive_empty < 5:
        print(f"Checking draw {check_draw}...")

        if draw_exists(supabase, check_draw):
            print(f"  Draw {check_draw} already exists — skipping")
            check_draw += 1
            continue

        draw = fetch_draw(check_draw)

        if draw is None:
            print(f"  Draw {check_draw} not available yet")
            consecutive_empty += 1
            check_draw += 1
            continue

        consecutive_empty = 0
        print(f"  New draw found: {draw['draw_date']} | {draw['n1']}-{draw['n2']}-{draw['n3']}-{draw['n4']}-{draw['n5']}-{draw['n6']} +{draw['additional']}")

        try:
            insert_draw(supabase, draw)
            print(f"  ✓ Inserted draw {check_draw}")
            new_count += 1
        except Exception as e:
            print(f"  ✗ Failed to insert draw {check_draw}: {e}")

        check_draw += 1

    if new_count == 0:
        print("No new draws found today.")
    else:
        print(f"\nDone! Added {new_count} new draw(s).")


if __name__ == "__main__":
    main()
