"""
fourd_import_csv.py
Imports historical 4D data from Results.csv into Supabase.
"""

import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

CSV_PATH = os.path.expanduser("~/Downloads/Results.csv")

df = pd.read_csv(CSV_PATH)
df['DrawDate'] = pd.to_datetime(df['DrawDate']).dt.strftime('%Y-%m-%d')

dates = sorted(df['DrawDate'].unique())
print(f"Total unique draw dates: {len(dates)}")
print(f"Date range: {dates[0]} to {dates[-1]}")

inserted = skipped = failed = 0

for date in dates:
    existing = supabase.table("fourd_draws").select("draw_no").eq("draw_date", date).execute()
    if existing.data:
        skipped += 1
        continue

    group = df[df['DrawDate'] == date]

    prize_1st = group[group['PrizeCode'] == '1']['Digit'].values
    prize_2nd = group[group['PrizeCode'] == '2']['Digit'].values
    prize_3rd = group[group['PrizeCode'] == '3']['Digit'].values
    starters = group[group['PrizeCode'] == 'S']['Digit'].values.tolist()
    consolations = group[group['PrizeCode'] == 'C']['Digit'].values.tolist()

    if len(prize_1st) == 0:
        print(f"  Skipping {date} — no 1st prize")
        failed += 1
        continue

    try:
        result = supabase.table("fourd_draws").insert({
            "draw_date": date,
            "prize_1st": str(prize_1st[0]).zfill(4),
            "prize_2nd": str(prize_2nd[0]).zfill(4) if len(prize_2nd) > 0 else None,
            "prize_3rd": str(prize_3rd[0]).zfill(4) if len(prize_3rd) > 0 else None,
        }).execute()

        draw_no = result.data[0]['draw_no']

        prize_rows = (
            [{"draw_no": draw_no, "category": "starter", "number": str(n).zfill(4)} for n in starters] +
            [{"draw_no": draw_no, "category": "consolation", "number": str(n).zfill(4)} for n in consolations]
        )
        if prize_rows:
            supabase.table("fourd_prizes").insert(prize_rows).execute()

        print(f"✓ {date} | 1st:{str(prize_1st[0]).zfill(4)}")
        inserted += 1

    except Exception as e:
        print(f"✗ {date}: {e}")
        failed += 1

print(f"\nDone! Inserted: {inserted} | Skipped: {skipped} | Failed: {failed}")
