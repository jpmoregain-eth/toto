"""
fourd_import_by_date.py
Imports 4D historical data from singapore_4d_by_date.csv
Handles duplicate dates silently via DB constraint.
"""

import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

CSV_PATH = os.path.expanduser("~/Downloads/singapore_4d_by_date.csv")

df = pd.read_csv(CSV_PATH)
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
df = df.sort_values('Date').reset_index(drop=True)

print(f"Total draws in CSV: {len(df)}")
print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")

starter_cols = [c for c in df.columns if 'Starter' in c]
cons_cols = [c for c in df.columns if 'Consolation' in c]

inserted = skipped = failed = 0

for idx, row in df.iterrows():
    date = row['Date']

    try:
        result = supabase.table("fourd_draws").insert({
            "draw_date": date,
            "prize_1st": str(int(row['1st Prize'])).zfill(4) if pd.notna(row['1st Prize']) else None,
            "prize_2nd": str(int(row['2nd Prize'])).zfill(4) if pd.notna(row['2nd Prize']) else None,
            "prize_3rd": str(int(row['3rd Prize'])).zfill(4) if pd.notna(row['3rd Prize']) else None,
        }).execute()

        draw_no = result.data[0]['draw_no']

        prize_rows = []
        for col in starter_cols:
            if pd.notna(row[col]):
                prize_rows.append({"draw_no": draw_no, "category": "starter", "number": str(int(row[col])).zfill(4)})
        for col in cons_cols:
            if pd.notna(row[col]):
                prize_rows.append({"draw_no": draw_no, "category": "consolation", "number": str(int(row[col])).zfill(4)})

        if prize_rows:
            supabase.table("fourd_prizes").insert(prize_rows).execute()

        print(f"✓ {date} | 1st:{str(int(row['1st Prize'])).zfill(4)}")
        inserted += 1

    except Exception as e:
        if '23505' in str(e):
            skipped += 1
        else:
            print(f"✗ {date}: {e}")
            failed += 1

    if (idx + 1) % 100 == 0:
        print(f"Progress: {idx+1}/{len(df)} | Inserted: {inserted} | Skipped: {skipped} | Failed: {failed}")

print(f"\nDone! Inserted: {inserted} | Skipped: {skipped} | Failed: {failed}")
