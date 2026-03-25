"""
fourd_scraper.py - Scrapes 4D results from Singapore Pools archive file.
"""

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime

ARCHIVE_URL = "https://www.singaporepools.com.sg/DataFileArchive/Lottery/Output/fourd_result_top_draws_en.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def fetch_latest_draws():
    try:
        r = requests.get(ARCHIVE_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch 4D archive: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    for table_wrap in soup.find_all("div", class_="tables-wrap"):
        try:
            # Draw number and date
            draw_no_tag = table_wrap.find("th", class_="drawNumber")
            draw_date_tag = table_wrap.find("th", class_="drawDate")
            if not draw_no_tag or not draw_date_tag:
                continue

            draw_no = int(re.search(r"\d+", draw_no_tag.get_text()).group())
            date_str = re.search(r"\d{2}\s+\w+\s+\d{4}", draw_date_tag.get_text()).group()
            draw_date = datetime.strptime(date_str, "%d %b %Y").date()

            # Top 3 prizes
            prize_1st = table_wrap.find("td", class_="tdFirstPrize")
            prize_2nd = table_wrap.find("td", class_="tdSecondPrize")
            prize_3rd = table_wrap.find("td", class_="tdThirdPrize")

            if not prize_1st:
                continue

            # Starter prizes
            starter_body = table_wrap.find("tbody", class_="tbodyStarterPrizes")
            starters = []
            if starter_body:
                starters = [td.get_text(strip=True) for td in starter_body.find_all("td") if re.match(r"^\d{4}$", td.get_text(strip=True))]

            # Consolation prizes
            consolation_body = table_wrap.find("tbody", class_="tbodyConsolationPrizes")
            consolations = []
            if consolation_body:
                consolations = [td.get_text(strip=True) for td in consolation_body.find_all("td") if re.match(r"^\d{4}$", td.get_text(strip=True))]

            results.append({
                "draw_no":     draw_no,
                "draw_date":   str(draw_date),
                "prize_1st":   prize_1st.get_text(strip=True).zfill(4),
                "prize_2nd":   prize_2nd.get_text(strip=True).zfill(4) if prize_2nd else None,
                "prize_3rd":   prize_3rd.get_text(strip=True).zfill(4) if prize_3rd else None,
                "starters":    starters,
                "consolations": consolations,
            })
        except Exception as e:
            print(f"Error parsing draw block: {e}")
            continue

    return results
