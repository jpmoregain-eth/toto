"""
fourd_scraper.py
- fetch_draw(date_str, draw_no): scrape single draw from Nestia (historical)
- fetch_latest_draws(): scrape latest 6 draws from official SP archive (daily)
"""

import re
import time
import requests
from bs4 import BeautifulSoup

NESTIA_BASE = "https://lottery.nestia.com/4d"
ARCHIVE_URL = "https://www.singaporepools.com.sg/DataFileArchive/Lottery/Output/fourd_result_top_draws_en.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_draw(date_str: str, draw_no: int, retries: int = 3):
    """Fetch a single draw from Nestia by date and draw number."""
    url = f"{NESTIA_BASE}/{date_str}-draw-{draw_no}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return parse_nestia_page(r.text, draw_no, date_str)
        except requests.RequestException as e:
            print(f"  Attempt {attempt+1} failed for draw {draw_no}: {e}")
            time.sleep(2 ** attempt)
    return None


def parse_nestia_page(html: str, expected_draw_no: int, date_str: str):
    """Parse a Nestia 4D result page."""
    soup = BeautifulSoup(html, "html.parser")

    # Prizes are in <i> tags inside the table header
    i_tags = [i.get_text(strip=True) for i in soup.find_all("i") if re.match(r"^\d{4}$", i.get_text(strip=True))]
    if len(i_tags) < 3:
        return None
    prize_1st, prize_2nd, prize_3rd = i_tags[0], i_tags[1], i_tags[2]

    # Starters and consolations from text
    text = soup.get_text()
    starters = []
    consolations = []

    starter_match = re.search(r"Starter Prizes\s*([\d\s]+?)Consolation Prizes", text, re.DOTALL)
    consolation_match = re.search(r"Consolation Prizes\s*([\d\s]+?)(?:How to win|Draw No\.|$)", text, re.DOTALL)

    if starter_match:
        starters = re.findall(r"\b\d{4}\b", starter_match.group(1))[:10]
    if consolation_match:
        consolations = re.findall(r"\b\d{4}\b", consolation_match.group(1))[:10]

    return {
        "draw_no":      expected_draw_no,
        "draw_date":    date_str,
        "prize_1st":    prize_1st,
        "prize_2nd":    prize_2nd,
        "prize_3rd":    prize_3rd,
        "starters":     starters,
        "consolations": consolations,
    }


def fetch_draw_list_from_nestia():
    """Get list of all available draws from Nestia's latest page."""
    try:
        r = requests.get(f"{NESTIA_BASE}", headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        draws = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = re.match(r".*/4d/(\d{4}-\d{2}-\d{2})-draw-(\d+)", href)
            if m:
                draws.append({
                    "date": m.group(1),
                    "draw_no": int(m.group(2))
                })
        return draws
    except Exception as e:
        print(f"Failed to fetch draw list: {e}")
        return []


def fetch_latest_draws():
    """
    Fetch latest draws from official Singapore Pools archive.
    Used by daily_update — authoritative source.
    """
    try:
        r = requests.get(ARCHIVE_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch SP archive: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    for table_wrap in soup.find_all("div", class_="tables-wrap"):
        try:
            draw_no_tag = table_wrap.find("th", class_="drawNumber")
            draw_date_tag = table_wrap.find("th", class_="drawDate")
            if not draw_no_tag or not draw_date_tag:
                continue

            draw_no = int(re.search(r"\d+", draw_no_tag.get_text()).group())
            from datetime import datetime
            date_text = re.search(r"\d{2}\s+\w+\s+\d{4}", draw_date_tag.get_text()).group()
            draw_date = datetime.strptime(date_text, "%d %b %Y").strftime("%Y-%m-%d")

            prize_1st = table_wrap.find("td", class_="tdFirstPrize")
            prize_2nd = table_wrap.find("td", class_="tdSecondPrize")
            prize_3rd = table_wrap.find("td", class_="tdThirdPrize")

            if not prize_1st:
                continue

            starter_body = table_wrap.find("tbody", class_="tbodyStarterPrizes")
            starters = []
            if starter_body:
                starters = [td.get_text(strip=True) for td in starter_body.find_all("td")
                            if re.match(r"^\d{4}$", td.get_text(strip=True))]

            consolation_body = table_wrap.find("tbody", class_="tbodyConsolationPrizes")
            consolations = []
            if consolation_body:
                consolations = [td.get_text(strip=True) for td in consolation_body.find_all("td")
                                if re.match(r"^\d{4}$", td.get_text(strip=True))]

            results.append({
                "draw_no":      draw_no,
                "draw_date":    draw_date,
                "prize_1st":    prize_1st.get_text(strip=True).zfill(4),
                "prize_2nd":    prize_2nd.get_text(strip=True).zfill(4) if prize_2nd else None,
                "prize_3rd":    prize_3rd.get_text(strip=True).zfill(4) if prize_3rd else None,
                "starters":     starters,
                "consolations": consolations,
            })
        except Exception as e:
            print(f"Error parsing draw block: {e}")
            continue

    return results
