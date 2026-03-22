"""
scraper.py - Core logic for fetching and parsing Singapore Pools TOTO results.
Shared by both the historical scraper and the daily update script.
"""

import base64
import re
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

RESULT_URL = "https://www.singaporepools.com.sg/en/product/sr/pages/toto_results.aspx?sppl="
RECENT_URL = "https://www.singaporepools.com.sg/DataFileArchive/Lottery/Output/toto_result_top_draws_en.html"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def build_url(draw_no: int) -> str:
    """Encode draw number into the sppl URL parameter."""
    encoded = base64.b64encode(f"DrawNumber={draw_no}".encode()).decode()
    return RESULT_URL + encoded


def parse_money(text: str) -> int | None:
    """Convert '$1,375,057' -> 1375057. Returns None if no winner (dash)."""
    text = text.strip()
    if text in ("-", "", "N/A"):
        return None
    cleaned = re.sub(r"[^\d]", "", text)
    return int(cleaned) if cleaned else None


def parse_draw_page(html: str) -> dict | None:
    """
    Parse a single draw result page and return a structured dict.
    Returns None if the page has no valid draw data.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # Extract draw date and draw number
    draw_no_match = re.search(r"Draw No\.\s*(\d+)", text)
    date_match = re.search(
        r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun),\s+(\d{2}\s+\w+\s+\d{4})", text
    )

    if not draw_no_match or not date_match:
        return None

    draw_no = int(draw_no_match.group(1))
    draw_date = datetime.strptime(date_match.group(2), "%d %b %Y").date()

    # Extract winning numbers — they have class like "win1", "win2" ... "win6"
    winning_numbers = []
    for i in range(1, 7):
        tag = soup.find("td", {"class": f"win{i}"})
        if tag:
            winning_numbers.append(int(tag.get_text(strip=True)))

    # Additional number
    add_tag = soup.find("td", {"class": "additional"})
    additional = int(add_tag.get_text(strip=True)) if add_tag else None

    # If we couldn't find the styled classes, fall back to positional parsing
    if not winning_numbers:
        winning_numbers, additional = _fallback_parse_numbers(soup)

    if len(winning_numbers) != 6 or additional is None or str(draw_date) == '0001-01-01':
        return None

    # Group 1 prize
    group1_prize = None
    g1_tag = soup.find("td", {"class": "jackpotPrize"})
    if g1_tag:
        group1_prize = parse_money(g1_tag.get_text())

    # Prize details table (Groups 1-7)
    prize_details = _parse_prize_table(soup)

    return {
        "draw_no": draw_no,
        "draw_date": str(draw_date),
        "n1": winning_numbers[0],
        "n2": winning_numbers[1],
        "n3": winning_numbers[2],
        "n4": winning_numbers[3],
        "n5": winning_numbers[4],
        "n6": winning_numbers[5],
        "additional": additional,
        "group1_prize": group1_prize,
        "prize_details": prize_details,
    }


def _fallback_parse_numbers(soup: BeautifulSoup):
    """
    Fallback: grab all <td> cells that look like toto numbers (1–49).
    Used if CSS classes change.
    """
    all_tds = soup.find_all("td")
    numbers = []
    for td in all_tds:
        val = td.get_text(strip=True)
        if val.isdigit() and 1 <= int(val) <= 49:
            numbers.append(int(val))
        if len(numbers) == 7:
            break

    if len(numbers) == 7:
        return numbers[:6], numbers[6]
    return [], None


def _parse_prize_table(soup: BeautifulSoup) -> list[dict]:
    """Extract prize group, share amount, and number of winners."""
    details = []
    rows = soup.find_all("tr")
    for row in rows:
        cells = row.find_all("td")
        if len(cells) >= 3:
            group_text = cells[0].get_text(strip=True)
            group_match = re.search(r"Group\s*(\d)", group_text)
            if group_match:
                group_num = int(group_match.group(1))
                amount = parse_money(cells[1].get_text())
                shares_text = re.sub(r"[^\d]", "", cells[2].get_text())
                shares = int(shares_text) if shares_text else 0
                details.append({
                    "prize_group": group_num,
                    "share_amount": amount,
                    "winning_shares": shares,
                })
    return details


def fetch_draw(draw_no: int, retries: int = 3) -> dict | None:
    """Fetch and parse a single draw by number."""
    url = build_url(draw_no)
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=15)
            if response.status_code == 200:
                result = parse_draw_page(response.text)
                if result and result["draw_no"] != draw_no:
                    return None
                return result
            elif response.status_code == 404:
                return None  # Draw doesn't exist
        except requests.RequestException as e:
            print(f"  Attempt {attempt + 1} failed for draw {draw_no}: {e}")
            time.sleep(2 ** attempt)  # Exponential backoff
    return None


def fetch_recent_draws() -> list[dict]:
    """
    Fetch the most recent draws from the archive file.
    Used by the daily update script — faster than looping by draw number.
    """
    try:
        response = requests.get(RECENT_URL, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch recent draws: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    results = []

    # The archive page lists multiple draws sequentially
    # Each draw block contains date, draw no, numbers, additional, prize table
    draw_blocks = soup.find_all("table")
    # Re-parse as full text sections split by draw number
    full_text = response.text
    draw_sections = re.split(r"(?=Draw No\.)", full_text)

    for section in draw_sections:
        if "Draw No." not in section:
            continue
        result = parse_draw_page(f"<html><body>{section}</body></html>")
        if result:
            results.append(result)

    return results
