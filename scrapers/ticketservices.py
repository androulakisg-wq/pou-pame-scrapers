import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
import re
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

MONTHS_GR = {
    "Ιαν": 1, "Φεβ": 2, "Μαρ": 3, "Απρ": 4,
    "Μαΐ": 5, "Ιουν": 6, "Ιουλ": 7, "Αυγ": 8,
    "Σεπ": 9, "Οκτ": 10, "Νοε": 11, "Δεκ": 12,
    "Ιανουαρίου": 1, "Φεβρουαρίου": 2, "Μαρτίου": 3,
    "Απριλίου": 4, "Μαΐου": 5, "Ιουνίου": 6,
    "Ιουλίου": 7, "Αυγούστου": 8, "Σεπτεμβρίου": 9,
    "Οκτωβρίου": 10, "Νοεμβρίου": 11, "Δεκεμβρίου": 12
}

def strip_html(text):
    if not text:
        return None
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:500]

def parse_date(date_str):
    try:
        date_str = date_str.strip()
        parts = date_str.split()
        for part in parts:
            if part in MONTHS_GR:
                idx = parts.index(part)
                day = int(re.sub(r'\D', '', parts[idx-1])) if idx > 0 else 1
                month = MONTHS_GR[part]
                year = int(parts[idx+1]) if idx+1 < len(parts) else datetime.now().year
                return datetime(year, month, day).isoformat()
    except:
        return None

def scrape():
    url = "https://www.ticketservices.gr/events/?region=crete"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = "iso-8859-7"
        soup = BeautifulSoup(r.text, "html.parser")

        events = soup.select("li.event")
        count = 0

        for ev in events:
            try:
                title_el = ev.select_one("h3") or ev.select_one(".event-title")
                link_el = ev.select_one("a")
                date_el = ev.select_one(".date") or ev.select_one(".event-date")
                desc_el = ev.select_one(".description") or ev.select_one("p")

                if not title_el or not link_el:
                    continue

                title = strip_html(title_el.get_text(strip=True))
