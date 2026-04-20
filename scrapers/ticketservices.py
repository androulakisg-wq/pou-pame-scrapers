import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
import re
import time
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

MONTHS_GR = {
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

def fetch_with_retry(url, headers, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.encoding = "utf-8"
            return r
        except requests.Timeout:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"Timeout {url} — retry {attempt+1}/{max_retries} σε {wait}s")
                time.sleep(wait)
            else:
                print(f"Αποτυχία μετά από {max_retries} προσπάθειες: {url}")
                return None
        except Exception as e:
            print(f"Error {url}: {e}")
            return None

def parse_date(date_str):
    try:
        date_str = date_str.strip()
        parts = date_str.split()
        for i, part in enumerate(parts):
            if part in MONTHS_GR:
                day = int(re.sub(r'\D', '', parts[i-1])) if i > 0 else 1
                month = MONTHS_GR[part]
                year = int(parts[i+1]) if i+1 < len(parts) and parts[i+1].isdigit() else datetime.now().year
                return datetime(year, month, day).strftime("%Y-%m-%d")
    except Exception:
        return None

def parse_time(text):
    """Εξάγει ώρα HH:MM από κείμενο όπως 'Ώρα έναρξης: 21:00' ή '22:00'"""
    if not text:
        return None
    match = re.search(r'\b(\d{1,2}:\d{2})\b', text)
    return match.group(1) if match else None

def fetch_event_detail(url, headers):
    """Επισκέπτεται τη σελίδα event και εξάγει time_start + description"""
    r = fetch_with_retry(url, headers)
    if not r:
        return None, None

    try:
        soup = BeautifulSoup(r.text, "html.parser")

        # Εξαγωγή ώρας — ψάχνουμε σε πολλά σημεία
        time_start = None
        for selector in [".event-time", ".time", "[class*='time']", ".event-details", ".info"]:
            el = soup.select_one(selector)
            if el:
                time_start = parse_time(el.get_text())
                if time_start:
                    break

        # Fallback: ψάχνουμε στο κύριο κείμενο
        if not time_start:
            body_text = soup.get_text()
            time_start = parse_time(body_text)

        # Εξαγωγή description
        description = None
        for selector in [".event-description", ".description", ".content", "article p", ".entry-content p"]:
            el = soup.select_one(selector)
            if el:
                text = strip_html(el.get_text())
                if text and len(text) > 20:
                    description = text
                    break

        return time_start, description

    except Exception as e:
        print(f"Detail parse error {url}: {e}")
        return None, None

def scrape():
    url = "https://www.ticketservices.gr/page/results/?q=%CE%BA%CF%81%CE%B7%CF%84%CE%B7"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "el-GR,el;q=0.9",
    }

    try:
        r = fetch_with_retry(url, headers)
        if not r:
            print("ticketservices.gr: αδύνατη η σύνδεση")
            return

        soup = BeautifulSoup(r.text, "html.parser")

        events = (
            soup.select("div.eventsnewr") or
            soup.select("article.event") or
            soup.select("div.event-item") or
            soup.select("li.event")
        )

        if not events:
            print("ticketservices.gr: 0 events βρέθηκαν — πιθανή αλλαγή HTML δομής")
            return

        print(f"ticketservices: βρέθηκαν {len(events)} events")
        count = 0

        for ev in events:
            try:
                link_el = ev.select_one("a")
                title_el = ev.select_one("h2") or ev.select_one("h3") or ev.select_one(".title")
                date_el = ev.select_one(".date") or ev.select_one("time") or ev.select_one(".event-date")

                if not link_el:
                    continue

                title = strip_html(title_el.get_text(strip=True)) if title_el else strip_html(link_el.get_text(strip=True))
                if not title:
                    continue

                source_url = link_el.get("href", "")
                if source_url.startswith("/"):
                    source_url = "https://www.ticketservices.gr" + source_url
                elif not source_url.startswith("http"):
                    continue

                date_start = parse_date(date_el.get_text(strip=True)) if date_el else None

                if date_start and date_start < datetime.now().strftime("%Y-%m-%d"):
                    continue

                # Επίσκεψη detail page για ώρα + description
                time.sleep(1)
                time_start, description = fetch_event_detail(source_url, headers)

                raw_payload = {
                    "title": title,
                    "description": description,
                    "date_start": date_start,
                    "location_name": "Κρήτη",
                    "image_url": None,
                    "time_start": time_start,
                }

                supabase.table("raw_events").insert({
                    "source": "ticketservices.gr",
                    "source_url": source_url,
                    "raw_payload": raw_payload,
                }).execute()

                count += 1

            except Exception as e:
                print(f"Event error: {e}")
                continue

        print(f"ticketservices.gr: {count} events saved to raw_events")

    except Exception as e:
        print(f"Scrape error: {e}")

if __name__ == "__main__":
    scrape()
