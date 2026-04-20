import time
import re
from bs4 import BeautifulSoup
from datetime import datetime
from scrapers.utils import strip_html, fetch_with_retry, insert_raw_event, report_scraper_health, get_supabase, parse_time

MONTHS_GR = {
    "Ιανουαρίου": 1, "Φεβρουαρίου": 2, "Μαρτίου": 3,
    "Απριλίου": 4, "Μαΐου": 5, "Ιουνίου": 6,
    "Ιουλίου": 7, "Αυγούστου": 8, "Σεπτεμβρίου": 9,
    "Οκτωβρίου": 10, "Νοεμβρίου": 11, "Δεκεμβρίου": 12
}

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

def fetch_event_detail(url):
    """Επισκέπτεται τη σελίδα event και εξάγει time_start + description"""
    r = fetch_with_retry(url)
    if not r:
        return None, None

    try:
        soup = BeautifulSoup(r.text, "html.parser")

        time_start = None
        for selector in [".event-time", ".time", "[class*='time']", ".event-details", ".info"]:
            el = soup.select_one(selector)
            if el:
                time_start = parse_time(el.get_text())
                if time_start:
                    break

        if not time_start:
            time_start = parse_time(soup.get_text())

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
        print(f"  Detail parse error {url}: {e}")
        return None, None

def scrape():
    supabase = get_supabase()
    url = "https://www.ticketservices.gr/page/results/?q=%CE%BA%CF%81%CE%B7%CF%84%CE%B7"
    today = datetime.now().strftime("%Y-%m-%d")

    r = fetch_with_retry(url)
    if not r:
        report_scraper_health("ticketservices.gr", 0)
        return

    soup = BeautifulSoup(r.text, "html.parser")
    events = (
        soup.select("div.eventsnewr") or
        soup.select("article.event") or
        soup.select("div.event-item") or
        soup.select("li.event")
    )

    if not events:
        report_scraper_health("ticketservices.gr", 0)
        return

    print(f"  ticketservices: βρέθηκαν {len(events)} events")
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

            if date_start and date_start < today:
                continue

            time.sleep(1)
            time_start, description = fetch_event_detail(source_url)

            payload = {
                "title": title,
                "description": description,
                "date_start": date_start,
                "location_name": "Κρήτη",
                "image_url": None,
                "time_start": time_start,
            }

            if insert_raw_event(supabase, "ticketservices.gr", source_url, payload):
                count += 1

        except Exception as e:
            print(f"  Event error: {e}")
            continue

    report_scraper_health("ticketservices.gr", count)

if __name__ == "__main__":
    scrape()
