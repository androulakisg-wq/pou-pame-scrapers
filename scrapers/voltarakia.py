import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
from datetime import datetime, timedelta

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

MONTHS_GR = {
    "Ιανουαρίου": 1, "Φεβρουαρίου": 2, "Μαρτίου": 3, "Απριλίου": 4,
    "Μαΐου": 5, "Ιουνίου": 6, "Ιουλίου": 7, "Αυγούστου": 8,
    "Σεπτεμβρίου": 9, "Οκτωβρίου": 10, "Νοεμβρίου": 11, "Δεκεμβρίου": 12
}

def parse_greek_date(date_str):
    try:
        date_str = date_str.replace(",", "").strip()
        parts = date_str.split()
        day = int(parts[1])
        month = MONTHS_GR.get(parts[2], 0)
        year = int(parts[3])
        return datetime(year, month, day).isoformat()
    except:
        return None

def scrape_day(date):
    date_str = date.strftime("%Y-%m-%d")
    url = f"https://www.voltarakia.gr/index.php/blank-list-kriti?date={date_str}"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")

        date_el = soup.select_one("div.currentmonth")
        page_date = parse_greek_date(date_el.get_text(strip=True)) if date_el else date.isoformat()

        events = soup.select("li.ev_td_li")
        count = 0

        for ev in events:
            try:
                link_el = ev.select_one("a.ev_link_row")
                if not link_el:
                    continue

                title = link_el.get("title", "").strip()
                if not title:
                    title = link_el.get_text(strip=True)

                source_url = link_el.get("href", "")
                if source_url.startswith("/"):
                    source_url = "https://www.voltarakia.gr" + source_url
                elif not source_url.startswith("http"):
                    source_url = "https://www.voltarakia.gr/" + source_url

                spans = ev.select("span")
                time_text = spans[0].get_text(strip=True) if len(spans) > 0 else None
                location = spans[2].get_text(strip=True) if len(spans) > 2 else "Κρήτη"

                data = {
                    "title": title,
                    "source_url": source_url,
                    "source_name": "voltarakia.gr",
                    "location": location,
                    "category": "Εκδηλώσεις",
                    "image_url": None,
                    "description": time_text,
                    "date_start": page_date,
                }

                supabase.table("events").upsert(data, on_conflict="source_url").execute()
                count += 1

            except Exception as e:
                print(f"Event error: {e}")
                continue

        return count

    except Exception as e:
        print(f"Scrape error for {date_str}: {e}")
        return 0

def scrape():
    total = 0
    today = datetime.now()
    # Scrape σήμερα + 30 επόμενες μέρες
    for i in range(31):
        date = today + timedelta(days=i)
        count = scrape_day(date)
        total += count
    
    print(f"voltarakia.gr: {total} events saved")

if __name__ == "__main__":
    scrape()
