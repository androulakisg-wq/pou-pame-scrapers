import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
import re
import time
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def strip_html(text):
    if not text:
        return None
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:500]

def parse_time(date_str):
    """Εξάγει ώρα HH:MM από ISO datetime string όπως 2026-04-19T22:00:00"""
    if not date_str:
        return None
    match = re.search(r'T(\d{2}:\d{2})', date_str)
    if match:
        t = match.group(1)
        return None if t == "00:00" else t
    return None

def scrape():
    urls = [
        "https://www.more.com/gr-el/tickets/?city=395",
        "https://www.more.com/gr-el/tickets/?city=395&page=2",
    ]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "el-GR,el;q=0.9",
        "Accept": "text/html,application/xhtml+xml",
    }
    today = datetime.now(timezone.utc).date().isoformat()
    seen_urls = set()
    count = 0

    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=60)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            events = soup.select("li.play-template")

            print(f"more.com: βρέθηκαν {len(events)} events στο {url}")

            for ev in events:
                try:
                    title_meta = ev.select_one("meta[itemprop='name']") or ev.select_one("meta[itemprop='description']")
                    date_meta = ev.select_one("meta[itemprop='startDate']")
                    img_meta = ev.select_one("meta[itemprop='image']")
                    url_meta = ev.select_one("meta[itemprop='url']")

                    if not title_meta or not url_meta:
                        continue

                    title = strip_html(title_meta.get("content", "").strip())
                    if not title:
                        continue

                    source_url_path = url_meta.get("content", "")
                    if not source_url_path.startswith("http"):
                        source_url = "https://www.more.com" + source_url_path
                    else:
                        source_url = source_url_path

                    # Αποφυγή duplicates στο ίδιο run
                    if source_url in seen_urls:
                        continue
                    seen_urls.add(source_url)

                    date_str = date_meta.get("content", "") if date_meta else None

                    # Φίλτρο παλιών events
                    if date_str and date_str[:10] < today:
                        continue

                    image_url = img_meta.get("content", "") if img_meta else None
                    if image_url and not image_url.startswith("http"):
                        image_url = "https://www.more.com" + image_url

                    time_start = parse_time(date_str)
                    date_start = date_str[:10] if date_str and len(date_str) >= 10 else None

                    raw_payload = {
                        "title": title,
                        "description": None,
                        "date_start": date_start,
                        "location_name": "Κρήτη",
                        "image_url": image_url,
                        "time_start": time_start,
                    }

                    supabase.table("raw_events").insert({
                        "source": "more.com",
                        "source_url": source_url,
                        "raw_payload": raw_payload,
                    }).execute()

                    count += 1
                    time.sleep(0.3)

                except Exception as e:
                    print(f"Event error: {e}")
                    continue

        except Exception as e:
            print(f"Scrape error {url}: {e}")
            continue

    print(f"more.com: {count} events saved to raw_events")

if __name__ == "__main__":
    scrape()
