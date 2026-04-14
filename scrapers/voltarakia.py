import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
from datetime import datetime, timedelta
import re
import time

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def strip_html(text):
    if not text:
        return None
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:500]

def fetch_with_retry(url, headers, max_retries=3):
    """Fetch URL με retry logic και exponential backoff"""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.encoding = "utf-8"
            return r
        except requests.Timeout:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1s, 2s, 4s
                print(f"Timeout για {url} — retry {attempt + 1}/{max_retries} σε {wait}s")
                time.sleep(wait)
            else:
                print(f"Αποτυχία μετά από {max_retries} προσπάθειες: {url}")
                return None
        except Exception as e:
            print(f"Error {url}: {e}")
            return None

def scrape_day(date):
    url = f"https://www.voltarakia.gr/kriti-events/eventsbyday/{date.year}/{date.month}/{date.day}/-"
    headers = {"User-Agent": "Mozilla/5.0"}

    r = fetch_with_retry(url, headers)
    if not r:
        return 0

    try:
        soup = BeautifulSoup(r.text, "html.parser")
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
                time_text = strip_html(spans[0].get_text(strip=True)) if len(spans) > 0 else None
                location = spans[2].get_text(strip=True) if len(spans) > 2 else "Κρήτη"

                raw_payload = {
                    "title": title,
                    "description": time_text,
                    "date_start": date.isoformat(),
                    "location_name": location,
                    "image_url": None,
                }

                supabase.table("raw_events").insert({
                    "source": "voltarakia.gr",
                    "source_url": source_url,
                    "raw_payload": raw_payload,
                }).execute()

                count += 1

            except Exception as e:
                print(f"Event error: {e}")
                continue

        return count

    except Exception as e:
        print(f"Parse error for {date}: {e}")
        return 0

def scrape():
    total = 0
    today = datetime.now()
    for i in range(30):
        date = today + timedelta(days=i)
        count = scrape_day(date)
        total += count
        # Μικρή παύση για να μην spam-άρουμε το site
        time.sleep(0.5)

    print(f"voltarakia.gr: {total} events saved to raw_events")

if __name__ == "__main__":
    scrape()
