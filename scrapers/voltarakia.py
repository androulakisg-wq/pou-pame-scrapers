import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape():
    url = "https://www.voltarakia.gr/index.php/blank-list-kriti"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = "utf-8"
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

                spans = ev.select("span")
                time_text = spans[0].get_text(strip=True) if len(spans) > 0 else None
                location = spans[1].get_text(strip=True) if len(spans) > 1 else "Κρήτη"

                data = {
                    "title": title,
                    "source_url": source_url,
                    "source_name": "voltarakia.gr",
                    "location": location,
                    "category": "Εκδηλώσεις",
                    "image_url": None,
                    "description": time_text,
                    "date_start": None,
                }

                supabase.table("events").upsert(data, on_conflict="source_url").execute()
                count += 1

            except Exception as e:
                print(f"Event error: {e}")
                continue

        print(f"voltarakia.gr: {count} events saved")

    except Exception as e:
        print(f"Scrape error: {e}")

if __name__ == "__main__":
    scrape()
