import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape():
    url = "https://www.more.com/gr-el/tickets/?city=395"
    headers = {"User-Agent": "Mozilla/5.0"}

    count = 0
    try:
        r = requests.get(url, headers=headers, timeout=60)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        events = soup.select("li.play-template")

        for ev in events:
            try:
                title_meta = ev.select_one("meta[itemprop='description']")
                date_meta = ev.select_one("meta[itemprop='startDate']")
                img_meta = ev.select_one("meta[itemprop='image']")
                url_meta = ev.select_one("meta[itemprop='url']")

                if not title_meta or not url_meta:
                    continue

                title = title_meta.get("content", "").strip()
                source_url = "https://www.more.com" + url_meta.get("content", "")
                date_text = date_meta.get("content", "") if date_meta else None
                image_url = "https://www.more.com" + img_meta.get("content", "") if img_meta else None

                if not title or not source_url:
                    continue

                data = {
                    "title": title,
                    "source_url": source_url,
                    "source_name": "more.com",
                    "location": "Ηράκλειο",
                    "category": "Συναυλίες & Παραστάσεις",
                    "image_url": image_url,
                    "description": date_text,
                    "date_start": None,
                }

                supabase.table("events").upsert(data, on_conflict="source_url").execute()
                count += 1

            except Exception as e:
                print(f"Event error: {e}")
                continue

    except Exception as e:
        print(f"Scrape error: {e}")

    print(f"more.com: {count} events saved")

if __name__ == "__main__":
    scrape()
