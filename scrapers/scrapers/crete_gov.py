import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape():
    url = "https://www.crete.gov.gr/category/ekdiloseis/"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")
        events = soup.select("article") or soup.select("div.post") or soup.select("div.event")

        count = 0
        for ev in events:
            try:
                title_el = ev.select_one("h2") or ev.select_one("h3") or ev.select_one(".entry-title")
                link_el = ev.select_one("a")
                date_el = ev.select_one(".date") or ev.select_one("time") or ev.select_one(".entry-date")
                img_el = ev.select_one("img")

                if not title_el or not link_el:
                    continue

                title = title_el.get_text(strip=True)
                source_url = link_el.get("href", "")
                if source_url.startswith("/"):
                    source_url = "https://www.crete.gov.gr" + source_url

                date_text = date_el.get_text(strip=True) if date_el else None
                image_url = img_el.get("src") if img_el else None

                data = {
                    "title": title,
                    "source_url": source_url,
                    "source_name": "crete.gov.gr",
                    "location": "Κρήτη",
                    "category": "Εκδηλώσεις",
                    "image_url": image_url,
                    "description": date_text,
                    "date_start": None,
                }

                supabase.table("events").upsert(data, on_conflict="source_url").execute()
                count += 1

            except Exception as e:
                print(f"Event error: {e}")
                continue

        print(f"crete.gov.gr: {count} events saved")

    except Exception as e:
        print(f"Scrape error: {e}")

if __name__ == "__main__":
    scrape()
