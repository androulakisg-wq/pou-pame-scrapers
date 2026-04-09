import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape():
    url = "https://www.ticketservices.gr/en/crete/"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Charset": "utf-8"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = "iso-8859-7"
        soup = BeautifulSoup(r.text, "html.parser")
        events = soup.select("li.event")

        count = 0
        for ev in events:
            try:
                link_el = ev.select_one("a")
                title_el = ev.select_one("h5 span")
                location_el = ev.select_one("h5 em")
                date_el = ev.select_one("span.dates")
                img_el = ev.select_one("img")

                if not title_el or not link_el:
                    continue

                title = title_el.get_text(strip=True)
                source_url = link_el.get("href", "")
                if source_url.startswith("/"):
                    source_url = "https://www.ticketservices.gr" + source_url

                location = location_el.get_text(strip=True) if location_el else "Κρήτη"
                date_text = date_el.get_text(strip=True) if date_el else None
                image_url = img_el.get("src") if img_el else None

                data = {
                    "title": title,
                    "source_url": source_url,
                    "source_name": "ticketservices.gr",
                    "location": location,
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

        print(f"ticketservices.gr: {count} events saved")

    except Exception as e:
        print(f"Scrape error: {e}")

if __name__ == "__main__":
    scrape()
