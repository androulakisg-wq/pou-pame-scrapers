import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

CRETE_CITIES = ["ηράκλειο", "χανιά", "ρέθυμνο", "λασίθι", "ηρακλειο", 
                "χανια", "ρεθυμνο", "λασιθι", "crete", "κρήτη", "κρητη"]

def scrape():
    urls = [
        "https://www.more.com/gr-el/tickets/?city=395",  # Ηράκλειο
        "https://www.more.com/gr-el/tickets/?city=396",  # Χανιά
        "https://www.more.com/gr-el/tickets/?city=397",  # Ρέθυμνο
        "https://www.more.com/gr-el/tickets/?city=398",  # Λασίθι
    ]
    headers = {"User-Agent": "Mozilla/5.0"}

    count = 0
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "html.parser")
            events = soup.select("li.play-template")

            for ev in events:
                try:
                    link_el = ev.select_one("a#ItemLink")
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
                        "location": "Κρήτη",
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
            print(f"Scrape error {url}: {e}")
            continue

    print(f"more.com: {count} events saved")

if __name__ == "__main__":
    scrape()
