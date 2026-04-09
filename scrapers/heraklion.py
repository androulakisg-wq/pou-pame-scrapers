import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def scrape():
    url = "https://www.heraklion.gr/rss/culture"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "xml")
        items = soup.select("item")

        keywords = ["εκδήλωση", "εκδηλώσεις", "συναυλία", "παράσταση", 
                    "φεστιβάλ", "θέατρο", "πολιτισμός", "festival", "event"]

        count = 0
        for item in items:
            try:
                title = item.find("title").get_text(strip=True)
                source_url = item.find("link").get_text(strip=True)
                description = item.find("description")
                desc_text = description.get_text(strip=True) if description else ""
                pub_date = item.find("pubDate")
                date_text = pub_date.get_text(strip=True) if pub_date else None
                img = item.find("url")
                image_url = img.get_text(strip=True) if img else None

                title_lower = title.lower()
                if not any(k in title_lower for k in keywords):
                    continue

                data = {
                    "title": title,
                    "source_url": source_url,
                    "source_name": "heraklion.gr",
                    "location": "Ηράκλειο",
                    "category": "Πολιτισμός",
                    "image_url": image_url,
                    "description": desc_text[:500] if desc_text else date_text,
                    "date_start": None,
                }

                supabase.table("events").upsert(data, on_conflict="source_url").execute()
                count += 1

            except Exception as e:
                print(f"Event error: {e}")
                continue

        print(f"heraklion.gr: {count} events saved")

    except Exception as e:
        print(f"Scrape error: {e}")

if __name__ == "__main__":
    scrape()
