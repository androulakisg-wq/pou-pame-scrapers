import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
from email.utils import parsedate_to_datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_rss_date(date_str):
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except:
        return None

def scrape():
    url = "https://www.heraklion.gr/rss/culture"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.content, "lxml-xml")
        items = soup.select("item")

        count = 0
        for item in items:
            try:
                # Παίρνουμε όλα τα title tags και κρατάμε το μη-άδειο
                titles = item.find_all("title")
                title = next((t.get_text(strip=True) for t in titles if t.get_text(strip=True)), None)

                guid_el = item.find("guid")
                desc_el = item.find("description")
                pub_el = item.find("pubDate")
                
                # Παίρνουμε image url από το image tag
                image_tag = item.find("image")
                img_url = image_tag.find("url").get_text(strip=True) if image_tag and image_tag.find("url") else None

                if not title or not guid_el:
                    continue

                source_url = guid_el.get_text(strip=True)
                desc_text = desc_el.get_text(strip=True)[:500] if desc_el else ""
                date_start = parse_rss_date(pub_el.get_text(strip=True)) if pub_el else None

                data = {
                    "title": title,
                    "source_url": source_url,
                    "source_name": "heraklion.gr",
                    "location": "Ηράκλειο",
                    "category": "Πολιτισμός",
                    "image_url": img_url,
                    "description": desc_text,
                    "date_start": date_start,
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
