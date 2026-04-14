import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
from email.utils import parsedate_to_datetime
import re

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

KEYWORDS = [
    "εκδήλωση", "εκδηλώσεις", "συναυλία", "παράσταση",
    "φεστιβάλ", "θέατρο", "χορός", "μουσική", "festival",
    "event", "πολιτισμός", "γιορτή", "πανηγύρι",
    "έκθεση", "ημερίδα", "συνέδριο", "αθλητισμός",
    "αγώνας", "τουρνουά", "σεμινάριο", "παιδικά",
    "κινηματογράφος", "διάλεξη", "ομιλία", "αφιέρωμα",
    "πρεμιέρα", "εγκαίνια", "συναυλίες"
]

def parse_rss_date(date_str):
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except:
        return None

def strip_html(text):
    if not text:
        return None
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:500]

def scrape():
    urls = [
        "https://www.crete.gov.gr/category/anakoinoseis/feed/",
        "https://www.crete.gov.gr/category/deltia-typoy/feed/",
    ]
    headers = {"User-Agent": "Mozilla/5.0"}

    count = 0
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.content, "lxml-xml")
            items = soup.select("item")

            for item in items:
                try:
                    title_el = item.find("title")
                    link_el = item.find("link")
                    desc_el = item.find("description")
                    pub_el = item.find("pubDate")

                    if not title_el or not link_el:
                        continue

                    title = title_el.get_text(strip=True)
                    source_url = link_el.get_text(strip=True)
                    desc_text = strip_html(desc_el.get_text(strip=True)) if desc_el else ""
                    date_start = parse_rss_date(pub_el.get_text(strip=True)) if pub_el else None

                    if date_start and date_start < "2026-01-01":
                        continue

                    title_lower = title.lower()
                    desc_lower = desc_text.lower() if desc_text else ""
                    if not any(k in title_lower or k in desc_lower for k in KEYWORDS):
                        continue

                    raw_payload = {
                        "title": title,
                        "description": desc_text,
                        "date_start": date_start,
                        "location_name": "Κρήτη",
                        "image_url": None,
                    }

                    supabase.table("raw_events").insert({
                        "source": "crete.gov.gr",
                        "source_url": source_url,
                        "raw_payload": raw_payload,
                    }).execute()

                    count += 1

                except Exception as e:
                    print(f"Event error: {e}")
                    continue

        except Exception as e:
            print(f"Scrape error {url}: {e}")
            continue

    print(f"crete.gov.gr: {count} events saved to raw_events")

if __name__ == "__main__":
    scrape()
