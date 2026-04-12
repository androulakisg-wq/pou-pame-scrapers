import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os
from email.utils import parsedate_to_datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

KEYWORDS = [
    "εκδήλωση", "εκδηλώσεις", "συναυλία", "παράσταση",
    "φεστιβάλ", "θέατρο", "χορός", "μουσική", "festival",
    "event", "πολιτισμός", "καλοκαίρι", "αποκριά", "χριστούγεννα",
    "γιορτή", "πανηγύρι", "έκθεση", "ημερίδα", "συνέδριο",
    "αθλητισμός", "αγώνας", "τουρνουά", "σεμινάριο",
    "παιδικά", "κινηματογράφος", "διάλεξη", "ομιλία",
    "συναυλίες", "αφιέρωμα", "πρεμιέρα", "εγκαίνια"
]

def parse_rss_date(date_str):
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except:
        return None

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
                    desc_text = desc_el.get_text(strip=True)[:500] if desc_el else ""
                    date_start = parse_rss_date(pub_el.get_text(strip=True)) if pub_el else None

                    title_lower = title.lower()
                    desc_lower = desc_text.lower()
                    if not any(k in title_lower or k in desc_lower for k in KEYWORDS):
                        continue

                    data = {
                        "title": title,
                        "source_url": source_url,
                        "source_name": "crete.gov.gr",
                        "location": "Κρήτη",
                        "category": "Εκδηλώσεις",
                        "image_url": None,
                        "description": desc_text,
                        "date_start": date_start,
                    }

                    supabase.table("events").upsert(data, on_conflict="source_url").execute()
                    count += 1

                except Exception as e:
                    print(f"Event error: {e}")
                    continue

        except Exception as e:
            print(f"Scrape error {url}: {e}")
            continue

    print(f"crete.gov.gr: {count} events saved")

if __name__ == "__main__":
    scrape()
