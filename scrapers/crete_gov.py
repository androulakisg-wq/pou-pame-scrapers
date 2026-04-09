import requests
from bs4 import BeautifulSoup
from supabase import create_client
import os

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

KEYWORDS = [
    "εκδήλωση", "εκδηλώσεις", "συναυλία", "παράσταση",
    "φεστιβάλ", "θέατρο", "χορός", "μουσική", "festival",
    "event", "πολιτισμός", "καλοκαίρι", "αποκριά", "χριστούγεννα"
]

def scrape():
    urls = [
        "https://www.crete.gov.gr/category/anakoinoseis/feed/",
        "https://www.crete.gov.gr/category/deltia-typoy/feed/"
    ]
    headers = {"User-Agent": "Mozilla/5.0"}

    count = 0
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.encoding = "utf-8"
            soup = BeautifulSoup(r.text, "xml")
            items = soup.select("item")

            for item in items:
                try:
                    title = item.find("title").get_text(strip=True)
                    source_url = item.find("link").get_text(strip=True)
                    description = item.find("description")
                    desc_text = description.get_text(strip=True) if description else ""
                    pub_date = item.find("pubDate")
                    date_text = pub_date.get_text(strip=True) if pub_date else None

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
                        "description": desc_text[:500] if desc_text else date_text,
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

    print(f"crete.gov.gr: {count} events saved")

if __name__ == "__main__":
    scrape()
