from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from scrapers.utils import strip_html, fetch_with_retry, insert_raw_event, report_scraper_health, get_supabase

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
    except Exception:
        return None

def scrape():
    supabase = get_supabase()
    urls = [
        "https://www.crete.gov.gr/category/anakoinoseis/feed/",
        "https://www.crete.gov.gr/category/deltia-typoy/feed/",
    ]
    headers = {"User-Agent": "Mozilla/5.0"}
    today = datetime.now(timezone.utc).date().isoformat()
    count = 0

    for url in urls:
        try:
            r = fetch_with_retry(url, headers=headers)
            if not r:
                continue

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

                    if date_start and date_start[:10] < today:
                        continue

                    title_lower = title.lower()
                    desc_lower = desc_text.lower() if desc_text else ""
                    if not any(k in title_lower or k in desc_lower for k in KEYWORDS):
                        continue

                    payload = {
                        "title": title,
                        "description": desc_text,
                        "date_start": date_start,
                        "location_name": "Κρήτη",
                        "image_url": None,
                        "time_start": None,
                    }

                    if insert_raw_event(supabase, "crete.gov.gr", source_url, payload):
                        count += 1

                except Exception as e:
                    print(f"  Event error: {e}")
                    continue

        except Exception as e:
            print(f"  Scrape error {url}: {e}")
            continue

    report_scraper_health("crete.gov.gr", count)

if __name__ == "__main__":
    scrape()
