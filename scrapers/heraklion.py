import os
from bs4 import BeautifulSoup
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from scrapers.utils import strip_html, fetch_with_retry, insert_raw_event, report_scraper_health, get_supabase

def parse_rss_date(date_str):
    try:
        return parsedate_to_datetime(date_str).isoformat()
    except Exception:
        return None

def scrape():
    supabase = get_supabase()
    url = "https://www.heraklion.gr/rss/culture"
    headers = {"User-Agent": "Mozilla/5.0"}
    today = datetime.now(timezone.utc).date().isoformat()

    try:
        r = fetch_with_retry(url, headers=headers)
        if not r:
            report_scraper_health("heraklion.gr", 0)
            return

        soup = BeautifulSoup(r.content, "lxml-xml")
        items = soup.select("item")
        count = 0

        for item in items:
            try:
                titles = item.find_all("title")
                title = next((t.get_text(strip=True) for t in titles if t.get_text(strip=True)), None)
                guid_el = item.find("guid")
                desc_el = item.find("description")
                pub_el = item.find("pubDate")
                image_tag = item.find("image")
                img_url = image_tag.find("url").get_text(strip=True) if image_tag and image_tag.find("url") else None

                if not title or not guid_el:
                    continue

                source_url = guid_el.get_text(strip=True)
                desc_text = strip_html(desc_el.get_text(strip=True)) if desc_el else None
                date_start = parse_rss_date(pub_el.get_text(strip=True)) if pub_el else None

                if date_start and date_start[:10] < today:
                    continue

                payload = {
                    "title": title,
                    "description": desc_text,
                    "date_start": date_start,
                    "location_name": "Ηράκλειο",
                    "image_url": img_url,
                    "time_start": None,
                }

                if insert_raw_event(supabase, "heraklion.gr", source_url, payload):
                    count += 1

            except Exception as e:
                print(f"  Event error: {e}")
                continue

        report_scraper_health("heraklion.gr", count)

    except Exception as e:
        print(f"  Scrape error: {e}")
        report_scraper_health("heraklion.gr", 0)

if __name__ == "__main__":
    scrape()
