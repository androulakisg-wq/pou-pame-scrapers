import time
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from scrapers.utils import strip_html, fetch_with_retry, insert_raw_event, report_scraper_health, get_supabase, parse_time_from_iso

def scrape():
    supabase = get_supabase()
    urls = [
        "https://www.more.com/gr-el/tickets/?city=395",
        "https://www.more.com/gr-el/tickets/?city=395&page=2",
    ]
    today = datetime.now(timezone.utc).date().isoformat()
    seen_urls = set()
    count = 0

    for url in urls:
        try:
            r = fetch_with_retry(url)
            if not r:
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            events = soup.select("li.play-template")
            print(f"  more.com: βρέθηκαν {len(events)} events στο {url}")

            for ev in events:
                try:
                    title_meta = ev.select_one("meta[itemprop='name']") or ev.select_one("meta[itemprop='description']")
                    date_meta = ev.select_one("meta[itemprop='startDate']")
                    img_meta = ev.select_one("meta[itemprop='image']")
                    url_meta = ev.select_one("meta[itemprop='url']")

                    if not title_meta or not url_meta:
                        continue

                    title = strip_html(title_meta.get("content", "").strip())
                    if not title:
                        continue

                    source_url_path = url_meta.get("content", "")
                    source_url = source_url_path if source_url_path.startswith("http") else "https://www.more.com" + source_url_path

                    if source_url in seen_urls:
                        continue
                    seen_urls.add(source_url)

                    date_str = date_meta.get("content", "") if date_meta else None

                    if date_str and date_str[:10] < today:
                        continue

                    image_url = img_meta.get("content", "") if img_meta else None
                    if image_url and not image_url.startswith("http"):
                        image_url = "https://www.more.com" + image_url

                    time_start = parse_time_from_iso(date_str)
                    date_start = date_str[:10] if date_str and len(date_str) >= 10 else None

                    payload = {
                        "title": title,
                        "description": None,
                        "date_start": date_start,
                        "location_name": "Κρήτη",
                        "image_url": image_url,
                        "time_start": time_start,
                    }

                    if insert_raw_event(supabase, "more.com", source_url, payload):
                        count += 1
                    time.sleep(0.3)

                except Exception as e:
                    print(f"  Event error: {e}")
                    continue

        except Exception as e:
            print(f"  Scrape error {url}: {e}")
            continue

    report_scraper_health("more.com", count)

if __name__ == "__main__":
    scrape()
