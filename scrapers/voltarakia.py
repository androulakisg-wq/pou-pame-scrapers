import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from scrapers.utils import strip_html, fetch_with_retry, insert_raw_event, report_scraper_health, get_supabase, parse_time


def scrape_day(supabase, date):
    url = f"https://www.voltarakia.gr/kriti-events/eventsbyday/{date.year}/{date.month}/{date.day}/-"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = fetch_with_retry(url, headers=headers)
    if not r:
        return 0
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        events = soup.select("li.ev_td_li")
        count = 0
        for ev in events:
            try:
                link_el = ev.select_one("a.ev_link_row")
                if not link_el:
                    continue
                title = link_el.get("title", "").strip()
                if not title:
                    title = link_el.get_text(strip=True)
                source_url = link_el.get("href", "")
                if source_url.startswith("/"):
                    source_url = "https://www.voltarakia.gr" + source_url
                elif not source_url.startswith("http"):
                    source_url = "https://www.voltarakia.gr/" + source_url
                spans = ev.select("span")
                time_text = strip_html(spans[0].get_text(strip=True)) if len(spans) > 0 else None
                location = spans[2].get_text(strip=True) if len(spans) > 2 else "Κρήτη"
                time_start = parse_time(time_text)
                payload = {
                    "title": title,
                    "description": None,
                    "date_start": date.isoformat(),
                    "location_name": location,
                    "image_url": None,
                    "time_start": time_start,
                }
                if insert_raw_event(supabase, "voltarakia.gr", source_url, payload):
                    count += 1
            except Exception as e:
                print(f"  Event error: {e}")
                continue
        return count
    except Exception as e:
        print(f"  Parse error for {date}: {e}")
        return 0


def scrape():
    supabase = get_supabase()
    total = 0
    today = datetime.now()
    for i in range(30):
        date = today + timedelta(days=i)
        count = scrape_day(supabase, date)
        total += count
        time.sleep(0.5)
    report_scraper_health("voltarakia.gr", total)


if __name__ == "__main__":
    scrape()
