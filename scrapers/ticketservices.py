import time
from bs4 import BeautifulSoup
from datetime import datetime
from scrapers.utils import strip_html, fetch_with_retry, insert_raw_event, report_scraper_health, get_supabase


def fetch_event_detail(url):
    """Επισκέπτεται τη σελίδα event και εξάγει time_start, date_start, location, description."""
    r = fetch_with_retry(url)
    if not r:
        return None, None, None, None

    try:
        soup = BeautifulSoup(r.text, "html.parser")

        # Ώρα και ημερομηνία από το data-time / data-date attribute
        show_li = soup.select_one("li[data-time]")
        time_start = None
        date_start = None
        if show_li:
            time_start = show_li.get("data-time", None)  # π.χ. "21:00"
            date_start = show_li.get("data-date", None)  # π.χ. "2026-06-15"

        # Venue
        venue_el = soup.select_one("h2 span.venuetitle")
        location_name = venue_el.get_text(strip=True) if venue_el else "Κρήτη"

        # Περιγραφή — πρώτο paragraph με >30 χαρακτήρες μέσα στο div#text
        description = None
        for p in soup.select("div#text p"):
            text = strip_html(p.get_text())
            if text and len(text) > 30:
                description = text
                break

        return time_start, date_start, location_name, description

    except Exception as e:
        print(f"  Detail parse error {url}: {e}")
        return None, None, None, None


def scrape():
    supabase = get_supabase()
    url = "https://www.ticketservices.gr/en/crete/"
    today = datetime.now().strftime("%Y-%m-%d")

    r = fetch_with_retry(url)
    if not r:
        report_scraper_health("ticketservices.gr", 0)
        return

    soup = BeautifulSoup(r.text, "html.parser")

    # Βρίσκουμε όλα τα event links
    event_links = []
    for a in soup.select("a[href*='/event/']"):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = "https://www.ticketservices.gr" + href
        if href not in event_links:
            event_links.append(href)

    if not event_links:
        print("  ticketservices: δεν βρέθηκαν events")
        report_scraper_health("ticketservices.gr", 0)
        return

    print(f"  ticketservices: βρέθηκαν {len(event_links)} event links")
    count = 0

    for event_url in event_links:
        try:
            time.sleep(1)
            time_start, date_start, location_name, description = fetch_event_detail(event_url)

            # Παλιά events παραλείπονται
            if date_start and date_start < today:
                continue

            # Τίτλος από το URL (fallback — θα οριστικοποιηθεί από detail page)
            r2 = fetch_with_retry(event_url)
            if not r2:
                continue
            detail_soup = BeautifulSoup(r2.text, "html.parser")
            title_el = detail_soup.select_one("h1 a.eventurl")
            if not title_el:
                title_el = detail_soup.select_one("h1")
            if not title_el:
                continue
            title = strip_html(title_el.get_text()).strip()
            if not title:
                continue

            payload = {
                "title": title,
                "description": description,
                "date_start": date_start,
                "location_name": location_name or "Κρήτη",
                "image_url": None,
                "time_start": time_start,
            }

            if insert_raw_event(supabase, "ticketservices.gr", event_url, payload):
                count += 1

        except Exception as e:
            print(f"  Event error: {e}")
            continue

    report_scraper_health("ticketservices.gr", count)


if __name__ == "__main__":
    scrape()
