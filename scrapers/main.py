from scrapers import ticketservices, heraklion, crete_gov, voltarakia, more
from scrapers import process_events
from supabase import create_client
import os
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def cleanup_old_events():
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        today = datetime.now(timezone.utc).date().isoformat()
        supabase.table("events")\
            .delete()\
            .lt("date_start", today)\
            .neq("source_name", "user_submitted")\
            .execute()
        print("🧹 Διαγράφηκαν παλιά events")
    except Exception as e:
        print(f"Cleanup error: {e}")

def main():
    print("=== Πού Πάμε; — Scrapers ξεκινούν ===\n")

    # Βήμα 1: Καθαρισμός παλιών events
    cleanup_old_events()

    # Βήμα 2: Scraping → raw_events
    print("--- ticketservices.gr ---")
    ticketservices.scrape()

    print("--- heraklion.gr ---")
    heraklion.scrape()

    print("--- crete.gov.gr ---")
    crete_gov.scrape()

    print("--- voltarakia.gr ---")
    voltarakia.scrape()

    print("--- more.com ---")
    more.scrape()

    # Βήμα 3: Processing → events
    print("\n--- Processing raw events ---")
    process_events.process()

    print("\n=== Ολοκληρώθηκε ===")

if __name__ == "__main__":
    main()
