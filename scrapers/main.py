from scrapers import ticketservices, heraklion, crete_gov, voltarakia, more
from supabase import create_client
import os
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def cleanup_old_events():
    """Διαγραφή παλιών events αυτόματα"""
    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        today = datetime.now(timezone.utc).date().isoformat()
        
        result = supabase.table("events")\
            .delete()\
            .lt("date_start", today)\
            .neq("source_name", "user_submitted")\
            .execute()
        
        print(f"🧹 Διαγράφηκαν παλιά events")
    except Exception as e:
        print(f"Cleanup error: {e}")

def main():
    print("=== Πού Πάμε; — Scrapers ξεκινούν ===\n")

    # Καθαρισμός παλιών events πριν το scraping
    cleanup_old_events()

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

    print("\n=== Ολοκληρώθηκε ===")

if __name__ == "__main__":
    main()
