import os
import hashlib
import re
from supabase import create_client
from datetime import datetime, timezone

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def strip_html(text):
    if not text:
        return None
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:500]

def generate_hash(title, date_start, location):
    raw = f"{(title or '').lower().strip()}{str(date_start)}{(location or '').lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()

def detect_category(title, description):
    text = f"{title or ''} {description or ''}".lower()
    if any(k in text for k in ["live", "dj", "concert", "συναυλία", "μουσική"]):
        return "Μουσική"
    if any(k in text for k in ["theatre", "θέατρο", "παράσταση"]):
        return "Θέατρο"
    if any(k in text for k in ["festival", "φεστιβάλ"]):
        return "Festival"
    if any(k in text for k in ["παιδικ", "kids"]):
        return "Παιδικά"
    if any(k in text for k in ["αθλητ", "αγώνας", "τουρνουά"]):
        return "Αθλητισμός"
    if any(k in text for k in ["πολιτισμός", "έκθεση", "εκθεση"]):
        return "Πολιτισμός"
    return "Εκδηλώσεις"

def detect_tags(title, description):
    text = f"{title or ''} {description or ''}".lower()
    tags = []
    if any(k in text for k in ["δωρεάν", "δωρεαν", "free", "ελεύθερη είσοδος"]):
        tags.append("free")
    if any(k in text for k in ["παιδιά", "παιδικ", "kids", "οικογένεια"]):
        tags.append("family")
    if any(k in text for k in ["υπαίθριο", "outdoor", "πλατεία"]):
        tags.append("outdoor")
    if any(k in text for k in ["νυχτερινή", "nightlife", "club"]):
        tags.append("nightlife")
    return tags

def process():
    print("=== Processing raw_events ===")

    result = supabase.table("raw_events").select("*").execute()
    raw_events = result.data

    total = len(raw_events)
    inserted = 0
    skipped = 0
    errors = 0

    today = datetime.now(timezone.utc).date().isoformat()

    for raw in raw_events:
        try:
            payload = raw.get("raw_payload", {})
            source = raw.get("source", "")
            source_url = raw.get("source_url", "")

            title = strip_html(payload.get("title", "")).strip() if payload.get("title") else None
            description = strip_html(payload.get("description", ""))
            date_start = payload.get("date_start")
            location = payload.get("location_name") or payload.get("location")
            image_url = payload.get("image_url")

            # Validation
            if not title or not date_start:
                skipped += 1
                continue

            # Φίλτρο παλιών events
            if str(date_start)[:10] < today:
                skipped += 1
                continue

            # Deduplication
            event_hash = generate_hash(title, date_start, location)
            existing = supabase.table("event_hashes").select("id").eq("hash", event_hash).execute()
            if existing.data:
                skipped += 1
                continue

            # Category & Tags
            category = detect_category(title, description)
            tags = detect_tags(title, description)
            is_free = "free" in tags

            # Insert στο events table — χρησιμοποιούμε "location" όχι "location_name"
            event_data = {
                "title": title,
                "description": description,
                "date_start": date_start,
                "location": location,
                "image_url": image_url,
                "category": category,
                "tags": tags,
                "source_name": source,
                "source_url": source_url,
                "is_free": is_free,
                "approved": True,
            }

            event_result = supabase.table("events").insert(event_data).execute()

            if event_result.data:
                event_id = event_result.data[0]["id"]

                supabase.table("event_hashes").insert({
                    "hash": event_hash,
                    "event_id": event_id,
                }).execute()

                inserted += 1

        except Exception as e:
            print(f"Error: {e}")
            errors += 1
            continue

    # Καθαρισμός raw_events
    supabase.table("raw_events").delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()

    print(f"\n📊 Αποτελέσματα:")
    print(f"   Σύνολο: {total}")
    print(f"   Εισήχθησαν: {inserted}")
    print(f"   Παραλείφθηκαν: {skipped}")
    print(f"   Σφάλματα: {errors}")

if __name__ == "__main__":
    process()
