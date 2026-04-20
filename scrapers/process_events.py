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

def normalize(text):
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)
    return text

def generate_hash(title, date_start, location):
    raw = f"{normalize(title)}{str(date_start)[:10]}{normalize(location)}"
    return hashlib.md5(raw.encode()).hexdigest()

def detect_category(title, description):
    text = f"{title or ''} {description or ''}".lower()
    if any(k in text for k in ["live", "dj", "concert", "συναυλία", "μουσική", "χορωδία", "φιλαρμονική"]):
        return "Μουσική"
    if any(k in text for k in ["θέατρο", "παράσταση", "θεατρική", "μονόλογος"]):
        return "Θέατρο"
    if any(k in text for k in ["festival", "φεστιβάλ"]):
        return "Festival"
    if any(k in text for k in ["παιδικ", "kids", "παιδιά"]):
        return "Παιδικά"
    if any(k in text for k in ["αθλητ", "αγώνας", "τουρνουά", "ποδόσφαιρο", "μπάσκετ"]):
        return "Αθλητισμός"
    if any(k in text for k in ["έκθεση", "εκθεση", "πολιτισμός", "εικαστικ"]):
        return "Πολιτισμός"
    return "Εκδηλώσεις"

def detect_tags(title, description):
    text = f"{title or ''} {description or ''}".lower()
    tags = []
    if any(k in text for k in ["δωρεάν", "δωρεαν", "free", "ελεύθερη είσοδος"]):
        tags.append("free")
    if any(k in text for k in ["παιδιά", "παιδικ", "kids", "οικογένεια"]):
        tags.append("family")
    if any(k in text for k in ["υπαίθριο", "outdoor", "πλατεία", "παραλία"]):
        tags.append("outdoor")
    if any(k in text for k in ["νυχτερινή", "nightlife", "club", "bar"]):
        tags.append("nightlife")
    return tags

def process():
    print("=== Processing raw_events ===")

    try:
        result = supabase.table("raw_events").select("*").execute()
        raw_events = result.data
    except Exception as e:
        print(f"Failed to fetch raw_events: {e}")
        return

    total = len(raw_events)
    inserted = 0
    updated = 0
    skipped = 0
    skipped_no_title = 0
    skipped_no_date = 0
    skipped_past = 0
    skipped_no_url = 0
    errors = 0

    today = datetime.now(timezone.utc).date().isoformat()

    for raw in raw_events:
        try:
            payload = raw.get("raw_payload", {})
            source = raw.get("source", "")
            source_url = raw.get("source_url", "")

            title_raw = payload.get("title", "")
            title = strip_html(title_raw).strip() if title_raw else None
            description = strip_html(payload.get("description") or "")
            date_start = payload.get("date_start")
            location = payload.get("location_name") or payload.get("location") or "Κρήτη"
            image_url = payload.get("image_url")
            time_start = payload.get("time_start")

            # Validation με logging
            if not title:
                skipped_no_title += 1
                skipped += 1
                continue

            if not source_url or not source_url.startswith("http"):
                skipped_no_url += 1
                skipped += 1
                continue

            # Date handling — δεν απορρίπτουμε events χωρίς date_start
            if date_start:
                if str(date_start)[:10] < today:
                    skipped_past += 1
                    skipped += 1
                    continue
            else:
                # Event χωρίς ημερομηνία — κρατάμε με null date
                skipped_no_date += 1
                print(f"  [NO DATE] {title[:60]} ({source})")

            category = detect_category(title, description)
            tags = detect_tags(title, description)
            is_free = "free" in tags

            # Δημιουργία date_start με ώρα αν υπάρχει time_start
            date_start_full = None
            if date_start:
                if time_start:
                    date_start_full = f"{str(date_start)[:10]}T{time_start}:00"
                else:
                    date_start_full = date_start

            event_data = {
                "title": title,
                "description": description or None,
                "date_start": date_start_full,
                "location": location,
                "image_url": image_url,
                "category": category,
                "tags": tags,
                "source_name": source,
                "source_url": source_url,
                "is_free": is_free,
                "approved": True,
                "time_start": time_start,
            }

            event_result = supabase.table("events").upsert(
                event_data,
                on_conflict="source_url"
            ).execute()

            if event_result.data:
                event_id = event_result.data[0]["id"]
                event_hash = generate_hash(title, date_start, location)

                try:
                    existing = supabase.table("event_hashes").select("id").eq("hash", event_hash).execute()
                    if not existing.data:
                        supabase.table("event_hashes").insert({
                            "hash": event_hash,
                            "event_id": event_id,
                        }).execute()
                        inserted += 1
                    else:
                        updated += 1
                except Exception:
                    updated += 1

        except Exception as e:
            errors += 1
            print(f"  [ERROR] {e}")
            continue

    # Καθαρισμός raw_events
    try:
        supabase.table("raw_events").delete().neq(
            "id", "00000000-0000-0000-0000-000000000000"
        ).execute()
        print("raw_events καθαρίστηκαν")
    except Exception as e:
        print(f"Cleanup raw_events error: {e}")

    print(f"\nΑποτελέσματα:")
    print(f"  Σύνολο raw:        {total}")
    print(f"  Νέα events:        {inserted}")
    print(f"  Ενημερώθηκαν:      {updated}")
    print(f"  Παραλείφθηκαν:     {skipped}")
    print(f"    - Χωρίς τίτλο:   {skipped_no_title}")
    print(f"    - Χωρίς URL:     {skipped_no_url}")
    print(f"    - Παλιά events:  {skipped_past}")
    print(f"    - Χωρίς ημ/νία: {skipped_no_date} (κρατήθηκαν με null date)")
    print(f"  Σφάλματα:          {errors}")

if __name__ == "__main__":
    process()
