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
    raw = f"{(title or '').lower().strip()}{str(date_start)[:10]}{(location or '').lower().strip()}"
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
    if any(k in text for k in ["δωρεάν", "δωρεαν", "free", "ελεύθερη είσοδος", "ελεύθερη είσοδο"]):
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
    errors = 0

    today = datetime.now(timezone.utc).date().isoformat()

    for raw in raw_events:
        try:
            payload = raw.get("raw_payload", {})
            source = raw.get("source", "")
            source_url = raw.get("source_url", "")

            # Εξαγωγή πεδίων
            title_raw = payload.get("title", "")
            title = strip_html(title_raw).strip() if title_raw else None
            description = strip_html(payload.get("description") or "")
            date_start = payload.get("date_start")
            location = payload.get("location_name") or payload.get("location") or "Κρήτη"
            image_url = payload.get("image_url")

            # Validation
            if not title:
                skipped += 1
                continue

            if not date_start:
                skipped += 1
                continue

            # Φίλτρο παλιών events
            if str(date_start)[:10] < today:
                skipped += 1
                continue

            # Φίλτρο άδειων URLs
            if not source_url or not source_url.startswith("http"):
                skipped += 1
                continue

            # Category & Tags
            category = detect_category(title, description)
            tags = detect_tags(title, description)
            is_free = "free" in tags

            event_data = {
                "title": title,
                "description": description or None,
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

            # Upsert — ενημερώνει αν υπάρχει, εισάγει αν δεν υπάρχει
            event_result = supabase.table("events").upsert(
                event_data,
                on_conflict="source_url"
            ).execute()

            if event_result.data:
                event_id = event_result.data[0]["id"]
                event_hash = generate_hash(title, date_start, location)

                # Αποθήκευση hash μόνο αν δεν υπάρχει
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
            continue

    # Καθαρισμός raw_events μετά την επεξεργασία
    try:
        supabase.table("raw_events").delete().neq(
            "id", "00000000-0000-0000-0000-000000000000"
        ).execute()
        print("🧹 raw_events καθαρίστηκαν")
    except Exception as e:
        print(f"Cleanup raw_events error: {e}")

    print(f"\n📊 Αποτελέσματα:")
    print(f"   Σύνολο: {total}")
    print(f"   Νέα: {inserted}")
    print(f"   Ενημερώθηκαν: {updated}")
    print(f"   Παραλείφθηκαν: {skipped}")
    print(f"   Σφάλματα: {errors}")

if __name__ == "__main__":
    process()
