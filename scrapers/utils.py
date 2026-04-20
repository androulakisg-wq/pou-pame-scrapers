import os
import re
import time
import requests
from supabase import create_client

# ── Supabase client ──────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Text helpers ─────────────────────────────────────────────
def strip_html(text, max_len=500):
    if not text:
        return None
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text[:max_len]

def normalize(text):
    """Lowercase + strip + collapse whitespace — για deduplication"""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text.lower().strip())

def parse_time(text):
    """Εξάγει ώρα HH:MM από οποιοδήποτε string"""
    if not text:
        return None
    match = re.search(r'\b(\d{1,2}:\d{2})\b', str(text))
    if match:
        t = match.group(1)
        return None if t in ("00:00", "07:13", "13:43") else t
    return None

def parse_time_from_iso(date_str):
    """Εξάγει ώρα HH:MM από ISO datetime string όπως 2026-04-19T22:00:00"""
    if not date_str:
        return None
    match = re.search(r'T(\d{2}:\d{2})', str(date_str))
    if match:
        t = match.group(1)
        return None if t == "00:00" else t
    return None

# ── HTTP helpers ─────────────────────────────────────────────
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "el-GR,el;q=0.9",
    "Accept": "text/html,application/xhtml+xml",
}

def fetch_with_retry(url, headers=None, max_retries=3, timeout=30):
    """Fetch URL με retry logic και exponential backoff"""
    if headers is None:
        headers = DEFAULT_HEADERS
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.encoding = "utf-8"
            return r
        except requests.Timeout:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"  Timeout {url} — retry {attempt+1}/{max_retries} σε {wait}s")
                time.sleep(wait)
            else:
                print(f"  Αποτυχία μετά από {max_retries} προσπάθειες: {url}")
                return None
        except Exception as e:
            print(f"  Error fetching {url}: {e}")
            return None

# ── Supabase insert helper ───────────────────────────────────
def insert_raw_event(supabase, source, source_url, payload):
    """Insert ένα event στο raw_events table"""
    try:
        supabase.table("raw_events").insert({
            "source": source,
            "source_url": source_url,
            "raw_payload": payload,
        }).execute()
        return True
    except Exception as e:
        print(f"  Insert error [{source}] {source_url}: {e}")
        return False

# ── Health check helper ──────────────────────────────────────
def report_scraper_health(source, count, expected_min=1):
    """Log αποτέλεσμα scraper — προειδοποίηση αν 0 events"""
    if count == 0:
        print(f"  WARNING: {source} επέστρεψε 0 events — πιθανή αλλαγή HTML ή αποτυχία σύνδεσης")
    else:
        print(f"  {source}: {count} events saved to raw_events")
