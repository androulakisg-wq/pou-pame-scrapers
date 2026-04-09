from scrapers import ticketservices
from scrapers import heraklion
from scrapers import crete_gov
from scrapers import voltarakia
from scrapers import more

print("=== Πού Πάμε; — Scrapers ξεκινούν ===")

print("\n--- ticketservices.gr ---")
ticketservices.scrape()

print("\n--- heraklion.gr ---")
heraklion.scrape()

print("\n--- crete.gov.gr ---")
crete_gov.scrape()

print("\n--- voltarakia.gr ---")
voltarakia.scrape()

print("\n--- more.com ---")
more.scrape()

print("\n=== Ολοκληρώθηκε ===")
