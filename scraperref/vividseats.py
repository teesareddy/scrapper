import json
import requests
import re

event_url = "https://www.vividseats.com/shucked-tickets-fort-worth-bass-performance-hall-7-31-2025--theater-musical/production/4855476"

match = re.search(r'/production/(\d+)', event_url)
if not match:
    raise ValueError("Production ID not found in URL")

PRODUCTION_ID = int(match.group(1))

listings_url = "https://www.vividseats.com/hermes/api/v1/listings"
listings_params = {
    "productionId": PRODUCTION_ID,
    "includeIpAddress": "true",
    "currency": "USD",
    "priceGroupId": "291",
    "localizeCurrency": "true"
}

response = requests.get(listings_url, params=listings_params)
data = response.json()

global_list = data.get("global", [])
if not global_list:
    print(" No 'global' info found in response. Check if productionId is valid or API limit hit.")
    exit(1)
global_info = global_list[0]
event_title = global_info.get("productionName", "")
venue_name = global_info.get("mapTitle", "")
event_id = global_info.get("eventId", "")
performance_id = PRODUCTION_ID

details_url = f"https://www.vividseats.com/hermes/api/v1/productions/{PRODUCTION_ID}/details?currency=USD"
details_response = requests.get(details_url)
print(details_response.json())
details_data = details_response.json()

event_date_iso = details_data.get("utcDate", "") 

section_name_to_ids = {}
for section in data.get("sections", []):
    section_name = section.get("n", "").strip().lower()
    section_name_to_ids[section_name] = {
        "section_id": section.get("i", ""),
        "level_id": section.get("g", "")
    }

seats = []
view_types = set()

for ticket in data.get("tickets", []):
    section_name = ticket.get("l", "").strip().lower()
    row = ticket.get("r", "").strip()
    seat_numbers = ticket.get("m", "").split(",")
    price = f"${float(ticket.get('p', 0)):.2f}"
    pricing_zone = ticket.get("z", "")
    pack_size = int(ticket.get("q", 1))
    view_type = ticket.get("stp", "").strip()
    accessibility = ticket.get("di", False)
    badges = [badge.get("title") for badge in ticket.get("badges", [])]
    visibility_type = ticket.get("vs", None)  
    companion_seats = ticket.get("ind", False) 
    zone_id = ticket.get("z", "")

    if view_type:
        view_types.add(view_type)

    section_ids = section_name_to_ids.get(section_name, {})
    section_id = section_ids.get("section_id", "")
    zone_id = section_ids.get("level_id", "")

    for seat in seat_numbers:
        seats.append({
            "event_id": event_id,
            "performance_id": performance_id,
            "zone_id": zone_id,
            "section_id": section_id,
            "Row": row,
            "Seat": seat.strip(),
            "Price": price,
            "pricing_zone": pricing_zone,
            "pack_size": pack_size,
            "attributes": badges,
            "accessibility": accessibility,
            "visibility_type": visibility_type,
            "companion_seats": companion_seats
        })

zone_info = []
for group in data.get("groups", []):
    zone_info.append({
        "zone_id": group.get("i", ""),
        "zone_name": group.get("n", ""),
        "price_high": group.get("h", ""),
        "price_low": group.get("l", ""),
        "available_qty": group.get("q", "")
    })

sections = []
for section in data.get("sections", []):
    sections.append({
        "Section Name": section.get("n", ""),
        "Section ID": section.get("i", ""),
        "Level ID": section.get("g", "")
    })

final_output = {
    "performance_id": performance_id,
    "event_id": event_id,
    "event_title": event_title,
    "event_date": event_date_iso,
    "venue": venue_name,
    "venue_timezone": global_info.get("venueTimeZone", ""),
    "view_types": sorted(list(view_types)),
    "sections": sections,
    "zones": zone_info,
    "seats": seats
}

with open("vividseats.json", "w", encoding="utf-8") as f:
    json.dump(final_output, f, indent=2)

print("Saved to vividseats.json")
