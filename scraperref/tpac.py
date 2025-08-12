import requests
import json
import re
import time
from collections import defaultdict
from requests.exceptions import RequestException, ConnectionError

BASE_URL = "https://cart.tpac.org/14390/14430"
HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 OPR/119.0.0.0",
    "x-requested-with": "XMLHttpRequest",
}


def get_performance_id(url: str) -> str:
    match = re.search(r"/(\d+)$", url)
    if match:
        return match.group(1)
    return None


PERFORMANCE_ID = get_performance_id(BASE_URL)


def safe_get(url, max_retries=3, timeout=60):
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response
        except (ConnectionError, RequestException) as e:
            print(f"Attempt {attempt}/{max_retries} failed for {url}: {e}")
            time.sleep(2)
    raise Exception(f"Failed to connect after {max_retries} attempts: {url}")


def get_event_details(perf_id):
    url = f"{BASE_URL}/GetPerformanceDetails?performanceId={perf_id}"
    return safe_get(url).json()


def get_screens(perf_id):
    url = f"{BASE_URL}/GetScreens?performanceId={perf_id}"
    return safe_get(url).json()


def get_seat_list(perf_id, facility_id, screen_id):
    url = f"{BASE_URL}/GetSeatList?performanceId={perf_id}&facilityId={facility_id}&screenId={screen_id}"
    return safe_get(url).json()


def extract_all_seats(perf_id):
    perf = get_event_details(perf_id)
    screens = get_screens(perf_id)

    facility_id = perf.get("facility_no")
    event_title = perf.get("description")
    date_time = perf.get("perf_dt")
    venue = perf.get("facility_desc")
    timezone = "America/Chicago"

    view_types = set()
    zones = {}
    sections = []
    all_seats = []

    for screen in screens:
        screen_id = screen["screen_no"]
        screen_label = screen["screen_desc"]
        sections.append(
            {
                "Section Name": screen_label,
                "Section ID": str(screen_id),
                "Level ID": str(facility_id),
            }
        )

        seat_data = get_seat_list(perf_id, facility_id, screen_id)

        zone_prices = {
            item["ZoneNo"]: item["Price"]
            for item in seat_data.get("AvailablePrices", [])
        }
        zone_names = {
            item["ZoneNo"]: item.get("ZoneDesc", f"Zone {item['ZoneNo']}")
            for item in seat_data.get("AvailablePrices", [])
        }

        row_grouped = defaultdict(list)

        for seat in seat_data.get("seats", []):
            if seat.get("seat_status_desc") != "Available":
                continue

            zone_no = seat.get("zone_no")
            row = seat.get("seat_row", "").strip()
            seat_num = seat.get("seat_num", "").strip()
            price = zone_prices.get(zone_no, 0.0)

            zones.setdefault(
                zone_no,
                {
                    "zone_id": str(zone_no),
                    "zone_name": zone_names.get(zone_no, f"Zone {zone_no}"),
                    "price_high": price,
                    "price_low": price,
                    "available_qty": 0,
                },
            )

            zone_info = zones[zone_no]
            zone_info["available_qty"] += 1
            zone_info["price_low"] = min(zone_info["price_low"], price)
            zone_info["price_high"] = max(zone_info["price_high"], price)

            row_grouped[(screen_id, row)].append(
                {
                    "seat_num": seat_num,
                    "price": price,
                    "zone_id": str(zone_no),
                    "section_id": str(screen_id),
                    "Row": row,
                    "screen_label": screen_label,
                    "pricing_zone": seat_data.get("DefaultPrice", ""),
                    "accessibility": seat.get("accessible_ind", False),
                }
            )

        for (section_id, row), seats in row_grouped.items():
            sorted_seats = sorted(
                seats, key=lambda x: int("".join(filter(str.isdigit, x["seat_num"])))
            )

            i = 0
            while i < len(sorted_seats):
                group = [sorted_seats[i]]
                j = i + 1
                while j < len(sorted_seats):
                    prev = int(
                        "".join(filter(str.isdigit, sorted_seats[j - 1]["seat_num"]))
                    )
                    curr = int(
                        "".join(filter(str.isdigit, sorted_seats[j]["seat_num"]))
                    )
                    if curr == prev + 1:
                        group.append(sorted_seats[j])
                        j += 1
                    else:
                        break
                for seat in group:
                    all_seats.append(
                        {
                            "event_id": f"tpac-{perf_id}",
                            "performance_id": perf_id,
                            "zone_id": seat["zone_id"],
                            "section_id": seat["section_id"],
                            "Row": seat["Row"],
                            "Seat": seat["seat_num"],
                            "Price": f"${seat['price']:.2f}",
                            "pricing_zone": seat["pricing_zone"],
                            "pack_size": len(group),
                            "attributes": [],
                            "accessibility": seat["accessibility"],
                            "visibility_type": None,
                            "companion_seats": False,
                        }
                    )
                i = j

    final_output = {
        "performance_id": perf_id,
        "event_id": f"tpac-{perf_id}",
        "event_title": event_title,
        "event_date": date_time,
        "venue": venue,
        "venue_timezone": timezone,
        "view_types": sorted(list(view_types)),
        "sections": sections,
        "zones": list(zones.values()),
        "seats": all_seats,
    }

    return final_output


if __name__ == "__main__":
    result = extract_all_seats(PERFORMANCE_ID)
    with open("tpac.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print("Saved to tpac.json")
