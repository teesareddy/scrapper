import json
import time
from collections import defaultdict

import requests
from bs4 import BeautifulSoup

EVENT_URL = "https://tickets.coloradoballet.org/9736/9741"

HEADERS = {
    "accept": "application/json, text/javascript, text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0",
    "x-requested-with": "XMLHttpRequest",
    "cookie": "_gcl_au=1.1.2038505759.1753365916; incap_ses_1841_2525815=lK0teSgTSG4p/Am/PIyMGa49gmgAAAAAAVAPGhLXlwOA31emqQH+NQ==; .AspNetCore.Antiforgery.6YJ0z8OwHIY=CfDJ8CEuhXfxdM9GpnBbN_HAE8Vn1O0FAshWEIPNT-2QZ4bSAk2yRvIcO1gPhjkclDULfmzhG37emWQUGnldw07ivAY1tfn2D0dFPOWi66teWLXQnDyQf4L7uVs8tSAjl-i-hC-tqWDXBulXfF2L1JEsMCI; _gid=GA1.2.1373767520.1753365947; _fbp=fb.1.1753365950173.714975336757727187; __qca=P1-1f1d81a1-57f5-4a1c-8068-15d18c5acf24; incap_ses_172_2525815=PAg5eCcLNStijoJ/jRFjAlhngmgAAAAAse2tUUYEMxwZ7c4WdHMFlw==; incap_ses_713_2525815=fBz1Kl2yX1swtYRoGBblCepygmgAAAAAcvBE/wAYB8BNSpncLXtHHQ==; reese84=3:3g3gHchoNwuh9yCG39uMJA==:jb6r165oGDcR1EEhGbYf7TlyGbXwv3lnz7dOzYW+a3ujONT7qcA0E5KEH4j1uPabsWYeoyq0UMid4ueC4SLXcHEB70uuuXnLmpDDhGkagpjQcbyk4joIRRcYxMADj4o/HBviXfLethEFQkoGVLS0TYtQkLn4yikd1tAVMn4vfnsx6DUI4j7cXGRtpIi1YMTPoEEG6tuj0KdNrTcFHP7z28L77qLR1obQctdHyX96rjQGjHr7zJKBHxYm+4YKsTwoXx4kmfaAPDdUOq6lX9LcH+IdNdi9ZEWoN6+dY4VFApRdrdwRQqbMEzsEYjuhuwh616b6xZyw9CrIRBtH30T+v72GBKviWGMhEQn3at0dhuIQUETlW81WBE5uwaWG6PkmIlTOJoRp8hQWXGjlMpEUgeZSNTb53k7hCBCE8s4ZA+L7/gwMpLqKvGcOanl09oCgDL+zaCuFKMuZTTwxQ8kRc7b0+t8uIblTtRswb2uRS5o=:Qq2o1At4TyTvm5RwoMJE5CNkEbmZALxob6C8ZSwb/uY=; nlbi_2525815=XCvXOuPGrjQhDTru0LL8WwAAAAB7/5NWGFi3Io4QfzEgw/u0; incap_sh_2525815=8HKCaAAAAACO4gcFDAAI8OWJxAYQ7OWJxAbrALnjoI61hCXLQvqKrOXe; .AspNetCore.Identity.Application=CfDJ8CEuhXfxdM9GpnBbN_HAE8WM081lUh8Pgdg1nu3vubflaXPlx2rn8cAZ9FcXROZre1Oken7OfDKXSv6qdxp5pxfwOsl4_P_tjCgEMeHwYRvZR3dwsg03tUOFmwSf21DStqBsMhbyTxK0SecP_Ly4hH_2ty0vlGLZrkrJp7-ba6vpQ_Ty_nOe70ADYwyHG3QRVtxcxEBT0kpdg4P9ottxx2cgc2FdgEAGSaRbyM0tzvOh2CzIAICjY4GOjiWi1fEG96kAOKISqLY-2acUUIohL6p_MyqJYNyQAeGz7lso3TmhGP5IkhWiMXLA0v6czszk0CiLxqk8AiUqbvKcHJzPQB5Z_8U8juVHH_400JJbkDyXAMPAO9LqURRG3C-ZSk8-7RwBBgXpPIWu_cABOEVyB8Lr50iSndRmouG8ERh7sAsfVZJYZpKkEDMls1B6x02rSG5ZmNEBMEHHxZJqNxKyHLlFW03MPwfKJw9VlGfyPRe3UAZN29GgX5XGJq8dyhMTot7bGgofqqanLXpTbKP9-9sThupbubCUbozo7yEwUUTcO9inkg8bmv6tefOKMGZs_cDbUsj7mXSxdED2QXAWQwUeyeKcKEhQ3PK4V9DcpNNaWKz5MOqHSV-2X0lrwAbj8msO8qLkrhm9Ko2r73bLch1MrMzl3_Zal3nG_F7tYZUu; visid_incap_2525815=TCk9EvJkTMuhQNgVyeXuua09gmgAAAAAQ0IPAAAAAACAUOi9AWre5FCS/NtjE7/deKzQH/n9thrE; _gat_gtag_UA_47538041_1=1; _ga_H465QVKWXL=GS2.1.s1753379564$o3$g1$t1753379575$j49$l0$h0; _ga=GA1.1.1954717270.1753365946; _dc_gtm_UA-47538041-2=1; _ga_R62T7MM40D=GS2.1.s1753379564$o3$g1$t1753379575$j49$l0$h0; nlbi_2525815_2147483392=QnvVcMXHITSygrDl0LL8WwAAAAA5qmLu37X1LBeyd/+hTUeS; _ga_KWE803DCC2=GS2.2.s1753379576$o3$g0$t1753379576$j60$l0$h0; _uetsid=4cccf420689711f0882a87cfb3add15d; _uetvid=4ccd3100689711f0bdd6f3e6ba6a4e42; TNEW=N4o4gGqtztXLbDJR/FsvZ+ZbB9jFzsN3zyJhetg+xkniZsEBOYq8YuoPnRUCE5nnYYCVUaJXY9+foUPq1BXx0Oezw5m7TM6l0oN9zacQhgu1oyLpz8U4PLRIUXbb156BOrKO8yLqCleN4vmu0aeBlKDYtKC6WgKmyKMkL5MKrubdyR4Jl7yxgAoPqmKhePxQ; _gali=tn-syos-screen-list",
}
BASE_URL = "https://tickets.coloradoballet.org/api/syos"


def get_perfomance_id():
    return EVENT_URL.split("/")[-1]


PERFORMANCE_ID = get_perfomance_id()


def get_event_details_from_web():
    try:
        r = requests.get(EVENT_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.select_one(".tn-event-detail__title")
        date = soup.select_one(".tn-event-detail__display-time")
        venue = soup.select_one(".tn-event-detail__location")

        print("Title:", title.text.strip() if title else None)
        print("Date:", date.text.strip() if date else None)
        print("Venue:", venue.text.strip() if venue else None)

        return {
            "E": title.text.strip() if title else "Colorado Ballet Event",
            "DT": date.text.strip() if date else "Unknown",
            "V": venue.text.strip() if venue else "Unknown",
        }
    except Exception as e:
        print(f"[Warning] Failed to get event metadata from HTML: {e}")
        return {
            "E": "Colorado Ballet Event",
            "DT": "Unknown",
            "V": "Ellie Caulkins Opera House",
        }



def safe_get(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"Retry {attempt+1}/{max_retries} failed: {e}")
            time.sleep(2)
    raise Exception(f"Failed to fetch {url}")


def build_colorado_ballet_json(performance_id):
    init_url = f"{BASE_URL}/GetInitData?performanceId={performance_id}"
    init_data = safe_get(init_url)

    zone_price_map = {
        item["zone_no"]: {
            "price": item["price"],
            "description": item["description"],
            "category": item["price_type_desc"],
        }
        for item in init_data.get("Pricing", [])
    }

    screen_zone_map = defaultdict(list)
    for item in init_data.get("ScreenZoneList", []):
        screen_zone_map[item["screen_no"]].append(item["zone_no"])

    screen_id_to_label = {
        s["ScreenId"]: s["ScreenDescription"] for s in init_data.get("Screens", [])
    }

    facility_id = init_data.get("FacilityId")

    all_seats = []
    for screen_id in screen_zone_map:
        seat_list_url = f"{BASE_URL}/GetSeatList?performanceId={performance_id}&facilityId={facility_id}&screenId={screen_id}"
        seat_data = safe_get(seat_list_url)
        for seat in seat_data.get("seats", []):
            zone_no = seat.get("zone_no")
            if zone_no not in zone_price_map:
                continue
            zone_info = zone_price_map[zone_no]
            all_seats.append(
                {
                    "Level": screen_id_to_label.get(screen_id, f"Screen {screen_id}"),
                    "Row": seat.get("seat_row", "").strip(),
                    "Seat": seat.get("seat_num", "").strip(),
                    "Price": f"${zone_info['price']:.2f}",
                    "Category of the seat": zone_info["description"],
                }
            )

    e = get_event_details_from_web()
    event = {
        "E": e.get("E", "Colorado Ballet Event"),
        "DT": e.get("DT", "Unknown"),
        "V": e.get("V", "Ellie Caulkins Opera House"),
        "seats": all_seats,
    }

    return event


if __name__ == "__main__":
    output = build_colorado_ballet_json(PERFORMANCE_ID)
    with open("colorado_ballet.json", "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print("Saved to colorado_ballet.json")
