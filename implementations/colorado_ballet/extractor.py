import asyncio
import aiohttp
import time
from typing import Dict, Any, Tuple, Optional, List
from collections import defaultdict
from bs4 import BeautifulSoup
from ...exceptions.scraping_exceptions import NetworkException, ParseException, TimeoutException
from .types import ColoradoBalletEventDetails, ColoradoBalletInitData, ColoradoBalletSeatData


class ColoradoBalletExtractor:
    
    def __init__(self):
        self.headers = {
            "accept": "application/json, text/javascript, text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 OPR/120.0.0.0",
            "x-requested-with": "XMLHttpRequest",
        }
        self.base_url = "https://tickets.coloradoballet.org/api/syos"
        self.max_retries = 3
        self.timeout = 30

    async def extract(self, url: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        try:
            performance_id = self._get_performance_id(url)
            if not performance_id:
                raise ParseException("Could not extract performance ID from URL")

            event_details = await self._get_event_details_from_web(url)
            init_data = await self._get_init_data(performance_id)
            all_seats = await self._get_all_seats(performance_id, init_data)

            # Combine event details with seat data
            combined_data = {
                "event_details": event_details,
                "init_data": init_data,
                "seats": all_seats
            }

            return combined_data, combined_data

        except TimeoutException as e:
            raise TimeoutException(f"Colorado Ballet extraction timed out: {e}")
        except NetworkException as e:
            raise NetworkException(f"Colorado Ballet network error: {e}")
        except ParseException as e:
            raise ParseException(f"Colorado Ballet parsing error: {e}")
        except Exception as e:
            raise NetworkException(f"Colorado Ballet extraction failed: {e}")

    def _get_performance_id(self, url: str) -> Optional[str]:
        try:
            return url.split("/")[-1]
        except Exception:
            return None

    async def _safe_get(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        for attempt in range(self.max_retries):
            try:
                async with session.get(url, headers=self.headers, timeout=self.timeout) as response:
                    response.raise_for_status()
                    return await response.json()
            except Exception as e:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2)
                    continue
                raise NetworkException(f"Failed to fetch {url} after {self.max_retries} attempts: {e}")

    async def _get_event_details_from_web(self, url: str) -> Dict[str, Any]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self.headers, timeout=self.timeout) as response:
                    response.raise_for_status()
                    html_content = await response.text()

            soup = BeautifulSoup(html_content, "html.parser")

            title = soup.select_one(".tn-event-detail__title")
            date = soup.select_one(".tn-event-detail__display-time")
            venue = soup.select_one(".tn-event-detail__location")

            return {
                "title": title.text.strip() if title else "Colorado Ballet Event",
                "date": date.text.strip() if date else "Unknown",
                "venue": venue.text.strip() if venue else "Ellie Caulkins Opera House",
            }
        except Exception as e:
            return {
                "title": "Colorado Ballet Event",
                "date": "Unknown",
                "venue": "Ellie Caulkins Opera House",
            }

    async def _get_init_data(self, performance_id: str) -> Dict[str, Any]:
        init_url = f"{self.base_url}/GetInitData?performanceId={performance_id}"
        
        async with aiohttp.ClientSession() as session:
            return await self._safe_get(session, init_url)

    async def _get_all_seats(self, performance_id: str, init_data: Dict[str, Any]) -> List[Dict[str, Any]]:
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

        async with aiohttp.ClientSession() as session:
            tasks = []
            for screen_id in screen_zone_map:
                seat_list_url = f"{self.base_url}/GetSeatList?performanceId={performance_id}&facilityId={facility_id}&screenId={screen_id}"
                tasks.append(self._get_screen_seats(session, seat_list_url, screen_id, screen_id_to_label, zone_price_map))

            seat_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in seat_results:
                if isinstance(result, Exception):
                    continue
                if result:
                    all_seats.extend(result)

        return all_seats

    async def _get_screen_seats(self, session: aiohttp.ClientSession, seat_list_url: str, 
                               screen_id: str, screen_id_to_label: Dict[str, str], 
                               zone_price_map: Dict[str, Dict]) -> List[Dict[str, Any]]:
        try:
            seat_data = await self._safe_get(session, seat_list_url)
            seats = []
            
            for seat in seat_data.get("seats", []):
                zone_no = seat.get("zone_no")
                if zone_no not in zone_price_map:
                    continue
                    
                zone_info = zone_price_map[zone_no]
                seats.append({
                    "Level": screen_id_to_label.get(screen_id, f"Screen {screen_id}"),
                    "Row": seat.get("seat_row", "").strip(),
                    "Seat": seat.get("seat_num", "").strip(),
                    "Price": f"${zone_info['price']:.2f}",
                    "Category": zone_info["description"],
                    "zone_no": zone_no,
                    "screen_id": screen_id
                })
            
            return seats
        except Exception as e:
            return []