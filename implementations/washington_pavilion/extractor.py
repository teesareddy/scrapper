import asyncio
from typing import Dict, Any, Tuple, Optional
from ...utils.web_scraper_utils import WebScraperUtils
from ...exceptions.scraping_exceptions import NetworkException, ParseException, TimeoutException
from .types import EventPricingInfo, SeatFeaturesInfo


class WashingtonPavilionExtractor:

    async def extract(self, url: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        try:
            event_info, network_data = await WebScraperUtils.scrape_with_playwright(
                url=url,
                capture_func=self._capture_network_data,
                extract_func=self._extract_event_info,
                scraper_name="washington_pavilion_scraper_v5"
            )

            # Validate network data before processing
            if not network_data:
                raise ParseException("No network data captured during extraction")
            
            seatmap_data = network_data.get("seatmap_data")
            seats_data = network_data.get("seats_data")
            
            if not seatmap_data:
                raise ParseException("No seatmap data captured - required for pricing information")
            
            if not seats_data:
                raise ParseException("No seats data captured - required for seat information")
            
            pricing_info = self._parse_pricing_data(seatmap_data)
            seats_info = self._parse_seats_data(seats_data)

            if not pricing_info:
                raise ParseException("Failed to parse pricing data from seatmap response")
            
            if not seats_info:
                raise ParseException("Failed to parse seats data from seats response")

            if pricing_info and event_info:
                pricing_info['venue_info'] = event_info.get('venue_info')
                pricing_info['title'] = event_info.get('title')
                pricing_info['date'] = event_info.get('date')
                pricing_info['time'] = event_info.get('time')
                pricing_info['url'] = url

            return pricing_info, seats_info

        except TimeoutException as e:
            raise TimeoutException(f"Washington Pavilion extraction timed out: {e}")
        except NetworkException as e:
            raise NetworkException(f"Washington Pavilion network error: {e}")
        except ParseException as e:
            raise ParseException(f"Washington Pavilion parsing error: {e}")
        except Exception as e:
            raise NetworkException(f"Washington Pavilion extraction failed: {e}")

    async def _capture_network_data(self, page, url: str) -> Dict[str, Any]:
        url_patterns = {
            "seatmap/seats/free/ol": "seats_data",
            "seatmap/availability": "seatmap_data"
        }

        network_data_future = asyncio.create_task(
            WebScraperUtils.monitor_network(page, url_patterns, timeout=45.0)
        )

        await page.goto(url, wait_until="networkidle", timeout=45000)
        await page.evaluate("window.scrollBy(0, 300)")

        selectors_to_try = [
            ".seat-selection-container",
            ".seat",
            ".area",
            ".seatmap-container",
            ".seat-map",
            "[class*='seat']",
            "[class*='map']"
        ]

        selector_found = False
        for selector in selectors_to_try:
            try:
                if await page.is_visible(selector):
                    await page.click(selector)
                    await asyncio.sleep(2)
                    selector_found = True
                    break
            except (TimeoutException, NetworkException) as e:
                # Log the specific selector that failed
                continue
        
        if not selector_found:
            raise ParseException(f"Could not find any interactive seat selection elements. Tried selectors: {selectors_to_try}")

        await asyncio.sleep(5)
        network_data = await network_data_future

        # Validate network data completeness
        if not network_data:
            raise NetworkException("No network data captured during monitoring")
        
        # Check if we have both required data types
        missing_data = []
        if not network_data.get('seatmap_data'):
            missing_data.append('seatmap_data')
        if not network_data.get('seats_data'):
            missing_data.append('seats_data')
        
        if missing_data:
            # Try waiting a bit more for incomplete data
            await asyncio.sleep(10)
            
            # Re-check after additional wait
            updated_missing_data = []
            if not network_data.get('seatmap_data'):
                updated_missing_data.append('seatmap_data')
            if not network_data.get('seats_data'):
                updated_missing_data.append('seats_data')
            
            if updated_missing_data:
                raise NetworkException(f"Missing required network data after extended wait: {', '.join(updated_missing_data)}. Available data keys: {list(network_data.keys())}")

        return network_data

    async def _extract_event_info(self, page) -> Dict[str, Any]:
        try:
            await WebScraperUtils.wait_for_selector(page, ".content_product_info", timeout=20000)
        except TimeoutException:
            raise ParseException("Could not find event information container (.content_product_info) on the page")
        
        venue_info = await self._extract_venue_info(page)
        
        # Extract basic event information with fallbacks
        title = await WebScraperUtils.safe_text_content(page, ".product_title_container p.title")
        if not title:
            title = await WebScraperUtils.safe_text_content(page, "h1") or "Unknown Event"
        
        date = await WebScraperUtils.safe_text_content(page, "p.date .day")
        time = await WebScraperUtils.safe_text_content(page, "p.date .time")

        return {
            "title": title,
            "date": date,
            "time": time,
            "venue_info": venue_info
        }

    async def _extract_venue_info(self, page) -> Dict[str, Any]:
        venue_data = {
            "name": None,
            "location": None,
            "space": None,
            "site": None,
            "address": None,
            "city": None,
            "state": None,
            "country": "US"
        }

        location_selectors = ["p.location_container .location", ".location_topic .location"]

        for selector in location_selectors:
            try:
                if await page.is_visible(selector):
                    space = await WebScraperUtils.safe_text_content(page, f"{selector} .space")
                    site = await WebScraperUtils.safe_text_content(page, f"{selector} .site")

                    if space:
                        venue_data["space"] = space.strip()
                    if site:
                        venue_data["site"] = site.strip()

                    if space and site:
                        venue_data["location"] = f"{space}, {site}"
                        venue_data["name"] = site.strip()

                    break
            except Exception:
                # Continue trying other selectors if this one fails
                continue

        venue_name_selectors = [
            ".venue-name",
            ".location .site",
            "[class*='venue']",
            ".product_location"
        ]

        for selector in venue_name_selectors:
            if not venue_data["name"]:
                try:
                    name = await WebScraperUtils.safe_text_content(page, selector)
                    if name and name.strip():
                        venue_data["name"] = name.strip()
                        break
                except Exception:
                    # Continue trying other selectors if this one fails
                    continue

        if venue_data["location"]:
            location_parts = venue_data["location"].split(",")
            if len(location_parts) >= 2:
                venue_data["city"] = location_parts[-1].strip()
                if len(location_parts) >= 3:
                    venue_data["city"] = location_parts[-2].strip()
                    venue_data["state"] = location_parts[-1].strip()

        if not venue_data["name"]:
            venue_data["name"] = "Washington Pavilion"
        if not venue_data["city"]:
            venue_data["city"] = "Sioux Falls"
        if not venue_data["state"]:
            venue_data["state"] = "SD"

        return venue_data

    def _parse_pricing_data(self, raw_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not raw_data:
            return None

        try:
            if not isinstance(raw_data, dict):
                return None
            
            if 'priceRangeCategories' in raw_data:
                # Validate that priceRangeCategories is a list
                categories = raw_data['priceRangeCategories']
                if not isinstance(categories, list):
                    return None
                return raw_data
            return None
        except (KeyError, TypeError):
            return None

    def _parse_seats_data(self, raw_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not raw_data:
            return None

        try:
            if not isinstance(raw_data, dict):
                return None
            
            if 'features' in raw_data:
                # Validate that features is a list
                features = raw_data['features']
                if not isinstance(features, list):
                    return None
                return raw_data
            return None
        except (KeyError, TypeError):
            return None