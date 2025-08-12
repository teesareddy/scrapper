import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Dict, Any, Tuple, Optional
from urllib.parse import urlparse

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from .types import (
    PerformanceDetailResponse, SeatBriefResponse, SeatStatusResponse
)
# Removed import for deleted proxy_manager utils file
# from ...utils.proxy_manager import ProxyManagerFactory, ProxyConfig
from ...proxy.service import get_proxy_service
from ...proxy.base import ProxyType

logger = logging.getLogger(__name__)


class NetworkInterceptor:

    def __init__(self):
        self.responses = {}
        self.matched_endpoints = set()

    async def setup(self, page, endpoints):
        """Set up the network interceptor for specific endpoints"""

        async def handle_response(response):
            response_url = response.url
            for endpoint in endpoints:
                if endpoint in response_url:
                    try:
                        self.matched_endpoints.add(endpoint)
                        body = await response.json()
                        self.responses[endpoint] = {
                            "url": response_url,
                            "status": response.status,
                            "body": body,
                            "headers": dict(response.headers)
                        }
                        logger.info(f"Captured response for endpoint: {endpoint}")
                    except Exception as e:
                        logger.warning(f"Error processing response for {endpoint}: {str(e)}")
                        try:
                            text = await response.text()
                            self.responses[endpoint] = {
                                "url": response_url,
                                "status": response.status,
                                "text": text,
                                "headers": dict(response.headers)
                            }
                        except Exception as text_e:
                            self.responses[endpoint] = {
                                "url": response_url,
                                "status": response.status,
                                "headers": dict(response.headers)
                            }

        page.on("response", handle_response)


class DataExtractor:
    """Extracts structured data from API responses using working scraper logic"""

    def __init__(self, responses):
        self.responses = responses

    @staticmethod
    def clean_text(text):
        """Clean and normalize text"""
        if text is None:
            return None
        text = text.strip()
        text = re.sub(r'\s+', ' ', text)
        return text if text else None

    def extract_venue_info(self):
        """Extract venue information from performance data"""
        venue_info = {
            "raw_venue_id": None,
            "raw_venue_url": None,
            "raw_venue_name": None,
            "raw_venue_address": None,
            "raw_venue_timezone": None,
            "raw_venue_features": None
        }

        try:
            performance_data = self.responses.get("GetPerformanceDetailWithDiscountingEx", {}).get("body", {})

            perf = None
            if performance_data and "result" in performance_data and "GetPerformanceDetailWithDiscountingExResult" in \
                    performance_data["result"] and "Performance" in performance_data["result"][
                "GetPerformanceDetailWithDiscountingExResult"]:
                perf = performance_data["result"]["GetPerformanceDetailWithDiscountingExResult"]["Performance"]

            if perf:
                venue_info["raw_venue_id"] = str(perf.get("facility_no", ""))
                venue_info["raw_venue_name"] = self.clean_text(perf.get("facility_desc", "David H. Koch Theater"))
                venue_info["raw_venue_address"] = "20 Lincoln Center Plaza, New York, NY 10023"
                venue_info["raw_venue_timezone"] = "America/New_York"
                
                # Try to infer timezone from performance datetime
                if perf.get("perf_dt"):
                    perf_dt = perf["perf_dt"]
                    if isinstance(perf_dt, str) and len(perf_dt) > 6:
                        tz_offset = perf_dt[-6:]
                        if (tz_offset.startswith("+") or tz_offset.startswith("-")) and ":" in tz_offset:
                            tz_map = {
                                "-04:00": "America/New_York",
                                "-05:00": "America/New_York"
                            }
                            venue_info["raw_venue_timezone"] = tz_map.get(tz_offset, "America/New_York")

        except Exception as e:
            logger.warning(f"Error extracting venue info: {str(e)}")

        return venue_info

    def extract_event_info(self):
        """Extract event information"""
        event_info = {
            "raw_event_id": None,
            "raw_event_url": None,
            "raw_event_name": None,
            "raw_performance_datetime_text": None,
            "raw_event_features": None
        }

        try:
            performance_data = self.responses.get("GetPerformanceDetailWithDiscountingEx", {}).get("body", {})
            if performance_data and "result" in performance_data and "GetPerformanceDetailWithDiscountingExResult" in \
                    performance_data["result"] and "Performance" in performance_data["result"][
                "GetPerformanceDetailWithDiscountingExResult"]:
                perf = performance_data["result"]["GetPerformanceDetailWithDiscountingExResult"]["Performance"]
                event_info["raw_event_id"] = perf.get("inv_no")
                event_info["raw_event_name"] = self.clean_text(perf.get("description"))
                event_info["raw_performance_datetime_text"] = perf.get("perf_dt")

        except Exception as e:
            logger.warning(f"Error extracting event info: {str(e)}")

        return event_info

    def extract_pricing_info(self):
        """Extract pricing information"""
        pricing_info = {
            "raw_currency": "$",
            "raw_price_range_text": None
        }

        try:
            performance_data = self.responses.get("GetPerformanceDetailWithDiscountingEx", {}).get("body", {})
            
            # Get prices from AllPrice array
            prices = []
            if performance_data and "result" in performance_data:
                result = performance_data["result"].get("GetPerformanceDetailWithDiscountingExResult", {})
                all_prices = result.get("AllPrice", [])
                
                for price_zone in all_prices:
                    if price_zone.get("price"):
                        try:
                            prices.append(float(price_zone["price"]))
                        except (ValueError, TypeError):
                            pass

            if prices:
                min_price = min(prices)
                max_price = max(prices)
                
                if min_price == max_price:
                    pricing_info["raw_price_range_text"] = f"${min_price:.2f}"
                else:
                    pricing_info["raw_price_range_text"] = f"${min_price:.2f}-${max_price:.2f}"
                    
        except Exception as e:
            logger.warning(f"Error extracting pricing info: {str(e)}")

        return pricing_info

    def extract_seat_map_info(self):
        """Extract seat map information"""
        seat_map_info = {
            "seat_map_image_url": None,
            "map_dimensions": None
        }

        try:
            seats_data = self.responses.get("GetSeatsBriefWithMOS", {}).get("body", {})
            
            # Try to find map dimensions
            dimension_fields = ["MapParams", "MapDimensions", "Map"]
            for field in dimension_fields:
                if field in seats_data:
                    map_params = seats_data[field]
                    width = map_params.get("Width") or map_params.get("width")
                    height = map_params.get("Height") or map_params.get("height")

                    if width and height:
                        seat_map_info["map_dimensions"] = {
                            "width": width,
                            "height": height
                        }
                        break

        except Exception as e:
            logger.warning(f"Error extracting seat map info: {str(e)}")

        return seat_map_info

    def extract_seating_structure(self):
        """Extract seating structure matching working scraper logic"""
        levels = []
        seat_data = []

        try:
            seats_response = self.responses.get("GetSeatsBriefWithMOS", {}).get("body", {})
            seats_data = seats_response

            # For David H Koch Theater format
            if not seats_data and "result" in seats_response:
                if "GetSeatsBriefExResults" in seats_response["result"]:
                    seats_data = seats_response["result"]["GetSeatsBriefExResults"]

            if not seats_data:
                logger.warning("No seat data found in the responses")
                return levels, seat_data

            # Process section data if available
            sections_map = {}
            if seats_data.get("Section"):
                for section_info in seats_data["Section"]:
                    section_id = section_info.get("section")
                    if section_id:
                        sections_map[section_id] = {
                            "id": f"section_{section_id}",
                            "name": self.clean_text(section_info.get("section_desc", "Unknown Section"))
                        }

            # Process seat types if available
            seat_types = {}
            if seats_data.get("SeatType"):
                for seat_type in seats_data["SeatType"]:
                    type_id = seat_type.get("seat_type")
                    if type_id:
                        seat_types[type_id] = {
                            "id": type_id,
                            "name": self.clean_text(seat_type.get("seat_type_desc", "Unknown Type"))
                        }

            # Create levels based on sections
            for section_id, section_info in sections_map.items():
                level_id = f"level_{section_id}"
                level_name = section_info.get("name", f"Level {section_id}")

                level_obj = {
                    "level_id": level_id,
                    "raw_level_name": level_name,
                    "raw_level_price_range_text": None,
                    "raw_level_availability_text": None,
                    "raw_level_features": None,
                    "map_bounds": None
                }
                levels.append(level_obj)

            # Create a level lookup
            level_lookup = {level["level_id"]: level for level in levels}
            section_price_ranges = {}

            # Parse seat data
            if seats_data.get("S"):
                for seat_item in seats_data["S"]:
                    if seat_item.get("D"):
                        seat_data_parts = seat_item["D"].split(",")
                        
                        if len(seat_data_parts) >= 18:
                            section_id = seat_data_parts[0]
                            row_name = seat_data_parts[1]
                            seat_num = seat_data_parts[2]
                            status = seat_data_parts[3]
                            seat_id = seat_data_parts[4]
                            zone_no = seat_data_parts[5]
                            
                            x_coord = seat_data_parts[8] if len(seat_data_parts) > 8 else None
                            y_coord = seat_data_parts[10] if len(seat_data_parts) > 10 else None

                            level_id = f"level_{section_id}"
                            if level_id not in level_lookup:
                                level_id = "level_default"

                            seat_label = f"{row_name}{seat_num}"

                            # Map status codes
                            status_map = {
                                "0": "Unavailable",
                                "4": "On Hold", 
                                "5": "Restricted",
                                "8": "Available",
                                "13": "Purchased"
                            }
                            status_name = status_map.get(status, "Unknown")

                            seat_obj = {
                                "seat_id": f"seat_{seat_id}",
                                "level_id": level_id,
                                "section_id": f"section_{section_id}",
                                "row_id": f"row_{section_id}_{row_name}",
                                "raw_seat_label": seat_label,
                                "raw_seat_status": status_name,
                                "raw_price_text": None,
                                "map_position": {
                                    "x": int(x_coord) if x_coord and x_coord.isdigit() else None,
                                    "y": int(y_coord) if y_coord and y_coord.isdigit() else None
                                } if x_coord and y_coord else None
                            }
                            seat_data.append(seat_obj)

        except Exception as e:
            logger.error(f"Error extracting seating structure: {str(e)}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")

        return levels, seat_data


class DavidHKochTheaterExtractor:
    """Extracts data from David H Koch Theater website using working scraper logic"""

    def __init__(self):
        self.source_website = urlparse("https://tickets.davidhkochtheater.com").netloc
        self.interceptor = NetworkInterceptor()

    async def extract(self, url: str, proxy_config=None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Extract David H Koch Theater specific data from URL"""
        try:
            scrape_result = await self._run_scraper(url, proxy_config)
            
            if not scrape_result["scrape_success"]:
                raise Exception(scrape_result.get("error_message", "Scraping failed"))

            # Create data extractor
            extractor = DataExtractor(self.interceptor.responses)

            # Extract all data components
            venue_info = extractor.extract_venue_info()
            event_info = extractor.extract_event_info()
            pricing_info = extractor.extract_pricing_info()
            seat_map_info = extractor.extract_seat_map_info()
            levels, seat_data = extractor.extract_seating_structure()

            # Build comprehensive result matching working scraper format
            performance_data = {
                "request_url": url,
                "performance_url": url,
                "source_website": self.source_website,
                "scraped_at_utc": datetime.utcnow().isoformat(),
                "scrape_success": scrape_result["scrape_success"],
                "error_message": scrape_result["error_message"],
                **venue_info,
                **event_info,
                **pricing_info,
                "http_status_code": 200,
                "response_headers": {},
                "scrape_duration_ms": scrape_result["scrape_duration_ms"],
                "scraper_version": "1.0.0",
                **seat_map_info,
                "levels": levels,
                "seat_data": seat_data
            }

            # Return the raw network responses as seats_data
            seats_data = self.interceptor.responses

            return performance_data, seats_data

        except Exception as e:
            raise Exception(f"David H Koch Theater extraction failed: {e}")

    def _should_use_proxy(self):
        """Check if proxy should be used via proxy service"""
        import os
        return os.getenv('USE_PROXY', 'false').lower() == 'true'

    def _get_proxy_config(self):
        """Get proxy configuration using new proxy service"""
        if not self._should_use_proxy():
            return None
        
        try:
            credentials = get_proxy_service().get_proxy_for_scraper(
                scraper_name="david_h_koch_theater_scraper",
                proxy_type=ProxyType.DATACENTER
            )
            
            if credentials:
                return {
                    "server": f"http://{credentials.host}:{credentials.port}",
                    "username": credentials.username,
                    "password": credentials.password
                }
            
            return None
        except Exception as e:
            # Check if this is a proxy requirement failure - if so, re-raise
            if "fail_without_proxy=True" in str(e):
                logger.error(f"Proxy requirement failure: {e}")
                raise e
            else:
                # For other errors, log and return None
                logger.error(f"Failed to get proxy configuration: {e}")
                return None

    async def _run_scraper(self, url: str, proxy_config=None):
        """Run the scraper using Playwright with working scraper logic"""
        start_time = datetime.now()
        scrape_success = False
        error_message = None

        try:
            async with async_playwright() as p:
                # Setup browser options
                browser_options = {"headless": True}
                
                # Use proxy config from ScraperDefinition if provided
                if proxy_config:
                    browser_options["proxy"] = {
                        "server": f"{proxy_config.get('protocol', 'http')}://{proxy_config['host']}:{proxy_config['port']}",
                        "username": proxy_config.get('username'),
                        "password": proxy_config.get('password')
                    }
                    logger.info(f"Using proxy from ScraperDefinition: {proxy_config['host']}:{proxy_config['port']}")
                elif self._should_use_proxy():
                    fallback_proxy_config = self._get_proxy_config()
                    if fallback_proxy_config:
                        browser_options["proxy"] = fallback_proxy_config
                        logger.info(f"Using fallback proxy: {fallback_proxy_config['server']}")
                else:
                    logger.info("🚫 Proxy disabled - using direct connection")
                
                browser = await p.chromium.launch(**browser_options)
                context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
                )
                page = await context.new_page()

                # Set up network interception
                endpoints = [
                    "GetPerformanceDetailWithDiscountingEx",
                    "GetSeatsBriefWithMOS", 
                    "GetSeatStatus"
                ]
                await self.interceptor.setup(page, endpoints)

                try:
                    logger.info(f"Navigating to URL: {url}")
                    await page.goto(url, timeout=60000)
                    await page.wait_for_load_state("networkidle", timeout=60000)
                    
                    # Take initial screenshot after page load
                    if self._should_use_proxy():
                        try:
                            import time
                            import os
                            screenshot_dir = "/app/screenshots"
                            os.makedirs(screenshot_dir, exist_ok=True)
                            screenshot_path = f"{screenshot_dir}/debug_david_koch_initial_{int(time.time())}.png"
                            await page.screenshot(path=screenshot_path, full_page=True)
                            logger.info(f"📸 David Koch initial screenshot saved: {screenshot_path}")
                        except Exception as e:
                            logger.warning(f"Failed to take initial screenshot: {e}")

                    # Look for seat map buttons
                    seat_map_buttons = await page.query_selector_all(
                        'button:has-text("Seat Map"), button:has-text("Select Seats"), a:has-text("Seat Map"), a:has-text("Select Seats")')
                    if seat_map_buttons and len(seat_map_buttons) > 0:
                        logger.info("Found seat map button, clicking...")
                        await seat_map_buttons[0].click()
                        await page.wait_for_load_state("networkidle", timeout=30000)

                    await asyncio.sleep(5)

                    # Take debug screenshot if proxy is enabled
                    if self._should_use_proxy():
                        try:
                            import time
                            import os
                            screenshot_dir = "/app/screenshots"
                            os.makedirs(screenshot_dir, exist_ok=True)
                            screenshot_path = f"{screenshot_dir}/debug_david_koch_{int(time.time())}.png"
                            await page.screenshot(path=screenshot_path, full_page=True)
                            logger.info(f"📸 David Koch debug screenshot saved: {screenshot_path}")
                        except Exception as e:
                            logger.warning(f"Failed to take screenshot: {e}")

                    if self.interceptor.matched_endpoints:
                        logger.info(f"Captured data for endpoints: {self.interceptor.matched_endpoints}")
                        scrape_success = True
                    else:
                        # Try additional interactions
                        date_buttons = await page.query_selector_all(
                            'button:has-text("Select"), button:has-text("Date"), button:has-text("Time")')
                        if date_buttons and len(date_buttons) > 0:
                            logger.info("Found date/time button, clicking...")
                            await date_buttons[0].click()
                            await page.wait_for_load_state("networkidle", timeout=30000)
                            await asyncio.sleep(2)

                        if self.interceptor.matched_endpoints:
                            scrape_success = True
                        else:
                            error_message = "Failed to capture required data from network requests"

                except PlaywrightTimeoutError:
                    error_message = "Timeout while loading the page"
                except Exception as nav_e:
                    error_message = f"Navigation error: {str(nav_e)}"

                await browser.close()
        except Exception as e:
            error_message = f"Scraper error: {str(e)}"
            logger.error(error_message)

        end_time = datetime.now()
        scrape_duration_ms = (end_time - start_time).total_seconds() * 1000

        return {
            "scrape_success": scrape_success,
            "error_message": error_message,
            "scrape_duration_ms": scrape_duration_ms
        }