"""
TPAC processor for data transformation and seat pack generation
Based on the reference implementation in scraperref/tpac.py
Following VividSeats/Broadway SF processor patterns
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
from decimal import Decimal
import hashlib
import re
from collections import defaultdict
from dateutil import parser
from ...core.data_schemas import (
    ScrapedData, VenueData, EventData, PerformanceData,
    LevelData, ZoneData, SectionData, SeatData, SeatPackData,
    ScraperConfigData, ZoneFeaturesData
)
from ...core.seat_pack_generator import generate_seat_packs, detect_venue_seat_structure


def get_venue_seat_structure(source_venue_id: str, source_website: str) -> str:
    """Fetch seat_structure from DB (shared utility function)"""
    try:
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT seat_structure FROM scrapers_venue WHERE source_venue_id = %s AND source_website = %s LIMIT 1",
                [source_venue_id, source_website]
            )
            row = cursor.fetchone()
            if row and row[0]:
                return row[0]
    except Exception:
        pass
    return None


class TPACProcessor:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        
    def process(self, performance_data: Dict[str, Any], seat_data: Dict[str, Any],
                url: str, scrape_job_id: Optional[str] = None, enriched_data: Dict[str, Any] = None) -> ScrapedData:
        
        # Extract data from the structured inputs
        performance_details = performance_data.get("performance_details", {})
        screens = seat_data.get("screens", [])
        seat_lists = seat_data.get("seat_lists", {})
        facility_id = seat_data.get("facility_id")
        
        venue_info = self._process_venue_info(performance_details)
        event_info = self._process_event_info(performance_details, venue_info.source_venue_id, url)
        performance_info = self._process_performance_info(performance_details, performance_data.get("performance_id"), 
                                                        event_info.source_event_id, venue_info.source_venue_id, url)
        
        zones = self._process_zones(seat_lists)
        levels, sections = self._process_levels_and_sections(screens, facility_id)
        seats = self._process_seats(screens, seat_lists, zones, levels, sections, performance_data.get("performance_id"))
        
        # Fetch seat structure from DB if set, otherwise auto-detect
        seat_structure = get_venue_seat_structure(venue_info.source_venue_id, venue_info.source_website)
        if not seat_structure:
            if seats:
                seat_structure = detect_venue_seat_structure(seats)
            else:
                seat_structure = "consecutive"
        
        # Validate seat structure value
        valid_structures = ["consecutive", "odd_even"]
        if seat_structure not in valid_structures:
            seat_structure = "consecutive"
        
        venue_info.seat_structure = seat_structure
        
        # Update sections with appropriate numbering scheme
        for section in sections:
            if hasattr(section, 'numbering_scheme'):
                section.numbering_scheme = "odd-even" if seat_structure == "odd_even" else "consecutive"
        
        seat_packs = self._process_seat_packs(seats, sections, performance_info)

        return ScrapedData(
            source_website=self.config.get('source_website', "tpac"),
            scraped_at=datetime.utcnow(),
            url=url,
            venue_info=venue_info,
            event_info=event_info,
            performance_info=performance_info,
            levels=levels,
            zones=zones,
            sections=sections,
            seats=seats,
            seat_packs=seat_packs,
            scraper_config=ScraperConfigData(
                scraper_name=self.config.get('scraper_name', "tpac_scraper_v1")
            ),
            scraper_version=self.config.get('scraper_version', "v1")
        )

    def _process_venue_info(self, performance_details: Dict[str, Any]) -> VenueData:
        """Process venue information from TPAC performance details"""
        venue_name = performance_details.get('facility_desc', 'TPAC Venue')
        facility_id = performance_details.get('facility_no', 'unknown')
        
        # Create a consistent source_venue_id
        source_venue_id = f"tpac_{facility_id}" if facility_id != 'unknown' else hashlib.md5(venue_name.encode()).hexdigest()
        
        return VenueData(
            name=venue_name,
            source_venue_id=source_venue_id,
            source_website=self.config.get('source_website', "tpac"),
            city="Nashville",  # TPAC is in Nashville
            state="TN",
            country="US",
            venue_timezone="America/Chicago"  # As specified in reference
        )

    def _process_event_info(self, performance_details: Dict[str, Any], venue_source_id: str, url: str) -> EventData:
        """Process event information from TPAC performance details"""
        event_name = performance_details.get('description', 'TPAC Event')
        
        # Create a consistent source_event_id
        unique_string = f"{venue_source_id}_{event_name}"
        source_event_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        return EventData(
            name=event_name,
            source_event_id=source_event_id,
            source_website=self.config.get('source_website', "tpac"),
            url=url,
            currency='USD',
            title=event_name,
            description=event_name
        )

    def _process_performance_info(self, performance_details: Dict[str, Any], performance_id: str,
                                event_source_id: str, venue_source_id: str, url: str) -> PerformanceData:
        """Process performance information from TPAC performance details"""
        
        # Get performance datetime
        performance_datetime = datetime.utcnow()
        perf_dt = performance_details.get('perf_dt', '')
        if perf_dt:
            try:
                performance_datetime = parser.parse(perf_dt)
            except (ValueError, TypeError):
                pass
        
        # Use performance_id if available, otherwise generate from event info
        source_performance_id = performance_id if performance_id else f"tpac_{event_source_id}"
        
        return PerformanceData(
            source_performance_id=source_performance_id,
            source_website=self.config.get('source_website', "tpac"),
            performance_datetime_utc=performance_datetime,
            event_source_id=event_source_id,
            venue_source_id=venue_source_id,
            seat_map_url=url,
            performance_url=url
        )

    def _process_zones(self, seat_lists: Dict[str, Dict[str, Any]]) -> List[ZoneData]:
        """Process zone information from TPAC seat lists"""
        zones = {}  # Use dict to avoid duplicates
        
        for screen_id, seat_list_data in seat_lists.items():
            available_prices = seat_list_data.get('AvailablePrices', [])
            
            for price_info in available_prices:
                zone_no = price_info.get('ZoneNo')
                if not zone_no:
                    continue
                
                zone_id = str(zone_no)
                if zone_id in zones:
                    continue  # Skip duplicates
                
                zone_name = price_info.get('ZoneDesc', f'Zone {zone_no}')
                price = price_info.get('Price', 0.0)
                
                try:
                    price_decimal = Decimal(str(price))
                except (ValueError, TypeError):
                    price_decimal = Decimal('0.0')
                
                zone_features = ZoneFeaturesData(
                    view_description=zone_name
                )
                
                zone = ZoneData(
                    zone_id=zone_id,
                    source_website=self.config.get('source_website', "tpac"),
                    name=zone_name,
                    features=zone_features,
                    raw_identifier=zone_id,
                    color_code="#CCCCCC",
                    display_order=0,
                    min_price=price_decimal,
                    max_price=price_decimal,
                    wheelchair_accessible=False,  # TPAC doesn't specify in reference
                    miscellaneous={
                        'default_price': seat_list_data.get('DefaultPrice', ''),
                        'screen_id': screen_id
                    }
                )
                zones[zone_id] = zone
        
        return list(zones.values())

    def _process_levels_and_sections(self, screens: List[Dict[str, Any]], facility_id: str) -> tuple[List[LevelData], List[SectionData]]:
        """Process levels and sections from TPAC screens (screens become sections)"""
        levels = []
        sections = []
        
        # Create a single level for TPAC venue
        level_id = f"tpac_level_{facility_id}"
        level = LevelData(
            level_id=level_id,
            source_website=self.config.get('source_website', "tpac"),
            name="Main Level",
            raw_name="main",
            display_order=0
        )
        levels.append(level)
        
        # Convert screens to sections
        for screen in screens:
            screen_id = screen.get('screen_no', '')
            screen_label = screen.get('screen_desc', f'Screen {screen_id}')
            
            section = SectionData(
                section_id=f"tpac_section_{screen_id}",
                level_id=level_id,
                source_website=self.config.get('source_website', "tpac"),
                name=screen_label,
                raw_name=screen_label,
                display_order=0
            )
            sections.append(section)
        
        return levels, sections

    def _process_seats(self, screens: List[Dict[str, Any]], seat_lists: Dict[str, Dict[str, Any]], 
                      zones: List[ZoneData], levels: List[LevelData], sections: List[SectionData],
                      performance_id: str) -> List[SeatData]:
        """Process seats from TPAC seat lists, following reference grouping logic"""
        import logging
        logger = logging.getLogger(__name__)
        
        seats = []
        zone_lookup = {zone.zone_id: zone for zone in zones}
        section_lookup = {section.section_id.replace('tpac_section_', ''): section for section in sections}
        level_lookup = {level.level_id: level for level in levels}
        
        logger.info(f"🎫 Starting TPAC seat processing with {len(screens)} screens")
        
        for screen in screens:
            screen_id = screen.get('screen_no', '')
            screen_label = screen.get('screen_desc', '')
            
            if screen_id not in seat_lists:
                logger.warning(f"No seat list found for screen {screen_id}")
                continue
            
            seat_data = seat_lists[screen_id]
            zone_prices = {
                item["ZoneNo"]: item["Price"]
                for item in seat_data.get("AvailablePrices", [])
            }
            
            # Group seats by row like in reference implementation
            row_grouped = defaultdict(list)
            
            for seat in seat_data.get("seats", []):
                if seat.get("seat_status_desc") != "Available":
                    continue
                
                zone_no = seat.get("zone_no")
                row = seat.get("seat_row", "").strip()
                seat_num = seat.get("seat_num", "").strip()
                price = zone_prices.get(zone_no, 0.0)
                
                try:
                    price_decimal = Decimal(str(price))
                except (ValueError, TypeError):
                    price_decimal = Decimal('0.0')
                
                # Find corresponding section
                section = section_lookup.get(screen_id)
                if not section:
                    logger.warning(f"No section found for screen {screen_id}")
                    continue
                
                row_grouped[(screen_id, row)].append({
                    "seat_num": seat_num,
                    "price": price_decimal,
                    "zone_id": str(zone_no),
                    "section_id": section.section_id,
                    "row": row,
                    "screen_label": screen_label,
                    "accessibility": seat.get("accessible_ind", False),
                })
            
            # Process grouped seats like in reference (consecutive grouping)
            for (section_id, row), seat_list in row_grouped.items():
                sorted_seats = sorted(
                    seat_list, key=lambda x: int("".join(filter(str.isdigit, x["seat_num"])))
                )
                
                i = 0
                while i < len(sorted_seats):
                    group = [sorted_seats[i]]
                    j = i + 1
                    while j < len(sorted_seats):
                        prev = int("".join(filter(str.isdigit, sorted_seats[j - 1]["seat_num"])))
                        curr = int("".join(filter(str.isdigit, sorted_seats[j]["seat_num"])))
                        if curr == prev + 1:
                            group.append(sorted_seats[j])
                            j += 1
                        else:
                            break
                    
                    # Create SeatData objects for each seat in the group
                    for seat in group:
                        seat_id = f"tpac_{performance_id}_{section_id}_{seat['row']}_{seat['seat_num']}"
                        
                        seat_obj = SeatData(
                            seat_id=seat_id,
                            section_id=seat['section_id'],
                            zone_id=seat['zone_id'],
                            source_website=self.config.get('source_website', "tpac"),
                            row_label=seat['row'],
                            seat_number=seat['seat_num'],
                            row=seat['row'],
                            number=seat['seat_num'],
                            x_coord=None,
                            y_coord=None,
                            status="available",
                            price=seat['price'],
                            level_id=levels[0].level_id if levels else None,
                            pack_size=len(group),  # Store original pack size from reference logic
                        )
                        seats.append(seat_obj)
                    
                    i = j
        
        logger.info(f"🎯 TPAC seat processing completed: {len(seats)} seats created")
        return seats

    def _process_seat_packs(self, seats: List[SeatData], sections: List[SectionData], 
                          performance_info: PerformanceData) -> List[SeatPackData]:
        """Process seat packs using the generic seat pack generator"""
        venue_prefix_map = {
            self.config.get('source_website', "tpac"): self.config.get('venue_prefix', "tp")
        }
        
        available_seats = [seat for seat in seats if seat.status == "available"]
        
        # Fetch venue object from database for markup pricing
        venue = self._get_venue_from_database(performance_info)
        
        return generate_seat_packs(
            all_seats=available_seats,
            all_sections=sections,
            performance=performance_info,
            venue_prefix_map=venue_prefix_map,
            venue=venue,
            min_pack_size=self.config.get('min_pack_size', 2),
            packing_strategy=self.config.get('packing_strategy', "maximal")
        )

    def _get_venue_from_database(self, performance_info: PerformanceData):
        """Fetch venue object from database for markup pricing"""
        try:
            from ...models import Venue
            
            venue = Venue.objects.filter(
                source_venue_id=performance_info.venue_source_id,
                source_website=self.config.get('source_website', "tpac")
            ).first()
            
            return venue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not fetch venue from database: {e}")
            return None