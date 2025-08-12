from typing import Dict, List, Any
from datetime import datetime
from decimal import Decimal, InvalidOperation
from ...core.data_schemas import (
    ScrapedData, VenueData, EventData, PerformanceData,
    LevelData, ZoneData, SectionData, SeatData, SeatPackData,
    ScraperConfigData, ZoneFeaturesData
)
from ...core.seat_pack_generator import generate_seat_packs, detect_venue_seat_structure
from ...models import Venue  # Add this import for DB access

# Utility function to fetch seat_structure from DB (shared)
def get_venue_seat_structure(source_venue_id: str, source_website: str) -> str:
    try:
        venue = Venue.objects.filter(source_venue_id=source_venue_id, source_website=source_website).first()
        if venue and venue.seat_structure:
            return venue.seat_structure
    except Exception:
        pass
    return None

class DavidHKochTheaterProcessor:

    def __init__(self):
        pass

    def process(self, performance_data: Dict[str, Any], seats_data: Dict[str, Any],
                url: str, scrape_job_id: str = None) -> ScrapedData:
        
        # Extract data from the complex nested structure
        perf_response = seats_data.get('GetPerformanceDetailWithDiscountingEx', {}).get('body', {})
        seats_response = seats_data.get('GetSeatsBriefWithMOS', {}).get('body', {})
        
        perf_result = perf_response.get('result', {}).get('GetPerformanceDetailWithDiscountingExResult', {})
        seats_result = seats_response.get('result', {}).get('GetSeatsBriefExResults', {}) if seats_response else {}
        
        performance_info = perf_result.get('Performance', {})
        
        venue_info = self._process_venue_info(performance_data, performance_info)
        event_info = self._process_event_info(performance_data, performance_info, venue_info.source_venue_id, url)
        performance_info_data = self._process_performance_info(performance_data, performance_info, event_info.source_event_id, venue_info.source_venue_id, url)
        
        zones = self._process_zones(perf_result.get('AllPrice', []))
        levels = self._process_levels(seats_result.get('Section', []))
        sections = self._process_sections(seats_result.get('Section', []), levels)
        seats = self._process_seats(seats_result.get('S', []), seats_result.get('Section', []), 
                                   seats_result.get('SeatType', []), zones, levels, sections)
        
        # Fetch seat structure from DB if set, otherwise auto-detect
        seat_structure = get_venue_seat_structure(venue_info.source_venue_id, venue_info.source_website)
        if not seat_structure:
            seat_structure = detect_venue_seat_structure(seats)
        venue_info.seat_structure = seat_structure
        
        # Update sections with appropriate numbering scheme
        for section in sections:
            section.numbering_scheme = "odd-even" if seat_structure == "odd_even" else "consecutive"
        
        available_seats = [seat for seat in seats if seat.available]
        
        # Fetch venue object from database for markup pricing
        venue = self._get_venue_from_database(performance_info_data)
        
        seat_packs = generate_seat_packs(
            all_seats=available_seats,
            all_sections=sections,
            performance=performance_info_data,
            venue_prefix_map={"david_h_koch_theater": "dhkt"},
            venue=venue,  # Pass venue object with markup data
            min_pack_size=2,
            packing_strategy="maximal"
        )
        
        return ScrapedData(
            source_website="david_h_koch_theater",
            scraped_at=datetime.utcnow(),
            url=url,
            venue_info=venue_info,
            event_info=event_info,
            performance_info=performance_info_data,
            levels=levels,
            zones=zones,
            sections=sections,
            seats=seats,
            seat_packs=seat_packs,
            scraper_config=ScraperConfigData(
                scraper_name="david_h_koch_theater_scraper_v5"
            ),
            scraper_version="v5"
        )

    def _process_venue_info(self, performance_data: Dict[str, Any], performance_info: Dict[str, Any]) -> VenueData:
        """Process venue information"""
        venue_name = performance_data.get('raw_venue_name') or performance_info.get('facility_desc', 'David H. Koch Theater')
        facility_no = performance_info.get('facility_no', 'dhkt_main')
        
        source_venue_id = facility_no
        
        return VenueData(
            name=venue_name,
            source_venue_id=source_venue_id,
            source_website="david_h_koch_theater",
            city="New York",
            state="NY",
            country="US",
            address="20 Lincoln Center Plaza, New York, NY 10023",
            postal_code="10023",
            venue_timezone="America/New_York"
        )

    def _process_event_info(self, performance_data: Dict[str, Any], performance_info: Dict[str, Any], 
                           venue_source_id: str, url: str) -> EventData:
        """Process event information"""
        event_name = performance_data.get('raw_event_name') or performance_info.get('description', 'David H Koch Theater Performance')
        inv_no = performance_info.get('inv_no', 'unknown')
        
        source_event_id = inv_no
        
        return EventData(
            name=event_name,
            source_event_id=source_event_id,
            source_website="david_h_koch_theater",
            url=url,
            currency="USD",
            event_type=performance_info.get('performance_type', 'theater')
        )

    def _process_performance_info(self, performance_data: Dict[str, Any], performance_info: Dict[str, Any],
                                 event_source_id: str, venue_source_id: str, url: str) -> PerformanceData:
        """Process performance information"""
        inv_no = performance_info.get('inv_no', 'unknown')
        
        # Parse performance datetime
        performance_datetime = datetime.utcnow()  # Default fallback
        perf_dt = performance_data.get('raw_performance_datetime_text') or performance_info.get('perf_dt')
        if perf_dt:
            try:
                from dateutil import parser
                performance_datetime = parser.parse(perf_dt)
            except Exception:
                pass
        
        return PerformanceData(
            source_performance_id=inv_no,
            source_website="david_h_koch_theater",
            performance_datetime_utc=performance_datetime,
            event_source_id=event_source_id,
            venue_source_id=venue_source_id,
            seat_map_url=url,
            performance_url=url
        )

    def _process_zones(self, all_price_data: List[Dict[str, Any]]) -> List[ZoneData]:
        """Process David H Koch Theater zones from AllPrice data"""
        zones = []
        
        for zone_data in all_price_data:
            zone_id = zone_data.get('price_type_no', 'unknown')
            zone_name = zone_data.get('price_type_desc', f'Zone {zone_id}')
            
            # Extract price information
            min_price = None
            max_price = None
            try:
                price_val = zone_data.get('amt', 0)
                if price_val:
                    price_decimal = Decimal(str(price_val))
                    min_price = max_price = price_decimal
            except (ValueError, InvalidOperation):
                pass
            
            zone = ZoneData(
                zone_id=str(zone_id),
                source_website="david_h_koch_theater",
                name=zone_name,
                raw_identifier=str(zone_id),
                min_price=min_price,
                max_price=max_price,
                miscellaneous={
                    "price_type_no": zone_data.get('price_type_no'),
                    "amt": zone_data.get('amt'),
                    "fee": zone_data.get('fee', 0),
                    "tax": zone_data.get('tax', 0)
                }
            )
            zones.append(zone)
        
        return zones

    def _process_levels(self, section_data: List[Dict[str, Any]]) -> List[LevelData]:
        """Process David H Koch Theater levels from Section data"""
        levels_map = {}
        
        for section in section_data:
            level_name = section.get('section_desc', 'Unknown Level')
            level_id = section.get('section_no', level_name)
            
            if level_id not in levels_map:
                levels_map[level_id] = LevelData(
                    level_id=str(level_id),
                    source_website="david_h_koch_theater",
                    name=self._format_level_name(level_name),
                    raw_name=level_name
                )
        
        return list(levels_map.values())

    def _process_sections(self, section_data: List[Dict[str, Any]], levels: List[LevelData]) -> List[SectionData]:
        """Process David H Koch Theater sections"""
        sections = []
        level_lookup = {level.level_id: level for level in levels}
        
        for section in section_data:
            section_id = str(section.get('section_no', 'unknown'))
            section_name = section.get('section_desc', f'Section {section_id}')
            
            # Find matching level
            level_id = section_id
            if level_id not in level_lookup and levels:
                level_id = levels[0].level_id
            
            if level_id in level_lookup:
                section_obj = SectionData(
                    section_id=section_id,
                    level_id=level_id,
                    source_website="david_h_koch_theater",
                    name=section_name,
                    raw_name=section_name
                )
                sections.append(section_obj)
        
        return sections

    def _process_seats(self, seats_data: List[Dict[str, Any]], section_data: List[Dict[str, Any]],
                      seat_type_data: List[Dict[str, Any]], zones: List[ZoneData], 
                      levels: List[LevelData], sections: List[SectionData]) -> List[SeatData]:
        """Process David H Koch Theater seats"""
        seats = []
        
        # Create lookups
        zone_lookup = {zone.zone_id: zone for zone in zones}
        section_lookup = {section.section_id: section for section in sections}
        seat_type_lookup = {st.get('seat_type_no'): st for st in seat_type_data}
        
        for seat_data in seats_data:
            seat_id = seat_data.get('seat_no', 'unknown')
            section_no = str(seat_data.get('section_no', 'unknown'))
            price_type_no = str(seat_data.get('price_type_no', 'unknown'))
            
            # Find matching section and zone
            section = section_lookup.get(section_no)
            zone = zone_lookup.get(price_type_no)
            
            if section and zone:
                # Get seat price from zone
                price = zone.min_price
                
                # Determine seat status
                status = "available"
                if seat_data.get('seat_status_no') != 1:  # Assuming 1 = available
                    status = "unavailable"
                
                seat = SeatData(
                    seat_id=str(seat_id),
                    section_id=section_no,
                    zone_id=price_type_no,
                    source_website="david_h_koch_theater",
                    row_label=seat_data.get('row_desc', '1'),
                    seat_number=seat_data.get('seat_desc', '1'),
                    seat_type="standard",
                    status=status,
                    price=price,
                    available=(status == "available"),
                    level_id=section.level_id
                )
                seats.append(seat)
        
        return seats

    def _get_venue_from_database(self, performance_info: PerformanceData):
        """Fetch venue object from database for markup pricing."""
        try:
            # Import Venue model
            from ...models import Venue
            
            # Get venue using source_venue_id and source_website from performance
            venue = Venue.objects.filter(
                source_venue_id=performance_info.venue_source_id,
                source_website="david_h_koch_theater"
            ).first()
            
            return venue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not fetch venue from database: {e}")
            return None

    def _format_level_name(self, level: str) -> str:
        """Format level name for display"""
        if not level:
            return "Unknown Level"
        
        level_lower = level.lower()
        if 'orchestra' in level_lower:
            return "Orchestra"
        elif 'ring' in level_lower or 'tier' in level_lower:
            return f"Ring {level}"
        elif 'balcony' in level_lower:
            return "Balcony"
        elif 'box' in level_lower:
            return "Box Seats"
        else:
            return level.title()