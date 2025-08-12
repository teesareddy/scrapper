from typing import Dict, List, Any, Optional
from datetime import datetime
from decimal import Decimal
import hashlib
import re
from urllib.parse import urlparse, parse_qs
from dateutil import parser
from ...core.data_schemas import (
    ScrapedData, VenueData, EventData, PerformanceData,
    LevelData, ZoneData, SectionData, SeatData, SeatPackData,
    ScraperConfigData, ZoneFeaturesData
)
from ...core.seat_pack_generator import generate_seat_packs, detect_venue_seat_structure


class DemoScraperProcessor:

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        
    def process(self, raw_extracted_data: Dict[str, Any], url: str, scrape_job_id: Optional[str] = None) -> ScrapedData:
        performance_data = raw_extracted_data.get('performance_info', {}).get('performance', {})
        meta_data = raw_extracted_data.get('performance_info', {}).get('meta', {})
        seats_data = raw_extracted_data.get('seats_info', {}).get('seats', [])

        venue_info = self._process_venue_info(performance_data)
        event_info = self._process_event_info(performance_data, venue_info.source_venue_id, url)
        performance_info = self._process_performance_info(performance_data, event_info.source_event_id, venue_info.source_venue_id, url)
        
        zones = self._process_zones(meta_data.get('zones', []))
        levels = self._process_levels(meta_data.get('levels', []))
        sections = self._process_sections(meta_data.get('sections', []), seats_data, meta_data.get('levels', []))
        seats = self._process_seats(seats_data, zones, levels, sections)
        
        seat_structure = "consecutive"
        venue_info.seat_structure = seat_structure
        
        for section in sections:
            if hasattr(section, 'numbering_scheme'):
                section.numbering_scheme = "odd-even" if seat_structure == "odd_even" else "consecutive"
        
        seat_packs = self._process_seat_packs(seats, sections, performance_info)

        return ScrapedData(
            source_website=self.config.get('source_website', "demo_scraper"),
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
                scraper_name=self.config.get('scraper_name', "demo_scraper_v1")
            ),
            scraper_version=self.config.get('scraper_version', "v1")
        )

    def _process_venue_info(self, performance_data: Dict[str, Any]) -> VenueData:
        venue_name = performance_data.get('venue') or self.config.get('default_venue_name', 'Demo Venue')
        location = performance_data.get('location', '') or self.config.get('default_location', 'Demo City, DS')
        city = location.split(',')[0].strip() if ',' in location else self.config.get('default_city', 'Demo City')
        state = location.split(',')[-1].strip() if ',' in location else self.config.get('default_state', 'DS')
        address = performance_data.get('address') or self.config.get('default_address', '123 Demo St')
        
        unique_string = f"{venue_name}{city}{state}{address}"
        source_venue_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        return VenueData(
            name=venue_name,
            source_venue_id=source_venue_id,
            source_website=self.config.get('source_website', "demo_scraper"),
            city=city,
            state=state,
            country=self.config.get('default_country', 'US'),
            address=address,
            venue_timezone=self.config.get('venue_timezone', 'America/New_York')
        )

    def _process_event_info(self, performance_data: Dict[str, Any], venue_source_id: str, url: str) -> EventData:
        event_name = performance_data.get('event') or self.config.get('default_event_name', "Demo Event")
        
        unique_string = f"{event_name}{venue_source_id}"
        source_event_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        return EventData(
            name=event_name,
            source_event_id=source_event_id,
            source_website=self.config.get('source_website', "demo_scraper"),
            url=url,
            currency=self.config.get('currency', 'USD'),
            title=event_name,
            description=event_name
        )

    def _process_performance_info(self, performance_data: Dict[str, Any], event_source_id: str, venue_source_id: str, url: str) -> PerformanceData:
        performance_datetime_str = performance_data.get('datetime')
        
        performance_datetime = datetime.utcnow()
        if performance_datetime_str:
            try:
                performance_datetime = parser.parse(performance_datetime_str)
            except (ValueError, TypeError):
                pass
        
        unique_string = f"{event_source_id}{venue_source_id}{performance_datetime_str}"
        source_performance_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        return PerformanceData(
            source_performance_id=source_performance_id,
            source_website=self.config.get('source_website', "demo_scraper"),
            performance_datetime_utc=performance_datetime,
            event_source_id=event_source_id,
            venue_source_id=venue_source_id,
            seat_map_url=url,
            performance_url=url
        )

    def _process_zones(self, zones_data: List[Dict[str, Any]]) -> List[ZoneData]:
        zones = []
        processed_count = 0
        skipped_count = 0
        
        for zone_data in zones_data:
            try:
                zone_id = str(zone_data.get('id'))
                name = zone_data.get('name')
                color = zone_data.get('color')
                
                if not all([zone_id, name, color]):
                    skipped_count += 1
                    continue
                
                # For demo scraper, zones don't have prices - they get prices from levels
                # We'll set a default price that will be overridden by level prices
                default_price = Decimal('10.00')
                
                zones.append(ZoneData(
                    zone_id=zone_id,
                    source_website=self.config.get('source_website', "demo_scraper"),
                    name=name,
                    features=ZoneFeaturesData(view_description=name),
                    raw_identifier=zone_id,
                    color_code=color,
                    display_order=0,
                    min_price=default_price,
                    max_price=default_price,
                    wheelchair_accessible=False
                ))
                processed_count += 1
                    
            except Exception:
                skipped_count += 1
                continue
        return zones

    def _process_levels(self, levels_data: List[Dict[str, Any]]) -> List[LevelData]:
        levels = []
        processed_count = 0
        skipped_count = 0
        
        for level_data in levels_data:
            try:
                level_id = str(level_data.get('id'))
                name = level_data.get('name')
                
                if not all([level_id, name]):
                    skipped_count += 1
                    continue
                
                # Extract price from level data
                price = None
                try:
                    if level_data.get('price') is not None:
                        price = Decimal(str(level_data.get('price')))
                except (ValueError, TypeError):
                    pass
                
                # Set default testing prices if no price was extracted
                if price is None:
                    # Set different default prices based on level name for testing variety
                    level_name_lower = name.lower()
                    if any(keyword in level_name_lower for keyword in ['vip', 'premium', 'floor', 'box', 'suite']):
                        price = Decimal('20.00')  # Premium levels get $20
                    else:
                        price = Decimal('10.00')  # Standard levels get $10
                
                levels.append(LevelData(
                    level_id=level_id,
                    source_website=self.config.get('source_website', "demo_scraper"),
                    name=name,
                    raw_name=name,
                    display_order=0,
                    price=price
                ))
                processed_count += 1
                
            except Exception:
                skipped_count += 1
                continue
        return levels

    def _process_sections(self, sections_data: List[Dict[str, Any]], seats_data: List[Dict[str, Any]], levels_data: List[Dict[str, Any]]) -> List[SectionData]:
        sections = []
        processed_count = 0
        skipped_count = 0
        
        # Build a comprehensive mapping of section-level relationships
        section_to_levels_map = {}
        for seat in seats_data:
            section_id = str(seat.get('sectionId'))
            level_id = str(seat.get('levelId'))
            if section_id and level_id:
                if section_id not in section_to_levels_map:
                    section_to_levels_map[section_id] = set()
                section_to_levels_map[section_id].add(level_id)
        
        # Create a lookup map for level names
        level_name_map = {}
        for level_data in levels_data:
            level_id = str(level_data.get('id'))
            level_name = level_data.get('name')
            if level_id and level_name:
                level_name_map[level_id] = level_name
        
        # Process each section and create separate sections for each level
        for section_data in sections_data:
            try:
                section_id = str(section_data.get('id'))
                name = section_data.get('name')
                
                if not all([section_id, name]):
                    skipped_count += 1
                    continue
                
                # Get all levels that use this section
                levels_for_section = section_to_levels_map.get(section_id, set())
                
                # Create a separate section for each level
                for level_id in levels_for_section:
                    # Create unique section ID for each level-section combination
                    unique_section_id = f"{level_id}_{section_id}"
                    
                    sections.append(SectionData(
                        section_id=unique_section_id,
                        level_id=level_id,
                        source_website=self.config.get('source_website', "demo_scraper"),
                        name=name,
                        raw_name=name,
                        display_order=0
                    ))
                    processed_count += 1
                    
            except Exception:
                skipped_count += 1
                continue
        
        # Find levels that have seats but no sections created
        levels_with_seats = set()
        levels_with_sections = set()
        for seat in seats_data:
            level_id = str(seat.get('levelId'))
            section_id = str(seat.get('sectionId'))
            if level_id and section_id:
                levels_with_seats.add(level_id)
                if section_id in section_to_levels_map:
                    levels_with_sections.add(level_id)
        
        # Create default sections for levels that have seats but no explicit sections
        for level_id in levels_with_seats:
            if level_id not in levels_with_sections:
                # Use the actual level name as section name
                level_name = level_name_map.get(level_id, f"Level {level_id}")
                
                sections.append(SectionData(
                    section_id=level_id,  # Use level_id as section_id
                    level_id=level_id,
                    source_website=self.config.get('source_website', "demo_scraper"),
                    name=level_name,  # Same name as level
                    raw_name=level_name,
                    display_order=0
                ))
                processed_count += 1
        
        return sections

    def _process_seats(self, seats_data: List[Dict[str, Any]], zones: List[ZoneData], 
                      levels: List[LevelData], sections: List[SectionData]) -> List[SeatData]:
        seats = []
        zone_lookup = {zone.zone_id: zone for zone in zones}
        level_lookup = {level.level_id: level for level in levels}
        section_lookup = {section.section_id: section for section in sections}
        
        processed_count = 0
        skipped_count = 0
        
        for seat_data in seats_data:
            try:
                seat_id = str(seat_data.get('id'))
                row_label = seat_data.get('row')
                seat_number = seat_data.get('number')
                
                if not all([seat_id, row_label, seat_number]):
                    skipped_count += 1
                    continue
                
                x_coord = None
                y_coord = None
                try:
                    if seat_data.get('x') is not None:
                        x_coord = Decimal(str(seat_data.get('x')))
                    if seat_data.get('y') is not None:
                        y_coord = Decimal(str(seat_data.get('y')))
                except (ValueError, TypeError):
                    pass
                
                zone_id = str(seat_data.get('zoneId'))
                level_id = str(seat_data.get('levelId'))
                original_section_id = str(seat_data.get('sectionId'))
                is_wheelchair = seat_data.get('is_wheelchair_accessible', False)
                status = seat_data.get('status', 'available')

                zone = zone_lookup.get(zone_id)
                level = level_lookup.get(level_id)
                
                # Find the correct section using the level-section combination
                unique_section_id = f"{level_id}_{original_section_id}"
                section = section_lookup.get(unique_section_id)
                
                # Fallback to original section ID if unique one doesn't exist
                if not section:
                    section = section_lookup.get(original_section_id)

                if not zone:
                    skipped_count += 1
                    continue
                if not level:
                    skipped_count += 1
                    continue
                if not section:
                    skipped_count += 1
                    continue

                # Get price from level instead of zone for demo scraper
                price = getattr(level, 'price', None) or getattr(level, 'min_price', None) or Decimal('10.00')
                
                seats.append(SeatData(
                    seat_id=seat_id,
                    section_id=section.section_id,
                    zone_id=zone.zone_id,
                    source_website=self.config.get('source_website', "demo_scraper"),
                    row_label=row_label,
                    seat_number=seat_number,
                    row=row_label,
                    number=seat_number,
                    x_coord=x_coord,
                    y_coord=y_coord,
                    status=status,
                    price=price,
                    level_id=level.level_id,
                ))
                processed_count += 1
                
            except Exception:
                skipped_count += 1
                continue
        return seats

    def _process_seat_packs(self, seats: List[SeatData], sections: List[SectionData], performance_info: PerformanceData) -> List[SeatPackData]:
        venue_prefix_map = {
            self.config.get('source_website', "demo_scraper"): self.config.get('venue_prefix', "ds")
        }
        
        available_seats = [seat for seat in seats if seat.status == "available"]
        
        return generate_seat_packs(
            all_seats=available_seats,
            all_sections=sections,
            performance=performance_info,
            venue_prefix_map=venue_prefix_map,
            venue=None,
            min_pack_size=self.config.get('min_pack_size', 2),
            packing_strategy=self.config.get('packing_strategy', "maximal")
        )
