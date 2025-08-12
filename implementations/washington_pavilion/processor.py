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
from ...models import Venue  # Add this import for DB access

# Utility function to fetch seat_structure from DB (shared)
def get_venue_seat_structure(source_venue_id: str, source_website: str) -> str:
    try:
        # Use raw SQL to avoid async context issues
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

class WashingtonPavilionProcessor:

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        
    def process(self, pricing_data: Optional[Dict[str, Any]], seats_data: Optional[Dict[str, Any]],
                url: str, scrape_job_id: Optional[str] = None, enriched_data: Dict[str, Any] = None) -> ScrapedData:
        # Note: enriched_data parameter kept for backwards compatibility but no longer used for markup pricing
        
        venue_info = self._process_venue_info(pricing_data.get('venue_info', {}) if pricing_data else {})
        event_info = self._process_event_info(pricing_data, venue_info.source_venue_id, url)
        performance_info = self._process_performance_info(pricing_data, event_info.source_event_id, venue_info.source_venue_id, url)
        
        zones = self._process_zones(pricing_data.get('priceRangeCategories', []) if pricing_data else [])
        levels = self._process_levels(pricing_data.get('priceRangeCategories', []) if pricing_data else [])
        sections = self._process_sections(levels)
        seats = self._process_seats(seats_data.get('features', []) if seats_data else [], zones, levels, sections)
        
        # Fetch seat structure from DB if set, otherwise auto-detect
        seat_structure = get_venue_seat_structure(venue_info.source_venue_id, venue_info.source_website)
        if not seat_structure:
            if seats:
                seat_structure = detect_venue_seat_structure(seats)
            else:
                # Default to consecutive if no seats are available for analysis
                seat_structure = "consecutive"
        
        # Validate seat structure value
        valid_structures = ["consecutive", "odd_even"]
        if seat_structure not in valid_structures:
            seat_structure = "consecutive"  # Default fallback
        
        venue_info.seat_structure = seat_structure
        
        # Update sections with appropriate numbering scheme
        for section in sections:
            if hasattr(section, 'numbering_scheme'):
                section.numbering_scheme = "odd-even" if seat_structure == "odd_even" else "consecutive"
        
        seat_packs = self._process_seat_packs(seats, sections, performance_info)

        return ScrapedData(
            source_website=self.config.get('source_website', "washington_pavilion"),
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
                scraper_name=self.config.get('scraper_name', "washington_pavilion_scraper_v5")
            ),
            scraper_version=self.config.get('scraper_version', "v5")
        )

    def _process_venue_info(self, venue_data: Dict[str, Any]) -> VenueData:
        venue_name = venue_data.get('name') or self.config.get('default_venue_name', 'Washington Pavilion')
        city = venue_data.get('city') or self.config.get('default_city', 'Sioux Falls')
        state = venue_data.get('state') or self.config.get('default_state', 'SD')
        
        unique_string = f"{venue_name}{city}{state}"
        source_venue_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        return VenueData(
            name=venue_name,
            source_venue_id=source_venue_id,
            source_website=self.config.get('source_website', "washington_pavilion"),
            city=city,
            state=state,
            country=venue_data.get('country', self.config.get('default_country', 'US')),
            address=venue_data.get('address'),
            venue_timezone=self.config.get('venue_timezone', 'America/Chicago')
        )

    def _process_event_info(self, pricing_data: Optional[Dict[str, Any]], venue_source_id: str, url: str) -> EventData:
        event_name = pricing_data.get('title') if pricing_data else self.config.get('default_event_name', "Washington Pavilion Performance")
        
        product_id = 'unknown'
        if url:
            try:
                product_id = parse_qs(urlparse(url).query).get('productId', ['unknown'])[0]
            except (IndexError, AttributeError):
                product_id = 'unknown'

        if product_id == 'unknown':
            event_date = pricing_data.get('date') if pricing_data else ''
            event_time = pricing_data.get('time') if pricing_data else ''
            unique_string = f"{event_name}{venue_source_id}{event_date}{event_time}"
            source_event_id = hashlib.md5(unique_string.encode()).hexdigest()
        else:
            source_event_id = product_id
        
        return EventData(
            name=event_name,
            source_event_id=source_event_id,
            source_website=self.config.get('source_website', "washington_pavilion"),
            url=url,
            currency=self.config.get('currency', 'USD'),
            title=event_name,
            description=event_name
        )

    def _process_performance_info(self, pricing_data: Optional[Dict[str, Any]], event_source_id: str, venue_source_id: str, url: str) -> PerformanceData:
        product_id = 'unknown'
        if url:
            try:
                product_id = parse_qs(urlparse(url).query).get('productId', ['unknown'])[0]
            except (IndexError, AttributeError):
                product_id = 'unknown'
        
        if product_id == 'unknown':
            event_date = pricing_data.get('date') if pricing_data else ''
            event_time = pricing_data.get('time') if pricing_data else ''
            performance_datetime_str = f"{event_date} {event_time}"
            unique_string = f"{event_source_id}{venue_source_id}{performance_datetime_str}"
            source_performance_id = hashlib.md5(unique_string.encode()).hexdigest()
        else:
            source_performance_id = product_id
        
        performance_datetime = datetime.utcnow()
        event_date = pricing_data.get('date') if pricing_data else None
        event_time = pricing_data.get('time') if pricing_data else None
        
        if event_date and event_time:
            try:
                datetime_str = f"{event_date} {event_time}"
                performance_datetime = parser.parse(datetime_str)
            except (ValueError, TypeError) as e:
                # Log the parsing error but continue with default UTC time
                pass
        
        return PerformanceData(
            source_performance_id=source_performance_id,
            source_website=self.config.get('source_website', "washington_pavilion"),
            performance_datetime_utc=performance_datetime,
            event_source_id=event_source_id,
            venue_source_id=venue_source_id,
            seat_map_url=url,
            performance_url=url
        )

    def _process_zones(self, categories: List[Dict[str, Any]]) -> List[ZoneData]:
        zones = []
        
        for category in categories:
            min_price_cents = category.get('minPrice', 0)
            max_price_cents = category.get('maxPrice', 0)
            
            # Validate price data is numeric and positive
            min_price = None
            max_price = None
            
            if min_price_cents and isinstance(min_price_cents, (int, float)) and min_price_cents > 0:
                min_price = Decimal(str(min_price_cents / 1000))
            
            if max_price_cents and isinstance(max_price_cents, (int, float)) and max_price_cents > 0:
                max_price = Decimal(str(max_price_cents / 1000))
            
            # Validate availability data
            availability = 0
            area_blocks = category.get('areaBlocksAvailability', {})
            if isinstance(area_blocks, dict):
                for avail in area_blocks.values():
                    if isinstance(avail, dict) and 'availability' in avail:
                        avail_value = avail.get('availability', 0)
                        if isinstance(avail_value, (int, float)) and avail_value >= 0:
                            availability += avail_value
            
            # Validate zone name data
            zone_name_data = category.get('name', {})
            zone_name = 'Unknown Zone'
            if isinstance(zone_name_data, dict) and 'en' in zone_name_data:
                zone_name = zone_name_data['en'] or 'Unknown Zone'
            
            zone_features = ZoneFeaturesData(
                view_description=zone_name
            )
            
            # Validate category ID
            category_id = category.get('id')
            if not isinstance(category_id, (int, str)):
                continue  # Skip invalid categories
            
            # Validate colors
            bg_color = category.get('bgColor', 'CCCCCC')
            text_color = category.get('textColor', '000000')
            if not isinstance(bg_color, str):
                bg_color = 'CCCCCC'
            if not isinstance(text_color, str):
                text_color = '000000'
            
            # Validate display order
            display_order = category.get('rank', 0)
            if not isinstance(display_order, (int, float)):
                display_order = 0
            
            zone = ZoneData(
                zone_id=str(category_id),
                source_website=self.config.get('source_website', "washington_pavilion"),
                name=zone_name,
                features=zone_features,
                raw_identifier=str(category_id),
                color_code=f"#{bg_color}",
                display_order=int(display_order),
                min_price=min_price,
                max_price=max_price,
                wheelchair_accessible="wheelchair" in zone_name.lower(),
                miscellaneous={
                    'text_color': f"#{text_color}",
                    'blocks': [{"id": block.get('id'), "name": block.get('name', {}).get('en', 'Unknown')} 
                              for block in category.get('blocks', []) if isinstance(block, dict)],
                    'availability': availability
                }
            )
            zones.append(zone)
        
        return sorted(zones, key=lambda x: x.display_order)

    def _process_levels(self, categories: List[Dict[str, Any]]) -> List[LevelData]:
        levels_map = {}
        
        for category in categories:
            for block in category.get('blocks', []):
                block_id = str(block.get('id'))
                block_name = block.get('name', {}).get('en', 'Unknown Level')
                
                if block_id not in levels_map:
                    levels_map[block_id] = LevelData(
                        level_id=block_id,
                        source_website=self.config.get('source_website', "washington_pavilion"),
                        name=block_name,
                        raw_name=block_name,
                        display_order=0
                    )
        
        return list(levels_map.values())

    def _process_sections(self, levels: List[LevelData]) -> List[SectionData]:
        sections = []
        
        for level in levels:
            section = SectionData(
                section_id=f"section_{level.level_id}",
                level_id=level.level_id,
                source_website=self.config.get('source_website', "washington_pavilion"),
                name=f"Main Section - {level.name}",
                raw_name=level.name,
                display_order=0
            )
            sections.append(section)
        
        return sections

    def _process_seats(self, features: List[Dict[str, Any]], zones: List[ZoneData], 
                      levels: List[LevelData], sections: List[SectionData]) -> List[SeatData]:
        seats = []
        zone_lookup = {zone.zone_id: zone for zone in zones}
        level_lookup = {level.level_id: level for level in levels}
        section_lookup = {section.level_id: section for section in sections}
        
        for feature in features:
            # Validate feature structure
            if not isinstance(feature, dict):
                continue
                
            props = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            # Validate properties structure
            if not isinstance(props, dict):
                continue
                
            # Validate required seat data
            seat_id = props.get('id')
            if not seat_id:
                continue
                
            # Validate block information
            block_info = props.get('block', {})
            if not isinstance(block_info, dict):
                continue
                
            level_id = str(block_info.get('id', ''))
            if not level_id:
                continue
                
            # Validate zone information
            zone_id = props.get('seatCategoryId')
            if zone_id is None:
                continue
            zone_id = str(zone_id)
            
            # Validate lookups
            section = section_lookup.get(level_id)
            zone = zone_lookup.get(zone_id)
            
            if not section or not zone:
                continue
            
            # Validate geometry and coordinates
            coordinates = [None, None]
            if isinstance(geometry, dict):
                coord_data = geometry.get('coordinates', [None, None])
                if isinstance(coord_data, list) and len(coord_data) >= 2:
                    coordinates = coord_data[:2]
            
            # Validate coordinate values
            x_coord = None
            y_coord = None
            if coordinates[0] is not None:
                try:
                    x_coord = Decimal(str(coordinates[0]))
                except (ValueError, TypeError):
                    pass
                    
            if coordinates[1] is not None:
                try:
                    y_coord = Decimal(str(coordinates[1]))
                except (ValueError, TypeError):
                    pass
            
            # Validate seat row and number
            row_label = props.get('row', '1')
            seat_number = props.get('number', '1')
            
            if not isinstance(row_label, str):
                row_label = str(row_label) if row_label else '1'
            if not isinstance(seat_number, str):
                seat_number = str(seat_number) if seat_number else '1'
            
            seat = SeatData(
                seat_id=str(seat_id),
                section_id=section.section_id,
                zone_id=zone_id,
                source_website=self.config.get('source_website', "washington_pavilion"),
                row_label=row_label,
                seat_number=seat_number,
                row=props.get('row'),
                number=props.get('number'),
                x_coord=x_coord,
                y_coord=y_coord,
                status="available",
                price=zone.min_price,
                level_id=level_id
            )
            seats.append(seat)
        
        return seats

    def _process_seat_packs(self, seats: List[SeatData], sections: List[SectionData], performance_info: PerformanceData) -> List[SeatPackData]:
        """Process seat packs using the generic seat pack generator."""
        # Washington Pavilion venue prefix mapping
        venue_prefix_map = {
            self.config.get('source_website', "washington_pavilion"): self.config.get('venue_prefix', "wp")
        }
        
        available_seats = [seat for seat in seats if seat.status == "available"]
        
        # Fetch venue object from database for markup pricing
        venue = self._get_venue_from_database(performance_info)
        
        return generate_seat_packs(
            all_seats=available_seats,
            all_sections=sections,
            performance=performance_info,
            venue_prefix_map=venue_prefix_map,
            venue=venue,  # Pass venue object with markup data from NestJS
            min_pack_size=self.config.get('min_pack_size', 2),  # Configurable minimum pack size
            packing_strategy=self.config.get('packing_strategy', "maximal")  # Configurable packing strategy
        )

    def _get_venue_from_database(self, performance_info: PerformanceData):
        """Fetch venue object from database for markup pricing."""
        try:
            # Import Venue model
            from ...models import Venue
            
            # Get venue using source_venue_id and source_website from performance
            venue = Venue.objects.filter(
                source_venue_id=performance_info.venue_source_id,
                source_website=self.config.get('source_website', "washington_pavilion")
            ).first()
            
            return venue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not fetch venue from database: {e}")
            return None

