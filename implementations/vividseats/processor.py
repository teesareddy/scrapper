"""
VividSeats processor for data transformation and seat pack generation
Using Washington Pavilion's approach with the generic seat pack generator
"""
from typing import Dict, List, Any, Optional
from datetime import datetime
from decimal import Decimal
import hashlib
import re
from urllib.parse import urlparse
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


class VividSeatsProcessor:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        
    def process(self, listings_data: Optional[Dict[str, Any]], production_details: Optional[Dict[str, Any]],
                url: str, scrape_job_id: Optional[str] = None, enriched_data: Dict[str, Any] = None) -> ScrapedData:
        
        # Extract global info from listings data
        global_info = self._get_global_info(listings_data)
        
        venue_info = self._process_venue_info(global_info, production_details)
        event_info = self._process_event_info(global_info, production_details, venue_info.source_venue_id, url)
        performance_info = self._process_performance_info(global_info, production_details, event_info.source_event_id, venue_info.source_venue_id, url)
        
        zones = self._process_zones(listings_data.get('groups', []) if listings_data else [])
        levels = self._process_levels(listings_data.get('sections', []) if listings_data else [])
        sections = self._process_sections(levels)
        # Pass the original sections data to _process_seats for proper mapping
        seats = self._process_seats(listings_data.get('tickets', []) if listings_data else [], zones, levels, sections, 
                                   listings_data.get('sections', []) if listings_data else [])
        
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
            source_website=self.config.get('source_website', "vividseats"),
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
                scraper_name=self.config.get('scraper_name', "vividseats_scraper_v1")
            ),
            scraper_version=self.config.get('scraper_version', "v1")
        )

    def _get_global_info(self, listings_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Extract global info from listings data"""
        if not listings_data:
            return {}
        
        global_list = listings_data.get("global", [])
        if not global_list:
            return {}
        
        return global_list[0]

    def _process_venue_info(self, global_info: Dict[str, Any], production_details: Optional[Dict[str, Any]]) -> VenueData:
        """Process venue information from VividSeats data"""
        venue_name = global_info.get('mapTitle', 'Unknown Venue')
        city = 'Unknown City'
        state = 'Unknown State'
        country = 'US'
        address = None
        venue_timezone = global_info.get('venueTimeZone', 'America/New_York')
        
        # Try to get venue details from production details
        if production_details and 'venue' in production_details:
            venue_data = production_details['venue']
            venue_name = venue_data.get('name', venue_name)
            city = venue_data.get('city', city)
            state = venue_data.get('state', state)
            country = venue_data.get('countryCode', country)
            address1 = venue_data.get('address1', '')
            address2 = venue_data.get('address2', '')
            if address1:
                address = f"{address1}"
                if address2:
                    address += f", {address2}"
            venue_timezone = venue_data.get('timezone', venue_timezone)
        
        # Fallback to global info
        if city == 'Unknown City':
            address1 = global_info.get('venueAddress1', '')
            address2 = global_info.get('venueAddress2', '')
            state = global_info.get('venueState', state)
            if address1:
                address = f"{address1}"
                if address2:
                    address += f", {address2}"
        
        unique_string = f"{venue_name}{city}{state}"
        source_venue_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        return VenueData(
            name=venue_name,
            source_venue_id=source_venue_id,
            source_website=self.config.get('source_website', "vividseats"),
            city=city,
            state=state,
            country=country,
            address=address,
            venue_timezone=venue_timezone
        )

    def _process_event_info(self, global_info: Dict[str, Any], production_details: Optional[Dict[str, Any]], venue_source_id: str, url: str) -> EventData:
        """Process event information"""
        event_name = global_info.get('productionName', 'Unknown Event')
        event_id = global_info.get('eventId', '')
        production_id = global_info.get('productionId', '')
        
        # Use production ID as source event ID if available
        source_event_id = production_id if production_id else event_id
        
        if not source_event_id:
            # Generate ID from event details
            event_date = production_details.get('utcDate', '') if production_details else ''
            unique_string = f"{event_name}{venue_source_id}{event_date}"
            source_event_id = hashlib.md5(unique_string.encode()).hexdigest()
        
        return EventData(
            name=event_name,
            source_event_id=str(source_event_id),
            source_website=self.config.get('source_website', "vividseats"),
            url=url,
            currency='USD',
            title=event_name,
            description=event_name
        )

    def _process_performance_info(self, global_info: Dict[str, Any], production_details: Optional[Dict[str, Any]], event_source_id: str, venue_source_id: str, url: str) -> PerformanceData:
        """Process performance information"""
        production_id = global_info.get('productionId', '')
        source_performance_id = production_id if production_id else event_source_id
        
        # Get performance datetime
        performance_datetime = datetime.utcnow()
        if production_details:
            event_date = production_details.get('utcDate', '')
            if event_date:
                try:
                    performance_datetime = parser.parse(event_date)
                except (ValueError, TypeError):
                    pass
        
        return PerformanceData(
            source_performance_id=str(source_performance_id),
            source_website=self.config.get('source_website', "vividseats"),
            performance_datetime_utc=performance_datetime,
            event_source_id=event_source_id,
            venue_source_id=venue_source_id,
            seat_map_url=url,
            performance_url=url
        )

    def _process_zones(self, groups: List[Dict[str, Any]]) -> List[ZoneData]:
        """Process zone/price group information"""
        zones = []
        
        for group in groups:
            if not isinstance(group, dict):
                continue
                
            zone_id = str(group.get('i', ''))
            zone_name = group.get('n', 'Unknown Zone')
            
            if not zone_id:
                continue
            
            # Parse prices
            min_price = None
            max_price = None
            
            try:
                low_price = group.get('l', '')
                high_price = group.get('h', '')
                
                if low_price and isinstance(low_price, (str, int, float)):
                    min_price = Decimal(str(low_price))
                
                if high_price and isinstance(high_price, (str, int, float)):
                    max_price = Decimal(str(high_price))
            except (ValueError, TypeError):
                pass
            
            # Parse availability
            availability = 0
            try:
                qty = group.get('q', '0')
                if qty and isinstance(qty, (str, int, float)):
                    availability = int(float(qty))
            except (ValueError, TypeError):
                pass
            
            zone_features = ZoneFeaturesData(
                view_description=zone_name
            )
            
            zone = ZoneData(
                zone_id=zone_id,
                source_website=self.config.get('source_website', "vividseats"),
                name=zone_name,
                features=zone_features,
                raw_identifier=zone_id,
                color_code="#CCCCCC",
                display_order=0,
                min_price=min_price,
                max_price=max_price,
                wheelchair_accessible="wheelchair" in zone_name.lower() or "accessible" in zone_name.lower(),
                miscellaneous={
                    'availability': availability,
                    'zone_description': group.get('zd', ''),
                    'category': group.get('c', ''),
                    'type': group.get('t', '')
                }
            )
            zones.append(zone)
        
        return zones

    def _process_levels(self, sections: List[Dict[str, Any]]) -> List[LevelData]:
        """Process level information from sections"""
        levels_map = {}
        
        for section in sections:
            if not isinstance(section, dict):
                continue
                
            level_id = str(section.get('g', ''))  # Group/Level ID
            section_name = section.get('n', 'Unknown Level')
            
            if level_id and level_id not in levels_map:
                levels_map[level_id] = LevelData(
                    level_id=level_id,
                    source_website=self.config.get('source_website', "vividseats"),
                    name=section_name,
                    raw_name=section_name,
                    display_order=0
                )
        
        return list(levels_map.values())

    def _process_sections(self, levels: List[LevelData]) -> List[SectionData]:
        """Process section information"""
        sections = []
        
        for level in levels:
            section = SectionData(
                section_id=f"section_{level.level_id}",
                level_id=level.level_id,
                source_website=self.config.get('source_website', "vividseats"),
                name=f"Main Section - {level.name}",
                raw_name=level.name,
                display_order=0
            )
            sections.append(section)
        
        return sections

    def _process_seats(self, tickets: List[Dict[str, Any]], zones: List[ZoneData], 
                      levels: List[LevelData], sections: List[SectionData], 
                      original_sections: List[Dict[str, Any]] = None) -> List[SeatData]:
        """Process individual seat information from tickets"""
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"🎫 Starting seat processing with {len(tickets)} tickets")
        seats = []
        zone_lookup = {zone.zone_id: zone for zone in zones}
        level_lookup = {level.level_id: level for level in levels}
        section_lookup = {section.level_id: section for section in sections}
        
        # Build section_name_to_ids mapping exactly like the reference
        section_name_to_ids = {}
        if original_sections:
            for section in original_sections:
                section_name = section.get("n", "").strip().lower()
                section_name_to_ids[section_name] = {
                    "section_id": section.get("i", ""),
                    "level_id": section.get("g", "")
                }
        else:
            # Fallback to the old method if original_sections not provided
            for level in levels:
                section_name = level.name.strip().lower()
                section_name_to_ids[section_name] = {
                    "section_id": level.level_id,
                    "level_id": level.level_id
                }
        
        logger.info(f"🗂️ Built section mapping for {len(section_name_to_ids)} sections")
        
        tickets_processed = 0
        tickets_skipped = 0
        total_seat_numbers_found = 0
        
        for ticket in tickets:
            if not isinstance(ticket, dict):
                tickets_skipped += 1
                logger.debug(f"⚠️ Skipping non-dict ticket: {type(ticket)}")
                continue
            
            tickets_processed += 1
            
            # Extract ticket information exactly like the reference
            section_name = ticket.get('l', '').strip().lower()
            row = ticket.get('r', '').strip()
            seat_numbers_str = ticket.get('m', '')
            price_str = ticket.get('p', '0')
            pricing_zone = ticket.get('z', '')
            pack_size = int(ticket.get('q', 1))
            view_type = ticket.get('stp', '').strip()
            accessibility = ticket.get('di', False)
            badges = [badge.get('title') for badge in ticket.get('badges', []) if isinstance(badge, dict)]
            visibility_type = ticket.get('vs', None)
            companion_seats = ticket.get('ind', False)
            
            # Log ticket details for debugging
            if tickets_processed <= 5:  # Log first 5 tickets
                logger.info(f"🎫 Ticket {tickets_processed}: section='{section_name}', row='{row}', seats='{seat_numbers_str}', price='{price_str}'")
            
            # Skip only if no seat numbers (like reference)
            if not seat_numbers_str:
                tickets_skipped += 1
                logger.debug(f"⚠️ Skipping ticket with no seat numbers: section='{section_name}', row='{row}'")
                continue
            
            # Use section name mapping to get proper IDs (like reference)
            section_ids = section_name_to_ids.get(section_name, {})
            section_id = section_ids.get("section_id", "")
            level_id = section_ids.get("level_id", "")
            
            # Log section mapping issues
            if not section_ids and tickets_processed <= 5:
                logger.warning(f"⚠️ No section mapping found for section_name: '{section_name}'")
                logger.info(f"Available section mappings: {list(section_name_to_ids.keys())[:10]}")
            
            # Parse price
            price = None
            try:
                if price_str:
                    price = Decimal(str(price_str))
            except (ValueError, TypeError):
                pass
            
            # Find corresponding objects (but don't skip if missing - like reference)
            section = section_lookup.get(level_id)
            zone = zone_lookup.get(pricing_zone) if pricing_zone else None
            
            # Use level_id as zone_id if we have section mapping (like reference)
            zone_id_to_use = level_id if level_id else pricing_zone
            
            # Process individual seat numbers (split like reference)
            seat_numbers = seat_numbers_str.split(',')
            seat_count_for_ticket = len([s.strip() for s in seat_numbers if s.strip()])
            total_seat_numbers_found += seat_count_for_ticket
            
            if tickets_processed <= 5:
                logger.info(f"🪑 Processing {seat_count_for_ticket} seats from ticket {tickets_processed}: {[s.strip() for s in seat_numbers if s.strip()]}")
            
            for seat_num in seat_numbers:
                seat_num = seat_num.strip()
                if not seat_num:
                    continue
                
                # Generate unique seat ID
                seat_id = f"{level_id}_{row}_{seat_num}" if level_id else f"{section_name}_{row}_{seat_num}"
                
                # Build accessibility features list
                accessibility_features = []
                if accessibility:
                    accessibility_features.append("wheelchair_accessible")
                
                seat = SeatData(
                    seat_id=seat_id,
                    section_id=section.section_id if section else f"section_{level_id}",
                    zone_id=zone_id_to_use,
                    source_website=self.config.get('source_website', "vividseats"),
                    row_label=row,
                    seat_number=seat_num,
                    row=row,
                    number=seat_num,
                    x_coord=None,
                    y_coord=None,
                    status="available",
                    price=price,
                    level_id=level_id,
                    # accessibility_features=accessibility_features,
                    # miscellaneous={
                    #     'pack_size': pack_size,
                    #     'view_type': view_type,
                    #     'badges': badges,
                    #     'visibility_type': visibility_type,
                    #     'companion_seats': companion_seats,
                    #     'pricing_zone': pricing_zone
                    # }
                )
                seats.append(seat)
        
        # Summary logging
        logger.info(f"🎯 Seat processing summary:")
        logger.info(f"   - Tickets received: {len(tickets)}")
        logger.info(f"   - Tickets processed: {tickets_processed}")
        logger.info(f"   - Tickets skipped: {tickets_skipped}")
        logger.info(f"   - Total seat numbers found: {total_seat_numbers_found}")
        logger.info(f"   - SeatData objects created: {len(seats)}")
        
        if len(seats) != total_seat_numbers_found:
            logger.warning(f"⚠️ MISMATCH: Expected {total_seat_numbers_found} seats but created {len(seats)}")
        
        return seats

    def _process_seat_packs(self, seats: List[SeatData], sections: List[SectionData], performance_info: PerformanceData) -> List[SeatPackData]:
        """Process seat packs using the generic seat pack generator"""
        venue_prefix_map = {
            self.config.get('source_website', "vividseats"): self.config.get('venue_prefix', "vs")
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
                source_website=self.config.get('source_website', "vividseats")
            ).first()
            
            return venue
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not fetch venue from database: {e}")
            return None