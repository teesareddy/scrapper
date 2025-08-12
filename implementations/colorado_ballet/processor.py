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
    ScraperConfigData
)
from ...core.seat_pack_generator import generate_seat_packs, detect_venue_seat_structure
from ...models import Venue

def get_venue_seat_structure(source_venue_id: str, source_website: str) -> str:
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


class ColoradoBalletProcessor:

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}

    def process(self, raw_data: Dict[str, Any], url: str, 
                scrape_job_id: Optional[str] = None, enriched_data: Dict[str, Any] = None) -> ScrapedData:
        
        event_details = raw_data.get("event_details", {})
        init_data = raw_data.get("init_data", {})
        seats_data = raw_data.get("seats", [])

        venue_info = self._process_venue_info(event_details)
        event_info = self._process_event_info(event_details, venue_info.source_venue_id, url)
        performance_info = self._process_performance_info(event_details, event_info.source_event_id, venue_info.source_venue_id, url)
        
        zones = self._process_zones(init_data.get("Pricing", []))
        levels = self._process_levels(init_data.get("Screens", []))
        sections = self._process_sections(levels)
        seats = self._process_seats(seats_data, zones, levels, sections)

        venue_seat_structure = get_venue_seat_structure(venue_info.source_venue_id, "colorado_ballet")
        if not venue_seat_structure:
            venue_seat_structure = detect_venue_seat_structure(seats)

        seat_packs = generate_seat_packs(
            seats, 
            venue_info, 
            event_info, 
            performance_info, 
            seat_structure=venue_seat_structure
        )

        return ScrapedData(
            venue_info=venue_info,
            event_info=event_info,
            performance_info=performance_info,
            levels=levels,
            zones=zones,
            sections=sections,
            seats=seats,
            seat_packs=seat_packs,
            source_website="colorado_ballet",
            scraped_at=datetime.now(),
            scraper_config=ScraperConfigData(
                scraper_name="colorado_ballet_scraper_v1",
                optimization_enabled=True,
                optimization_level="balanced"
            ),
            url=url
        )

    def _process_venue_info(self, event_details: Dict[str, Any]) -> VenueData:
        venue_name = event_details.get("venue", "Ellie Caulkins Opera House")
        
        source_venue_id = hashlib.md5(venue_name.encode()).hexdigest()[:16]
        
        return VenueData(
            name=venue_name,
            source_venue_id=source_venue_id,
            source_website="colorado_ballet",
            address="1385 Curtis Street",
            city="Denver",
            state="CO",
            country="US",
            venue_timezone="America/Denver"
        )

    def _process_event_info(self, event_details: Dict[str, Any], source_venue_id: str, url: str) -> EventData:
        title = event_details.get("title", "Colorado Ballet Event")
        
        source_event_id = hashlib.md5(f"{title}_{source_venue_id}".encode()).hexdigest()[:16]
        
        return EventData(
            name=title,
            source_event_id=source_event_id,
            source_website="colorado_ballet",
            description=f"Colorado Ballet performance: {title}",
            category="Ballet",
            source_venue_id=source_venue_id,
            url=url
        )

    def _process_performance_info(self, event_details: Dict[str, Any], source_event_id: str, source_venue_id: str, url: str) -> PerformanceData:
        date_str = event_details.get("date", "")
        
        performance_date = None
        if date_str and date_str != "Unknown":
            try:
                performance_date = parser.parse(date_str)
            except Exception:
                performance_date = datetime.now()
        else:
            performance_date = datetime.now()
        
        performance_id = url.split("/")[-1] if "/" in url else hashlib.md5(f"{source_event_id}_{performance_date.isoformat()}".encode()).hexdigest()[:16]
        
        return PerformanceData(
            source_performance_id=performance_id,
            source_website="colorado_ballet",
            source_event_id=source_event_id,
            source_venue_id=source_venue_id,
            performance_date=performance_date,
            performance_time=performance_date.time() if performance_date else None,
            status="available",
            url=url
        )

    def _process_zones(self, pricing_data: List[Dict[str, Any]]) -> List[ZoneData]:
        zones = []
        for idx, zone_item in enumerate(pricing_data):
            zone_id = str(zone_item.get("zone_no", idx))
            price = Decimal(str(zone_item.get("price", 0)))
            
            zones.append(ZoneData(
                zone_id=zone_id,
                name=zone_item.get("description", f"Zone {zone_id}"),
                source_website="colorado_ballet",
                min_price=price,
                max_price=price,
                price_category=zone_item.get("price_type_desc", "Standard")
            ))
        
        return zones

    def _process_levels(self, screens_data: List[Dict[str, Any]]) -> List[LevelData]:
        levels = []
        for screen in screens_data:
            screen_id = str(screen.get("ScreenId"))
            level_name = screen.get("ScreenDescription", f"Level {screen_id}")
            
            levels.append(LevelData(
                level_id=screen_id,
                name=level_name,
                source_website="colorado_ballet",
                level_type="seating",
                display_order=int(screen_id) if screen_id.isdigit() else 0
            ))
        
        return levels

    def _process_sections(self, levels: List[LevelData]) -> List[SectionData]:
        sections = []
        for level in levels:
            sections.append(SectionData(
                section_id=f"section_{level.level_id}",
                name=f"{level.name} Section",
                source_website="colorado_ballet",
                level_id=level.level_id,
                section_type="general"
            ))
        
        return sections

    def _process_seats(self, seats_data: List[Dict[str, Any]], zones: List[ZoneData], 
                      levels: List[LevelData], sections: List[SectionData]) -> List[SeatData]:
        seats = []
        zone_map = {zone.zone_id: zone for zone in zones}
        level_map = {level.name: level for level in levels}
        section_map = {section.level_id: section for section in sections}
        
        for seat_info in seats_data:
            level_name = seat_info.get("Level", "")
            row = seat_info.get("Row", "")
            seat_num = seat_info.get("Seat", "")
            zone_no = str(seat_info.get("zone_no", ""))
            
            level = level_map.get(level_name)
            if not level:
                continue
                
            section = section_map.get(level.level_id)
            zone = zone_map.get(zone_no)
            
            price_str = seat_info.get("Price", "$0.00")
            try:
                price = Decimal(price_str.replace("$", "").replace(",", ""))
            except:
                price = Decimal("0.00")
            
            seat_id = f"{level.level_id}_{row}_{seat_num}"
            
            seats.append(SeatData(
                seat_id=seat_id,
                source_website="colorado_ballet",
                level_id=level.level_id,
                section_id=section.section_id if section else f"section_{level.level_id}",
                zone_id=zone.zone_id if zone else zone_no,
                row=row,
                seat_number=seat_num,
                price=price,
                availability_status="available",
                seat_type="regular"
            ))
        
        return seats