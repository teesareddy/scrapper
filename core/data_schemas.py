from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass
class ScraperConfigData:
    scraper_name: Optional[str] = None
    proxy_used: Optional[bool] = None
    timeout_seconds: Optional[int] = None
    user_agent: Optional[str] = None
    miscellaneous: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ZoneFeaturesData:
    entry_gate: Optional[str] = None
    concessions_nearby: Optional[bool] = None
    restrooms_nearby: Optional[bool] = None
    merchandise_stand: Optional[bool] = None
    view_description: Optional[str] = None


@dataclass
class VenueData:
    name: str
    source_venue_id: str
    source_website: str
    city: str
    state: str
    country: str = "US"
    address: Optional[str] = None
    postal_code: Optional[str] = None
    venue_timezone: Optional[str] = None
    url: Optional[str] = None
    seat_structure: Optional[str] = None


@dataclass
class EventData:
    name: str
    source_event_id: str
    source_website: str
    url: Optional[str] = None
    currency: str = "USD"
    event_type: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    performance_datetime: Optional[datetime] = None


@dataclass
class PerformanceData:
    source_performance_id: str
    source_website: str
    performance_datetime_utc: datetime
    event_source_id: str
    venue_source_id: str
    seat_map_url: Optional[str] = None
    map_width: Optional[int] = None
    map_height: Optional[int] = None
    performance_url: Optional[str] = None
    pos_enabled: Optional[bool] = None


@dataclass
class LevelData:
    level_id: str
    source_website: str
    name: str
    raw_name: Optional[str] = None
    level_number: Optional[int] = None
    display_order: int = 0
    level_type: Optional[str] = None
    price: Optional[Decimal] = None


@dataclass
class ZoneData:
    zone_id: str
    source_website: str
    name: str
    features: Optional[ZoneFeaturesData] = None
    raw_identifier: Optional[str] = None
    zone_type: Optional[str] = None
    color_code: Optional[str] = None
    view_type: Optional[str] = None
    wheelchair_accessible: bool = False
    display_order: int = 0
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None
    miscellaneous: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SectionData:
    section_id: str
    level_id: str
    source_website: str
    name: str
    raw_name: Optional[str] = None
    section_type: Optional[str] = None
    display_order: int = 0
    numbering_scheme: str = "consecutive"


@dataclass
class SeatData:
    seat_id: str
    section_id: str
    zone_id: str
    source_website: str
    row_label: str
    seat_number: str
    row: Optional[str] = None
    number: Optional[str] = None
    seat_type: Optional[str] = "standard"
    x_coord: Optional[Decimal] = None
    y_coord: Optional[Decimal] = None
    status: str = "available"
    price: Optional[Decimal] = None
    fees: Optional[Decimal] = None
    available: Optional[bool] = True
    level_id: Optional[str] = None


@dataclass
class SeatPackData:
    pack_id: str
    zone_id: str
    source_website: str
    row_label: str
    start_seat_number: str
    end_seat_number: str
    pack_size: int
    pack_price: Optional[Decimal] = None
    total_price: Optional[Decimal] = None
    seat_ids: List[str] = field(default_factory=list)
    row: Optional[str] = None
    start_seat: Optional[str] = None
    end_seat: Optional[str] = None
    performance: Optional[Any] = None
    event: Optional[Any] = None
    level: Optional[Any] = None
    level_id: Optional[str] = None  # Added to store the level identifier



@dataclass
class ScrapedData:
    source_website: str
    scraped_at: datetime
    url: str

    venue_info: VenueData
    event_info: EventData
    performance_info: PerformanceData

    levels: List[LevelData] = field(default_factory=list)
    zones: List[ZoneData] = field(default_factory=list)
    sections: List[SectionData] = field(default_factory=list)
    seats: List[SeatData] = field(default_factory=list)
    seat_packs: List[SeatPackData] = field(default_factory=list)

    scraper_config: Optional[ScraperConfigData] = None
    scraper_version: Optional[str] = None
    raw_data: Dict[str, Any] = field(default_factory=dict)
    success: bool = True
    error_message: Optional[str] = None
    http_status: Optional[int] = None
    
    # New field for strategy-aware seat pack generation
    scraper_instance: Optional[Any] = None

