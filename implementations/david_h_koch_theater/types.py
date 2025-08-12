"""
David H Koch Theater specific data types
Based on the comprehensive TypeScript interfaces from data.txt
Follows the exact structure from the working scraper
"""
from typing import List, Dict, Any, Optional, Union
from dataclasses import dataclass


@dataclass
class PerformanceInfo:
    """Performance information from GetPerformanceDetailWithDiscountingEx"""
    inv_no: str
    description: str
    text1: str
    text2: str  
    text3: str
    perf_dt: str
    facility_desc: str
    on_sale_ind: str
    performance_type: str
    perf_type_id: str
    seat_ind: str
    print_ind: str
    zmap_no: str
    facility_no: str
    prod_season_no: str
    season_no: str
    season_desc: str
    start_dt: str
    end_dt: str
    perf_status: str
    perf_status_desc: str
    time_slot_desc: str
    ga_ind: str


@dataclass
class PriceZone:
    """Price zone information"""
    perf_no: str
    zone_no: str
    price_type: str
    price: str
    base_price: str
    description: str
    available: str
    avail_count: Optional[str] = None
    zone_group: str = ""
    zone_group_desc: str = ""


@dataclass
class PriceTypeInfo:
    """Price type information"""
    price_type: str
    description: str
    short_desc: str
    category: str
    def_price_type: str
    promo: str


@dataclass
class DetailedPriceZone:
    """Detailed price zone with additional fees"""
    perf_no: str
    zone_no: str
    price_type: str
    price: str
    base_price: str
    description: str
    available: str
    avail_count: Optional[str]
    zone_group: str
    zone_group_desc: str
    def_price_type: str
    price_type_desc: str
    rank: str
    facility_fee: Union[int, float]
    service_fee: Union[int, float]


@dataclass
class PerformanceDetailResult:
    """Result structure from performance detail endpoint"""
    Performance: PerformanceInfo
    Price: List[PriceZone] 
    PriceType: PriceTypeInfo
    AllPrice: List[DetailedPriceZone]


@dataclass
class PerformanceDetailResponse:
    """Complete response from GetPerformanceDetailWithDiscountingEx"""
    id: Optional[Any]
    result: Dict[str, PerformanceDetailResult]
    error: Optional[Any]


@dataclass
class ParsedSeatData:
    """Parsed seat data from the D string format"""
    sectionId: str
    row: str
    seatNumber: str
    statusId: str
    seatId: str
    zoneNo: str
    allocationNo: str
    seatTypeId: str
    xCoord: Union[int, float]
    screenId: str
    yCoord: Union[int, float]
    rowId: str
    forSale: str
    isHandicap: str
    hasObstructedView: str
    companionSeatId: Optional[str] = None
    gaSection: str = ""
    gaSubSection: Optional[str] = None


@dataclass
class SeatData:
    """Raw seat data container"""
    D: str


@dataclass
class SectionInfo:
    """Section information for lookups"""
    section: str
    section_desc: str


@dataclass
class SeatTypeInfo:
    """Seat type information for lookups"""
    seat_type: str
    seat_type_desc: str


@dataclass
class AllocationInfo:
    """Allocation information"""
    ac_no: str
    alloc_desc: str


@dataclass
class SeatBriefResult:
    """Result structure from seat brief endpoint"""
    S: List[SeatData]
    Section: List[SectionInfo]
    SeatType: List[SeatTypeInfo]
    Allocation: AllocationInfo


@dataclass
class SeatBriefResponse:
    """Complete response from GetSeatsBriefWithMOS"""
    id: Optional[Any]
    result: Dict[str, SeatBriefResult]
    error: Optional[Any]


@dataclass
class StatusInfo:
    """Seat status information"""
    id: str
    status_code: str
    description: str
    fore_color: str
    back_color: str
    status_priority: str
    status_legend: str


@dataclass
class SeatStatusResponse:
    """Complete response from GetSeatStatus"""
    id: Optional[Any]
    result: Dict[str, Dict[str, List[StatusInfo]]]
    error: Optional[Any]


# Unified data structure for processed data
@dataclass
class ProcessedVenueInfo:
    """Processed venue information"""
    raw_venue_id: Optional[str] = None
    raw_venue_url: Optional[str] = None 
    raw_venue_name: Optional[str] = None
    raw_venue_address: Optional[str] = None
    raw_venue_timezone: Optional[str] = None
    raw_venue_features: Optional[List[str]] = None


@dataclass
class ProcessedEventInfo:
    """Processed event information"""
    raw_event_id: Optional[str] = None
    raw_event_url: Optional[str] = None
    raw_event_name: Optional[str] = None
    raw_performance_datetime_text: Optional[str] = None
    raw_event_features: Optional[List[str]] = None


@dataclass
class ProcessedPricingInfo:
    """Processed pricing information"""
    raw_currency: str = "$"
    raw_price_range_text: Optional[str] = None


@dataclass
class ProcessedSeatMapInfo:
    """Processed seat map information"""
    seat_map_image_url: Optional[str] = None
    map_dimensions: Optional[Dict[str, Union[int, float]]] = None


@dataclass
class ProcessedLevel:
    """Processed level/section information"""
    level_id: str
    raw_level_name: str
    raw_level_price_range_text: Optional[str] = None
    raw_level_availability_text: Optional[str] = None
    raw_level_features: Optional[List[str]] = None
    map_bounds: Optional[Dict[str, Any]] = None


@dataclass  
class ProcessedSeat:
    """Processed individual seat information"""
    seat_id: str
    level_id: str
    section_id: str
    row_id: str
    raw_seat_label: str
    raw_seat_status: str
    raw_price_text: Optional[str] = None
    map_position: Optional[Dict[str, Union[int, float]]] = None