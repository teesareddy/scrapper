"""
TPAC specific data types
Based on the TPAC API response structure from scraperref/tpac.py
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class TPACPerformanceDetails:
    """TPAC Performance details from GetPerformanceDetails endpoint"""
    facility_no: str
    description: str
    perf_dt: str
    facility_desc: str


@dataclass
class TPACScreen:
    """TPAC Screen data from GetScreens endpoint"""
    screen_no: str
    screen_desc: str


@dataclass
class TPACAvailablePrice:
    """TPAC Available price information"""
    ZoneNo: str
    Price: float
    ZoneDesc: Optional[str] = None


@dataclass
class TPACSeat:
    """TPAC Individual seat data"""
    seat_status_desc: str
    zone_no: str
    seat_row: str
    seat_num: str
    accessible_ind: bool = False


@dataclass
class TPACSeatListResponse:
    """TPAC Seat list response from GetSeatList endpoint"""
    AvailablePrices: List[TPACAvailablePrice]
    seats: List[TPACSeat]
    DefaultPrice: Optional[str] = None


@dataclass
class TPACEventData:
    """Complete TPAC event data structure"""
    performance_details: Optional[TPACPerformanceDetails]
    screens: Optional[List[TPACScreen]]
    seat_lists: Optional[Dict[str, TPACSeatListResponse]]  # screen_id -> seat_list
    base_url: str
    performance_id: str