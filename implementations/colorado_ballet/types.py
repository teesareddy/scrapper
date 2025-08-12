from dataclasses import dataclass
from typing import Dict, List, Any, Optional


@dataclass
class ColoradoBalletEventDetails:
    title: str
    date: str
    venue: str
    

@dataclass
class ColoradoBalletZoneInfo:
    zone_no: str
    price: float
    description: str
    category: str


@dataclass
class ColoradoBalletSeatInfo:
    level: str
    row: str
    seat: str
    price: str
    category: str
    zone_no: str


@dataclass
class ColoradoBalletInitData:
    pricing: List[Dict[str, Any]]
    screen_zone_list: List[Dict[str, Any]]
    screens: List[Dict[str, Any]]
    facility_id: str


@dataclass
class ColoradoBalletSeatData:
    seats: List[Dict[str, Any]]