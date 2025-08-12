"""
Broadway SF specific data types
Based on the interfaces defined in data/docs.txt
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class CalendarServiceResponse:
    """Response from Calendar Service API"""
    data: Dict[str, Any]


@dataclass 
class Show:
    """Show information from Calendar Service"""
    images: Dict[str, str]
    isEmbargoed: bool
    status: str
    performanceMode: str
    externalPurchaseLink: str
    dates: Dict[str, str]
    performances: List[Dict[str, Any]]


@dataclass
class Performance:
    """Performance information from Calendar Service"""
    id: str
    dates: Dict[str, str]
    performanceTimeDescription: str
    availabilityStatus: str
    isAccessiblePerformance: bool
    accessibilityType: Optional[str]
    purchaseFlowType: str
    ruleKey: Optional[str]
    promoCode: Dict[str, Any]
    price: Dict[str, float]
    sectionAvailability: List[Any]


@dataclass
class BoltApiResponse:
    """Response from Bolt Seating API"""
    seats: List[Dict[str, Any]]
    zones: Dict[str, Dict[str, Any]]
    tickets: Dict[str, Dict[str, Any]]
    promoCodes: List[Any]
    purchaseFlowType: str
    performance: Dict[str, Any]
    upsells: List[Dict[str, Any]]
    labels: Dict[str, Dict[str, Any]]
    shapes: Dict[str, Dict[str, Any]]
    sections: List[Dict[str, Any]]
    legends: List[Dict[str, Any]]
    cookieUpdated: bool


@dataclass
class Seat:
    """Individual seat information from Bolt API"""
    id: str
    available: bool
    x: int
    y: int
    level: str
    row: str
    number: str
    promoCodes: List[Any]
    benefitIds: List[Any]
    zone: str
    tags: List[str]
    soldOut: Optional[bool]
    label: str
    section: str
    holdId: str
    info: Optional[str]


@dataclass
class Zone:
    """Zone pricing information from Bolt API"""
    id: str
    available: bool
    defaultTicket: str
    tickets: Dict[str, Dict[str, Any]]
    promoCodes: List[Any]
    benefitIds: List[Any]
    tags: List[str]


@dataclass
class TicketDetails:
    """Ticket type information from Bolt API"""
    id: str
    whitelisted: bool
    available: bool
    name: str
    description: str
    group: str
    label: str
    promoCodes: List[Any]
    benefitIds: List[Any]
    tags: List[str]
    mapSymbol: str


@dataclass
class Section:
    """Section information from Bolt API"""
    id: str
    entrance: str
    entryTime: int
    message: str
    reservedCapacity: int
    name: str


@dataclass
class PerformanceInfo:
    """Performance details from Bolt API"""
    title: str
    venue: str
    avVenueId: str
    transactionFee: int
    groupBookingSeatCap: int
    benefitsAvailable: Dict[str, bool]
    accessibility: str
    date: str
    dateTimeISO: str


@dataclass
class BroadwaySFEventData:
    """Combined event data for Broadway SF"""
    event_info: Dict[str, str]
    calendar_data: Optional[CalendarServiceResponse]
    seating_data: Optional[BoltApiResponse]