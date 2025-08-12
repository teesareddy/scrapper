"""
VividSeats specific data types
Based on the VividSeats API response structure
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class LoyaltyPartnership:
    loyaltyCurrency: Optional[Any]


@dataclass
class Global:
    listingCount: str
    ticketCount: str
    venueCapacity: str
    vpcr: str
    mapTitle: str
    pageColor: str
    productionId: str
    productionName: str
    venueId: str
    venueCountry: str
    zoned: str
    staticMapUrl: str
    patternSize: str
    zoomFactor: str
    zoomXoff: str
    zoomYoff: str
    svgFileName: str
    jsonFileName: str
    venueAddress1: str
    venueAddress2: str
    venueState: str
    venueTimeZone: str
    showServiceCharge: str
    mapboxLayoutUrl: str
    layoutId: str
    primaryTicketProvider: str
    priceLocalized: bool
    dte: str
    atp: str
    lp: str
    hp: str
    mp: str
    hq: str
    eventId: str
    eventMobileHeroImage: str
    psd: str
    map_src: str
    flashActive: str
    jsonActive: str
    showAip: str
    vatAddedToAip: str
    showAipIncludedPrices: str
    aipIncludedPrices: Optional[Any]
    defaultAipOn: str
    isFaceValue: str
    countryCode: str
    productionCategory: str
    isPriceLocalized: bool
    percentRemaining: int
    localPrices: Optional[Any]
    loyaltyPartnership: LoyaltyPartnership
    hasGreatDeal: bool


@dataclass
class Badge:
    category: str
    title: str


@dataclass
class Ticket:
    s: str  # Section
    r: str  # Row
    q: str  # Quantity
    p: str  # Price
    i: str  # ID
    d: str  # Description
    n: str  # Name
    f: str  # Format
    l: str  # Level/Section name
    g: str  # Group/Level ID
    t: str  # Type
    m: str  # Seat numbers (comma separated)
    c: str  # Category
    z: str  # Zone ID
    zo: str  # Zone order
    ind: str  # Individual seats indicator
    instantElectronicTransfer: str
    instantFlashSeats: str
    st: str  # Seat type
    vs: Optional[str]  # Visibility status
    sd: str  # Seat description
    stp: str  # Seat type description
    ls: str  # Low seat
    hs: str  # High seat
    di: bool  # Disability accessible
    fdi: str  # Full disability info
    pdi: str  # Partial disability info
    badges: List[Badge]
    localPrices: Optional[Any]


@dataclass
class Group:
    productionId: int
    i: str  # Group ID
    n: str  # Group name
    c: str  # Category
    a: str  # Available
    t: str  # Type
    h: str  # High price
    l: str  # Low price
    q: str  # Quantity
    z: str  # Zone
    zd: str  # Zone description
    g: str  # Group
    localPrices: Optional[Any]


@dataclass
class Section:
    productionId: int
    i: str  # Section ID
    g: str  # Group ID
    a: str  # Available
    h: str  # High price
    l: str  # Low price
    q: str  # Quantity
    n: str  # Section name
    si: str  # Section info
    li: str  # Level info
    pi: str  # Price info
    mbi: str  # Minimum buy info
    s3d: str  # 3D section
    p3d: str  # 3D position
    rd: str  # Row data
    localPrices: Optional[Any]


@dataclass
class I18n:
    from_name: str
    to: str
    rate: int
    applyCurrencyConversion: bool


@dataclass
class ListingsResponse:
    global_data: List[Global]
    tickets: List[Ticket]
    groups: List[Group]
    sections: List[Section]
    i18n: I18n


@dataclass
class Geo:
    permalink: str
    title: str
    name: str
    subtitle: str
    latitude: float
    longitude: float
    radius: int
    content: str
    url: str
    meta: dict
    parentsBreadcrumbs: List[dict]
    geoType: str
    geoTypeLevel: int


@dataclass
class Venue:
    id: int
    name: str
    address1: str
    address2: str
    city: str
    state: str
    postalCode: str
    countryCode: str
    timezone: str
    webPath: str
    latitude: float
    longitude: float
    regionId: int
    geo: Geo


@dataclass
class FormattedDate:
    date: str
    time: str


@dataclass
class Asset:
    id: int
    type: str
    images: List
    relatedResources: List[dict]


@dataclass
class Performer:
    id: int
    name: str
    category: dict
    master: bool
    parkingId: int
    webPath: str
    exclusiveWsUserId: int
    revenueRank: int
    allTimeRevenueRank: int
    priority: int
    urlName: str
    productionCount: int


@dataclass
class ProductionDetails:
    id: int
    name: str
    venue: Venue
    listingCount: int
    ticketCount: int
    formattedDate: FormattedDate
    utcDate: str
    localDate: str
    webPath: str
    minPrice: int
    maxPrice: int
    minAipPrice: float
    maxAipPrice: float
    avgPrice: float
    medianPrice: int
    categoryId: int
    hidden: bool
    seatRestrictions: Optional[Any]
    assets: List[Asset]
    performers: List[Performer]
    singleProduction: bool
    productionsCount: int
    article: str
    i18n: I18n
    localPrices: Optional[Any]
    isDoaActive: bool
    isOssEnabled: bool


@dataclass
class VividSeatsEventData:
    listings_data: Optional[ListingsResponse]
    production_details: Optional[ProductionDetails]
    event_info: Dict[str, str]