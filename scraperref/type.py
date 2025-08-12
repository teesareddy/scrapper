from dataclasses import dataclass
from typing import List, Optional, Any

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
    s: str
    r: str
    q: str
    p: str
    i: str
    d: str
    n: str
    f: str
    l: str
    g: str
    t: str
    m: str
    c: str
    z: str
    zo: str
    ind: str
    instantElectronicTransfer: str
    instantFlashSeats: str
    st: str
    vs: Optional[str]
    sd: str
    stp: str
    ls: str
    hs: str
    di: bool
    fdi: str
    pdi: str
    badges: List[Badge]
    localPrices: Optional[Any]

@dataclass
class Group:
    productionId: int
    i: str
    n: str
    c: str
    a: str
    t: str
    h: str
    l: str
    q: str
    z: str
    zd: str
    g: str
    localPrices: Optional[Any]

@dataclass
class Section:
    productionId: int
    i: str
    g: str
    a: str
    h: str
    l: str
    q: str
    n: str
    si: str
    li: str
    pi: str
    mbi: str
    s3d: str
    p3d: str
    rd: str
    localPrices: Optional[Any]

@dataclass
class I18n:
    from_name: str
    to: str
    rate: int
    applyCurrencyConversion: bool

@dataclass
class Listings:
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