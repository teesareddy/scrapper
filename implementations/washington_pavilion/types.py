"""
Washington Pavilion specific data types
Each scraper implementation will have its own types based on the venue's API structure
"""
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class LocalizedName:
    en: str


@dataclass
class BlockInfo:
    id: int
    name: LocalizedName


@dataclass
class AvailabilityDetails:
    availability: int
    availabilityResale: int


@dataclass
class PriceRangeCategory:
    id: int
    name: LocalizedName
    rank: int
    bgColor: str
    textColor: str
    sameBlockRestrictionMode: str
    minPrice: int
    maxPrice: int
    blocks: List[BlockInfo]
    areas: List[Any]
    areaBlocksAvailability: Dict[str, AvailabilityDetails]


@dataclass
class EventPricingInfo:
    """Washington Pavilion pricing data structure - seatmap/availability"""
    priceRangeCategories: List[PriceRangeCategory]
    addedSeats: List[Any]
    productType: str
    priceFilters: Dict[str, Any]


@dataclass
class Geometry:
    coordinates: List[float]
    rotation: float
    type: str


@dataclass
class AreaInfo:
    id: int
    name: LocalizedName


@dataclass
class SeatProperties:
    id: int
    block: BlockInfo
    area: AreaInfo
    color: str
    row: str
    number: str
    seatCategoryId: int
    seatCategory: str
    audienceSubCategoryId: int


@dataclass
class SeatFeature:
    id: int
    geometry: Geometry
    properties: SeatProperties


@dataclass
class SeatFeaturesInfo:
    """Washington Pavilion seat data structure - seatmap/seats/free/ol"""
    features: List[SeatFeature]
