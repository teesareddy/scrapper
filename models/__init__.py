# scrapers/models/__init__.py
"""
Modular models package for scrapers app.
Following Django best practices for maintainability and performance.
"""

# Core venue/event models
from .base import (
    Venue, Event, EventVenue, Performance, Level, Zone, Section, Seat, PerformanceLevel
)

# Seat pack model with enhanced lifecycle management
from .seat_packs import SeatPack

# POS listing model
from .pos import POSListing

# Scraper configuration models
from .scraper_config import (
    CaptchaType, OptimizationRule, ScraperDefinition, ScraperOptimizationSettings,
    ScraperExecution, ScraperSchedule
)

# Proxy management models
from .proxy import (
    ProxyProvider, ProxyConfiguration, ScraperProxyAssignment, ProxyUsageLog
)

# Monitoring and metrics models
from .monitoring import (
    ScraperStatus, ScraperMetrics, ResourceMonitor, ScrapeJob
)

# Snapshot and historical data models
from .snapshots import (
    SeatSnapshot, LevelPriceSnapshot, ZonePriceSnapshot, SectionPriceSnapshot
)

# Legacy models for backward compatibility (will be deprecated)
from .legacy import (
    ProxySetting, ScraperConfiguration
)

# Make all models available at package level
__all__ = [
    # Base models
    'Venue', 'Event', 'EventVenue', 'Performance', 'Level', 'Zone', 'Section', 'Seat', 'PerformanceLevel', 'SeatPack',

    # POS Listing
    'POSListing',
    
    # Scraper configuration
    'CaptchaType', 'OptimizationRule', 'ScraperDefinition', 'ScraperOptimizationSettings',
    'ScraperExecution', 'ScraperSchedule',
    
    # Proxy management
    'ProxyProvider', 'ProxyConfiguration', 'ScraperProxyAssignment', 'ProxyUsageLog',
    
    # Monitoring
    'ScraperStatus', 'ScraperMetrics', 'ResourceMonitor', 'ScrapeJob',
    
    # Snapshots
    'SeatSnapshot', 'LevelPriceSnapshot', 'ZonePriceSnapshot', 'SectionPriceSnapshot',
    
    # Legacy (deprecated)
    'ProxySetting', 'ScraperConfiguration',
]