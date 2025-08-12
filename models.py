# scrapers/models.py
"""
Modular Django models for multi-website event scraper.
This file imports all models from the modular structure for backward compatibility.
"""

# Import all models from the modular structure
from .models.base import *
from .models.scraper_config import *
from .models.proxy import *
from .models.monitoring import *
from .models.snapshots import *
from .models.legacy import *

# Import custom managers
from .models.managers import *

# For backward compatibility, maintain the original model imports
__all__ = [
    # Core models
    'Venue', 'Event', 'EventVenue', 'Performance', 'Level', 'Zone', 'Section', 'Seat', 'SeatPack',
    
    # Scraper configuration models
    'CaptchaType', 'OptimizationRule', 'ScraperDefinition', 'ScraperOptimizationSettings',
    'ScraperExecution', 'ScraperSchedule',
    
    # Proxy models
    'ProxyProvider', 'ProxyConfiguration', 'ScraperProxyAssignment', 'ProxyUsageLog',
    
    # Monitoring models
    'ScrapeJob', 'ScraperStatus', 'ResourceMonitor', 'ScraperMetrics',
    
    # Snapshot models
    'SeatSnapshot', 'LevelPriceSnapshot', 'ZonePriceSnapshot', 'SectionPriceSnapshot',
    
    # Legacy models (for backward compatibility)
    'ProxySetting', 'ScraperConfiguration',
    
    # Custom managers
    'VenueManager', 'EventManager', 'PerformanceManager', 'SeatManager',
    'ScraperDefinitionManager', 'ScrapeJobManager', 'ProxyConfigurationManager',
]