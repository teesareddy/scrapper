# scrapers/models/managers.py
"""
Custom model managers for optimized queries and common operations.
Following Django best practices for query optimization and performance.
"""

from django.db import models
from django.utils import timezone
from datetime import timedelta


class VenueManager(models.Manager):
    """Custom manager for Venue model with optimized queries"""
    
    def active(self):
        """Get only active venues"""
        return self.filter(is_active=True)
    
    def by_city(self, city):
        """Get venues by city"""
        return self.filter(city__iexact=city, is_active=True)
    
    def by_state(self, state):
        """Get venues by state"""
        return self.filter(state__iexact=state, is_active=True)
    
    def with_events(self):
        """Get venues that have events"""
        return self.filter(events__isnull=False, is_active=True).distinct()
    
    def search(self, query):
        """Search venues by name or city"""
        return self.filter(
            models.Q(name__icontains=query) | 
            models.Q(city__icontains=query),
            is_active=True
        )


class EventManager(models.Manager):
    """Custom manager for Event model with optimized queries"""
    
    def active(self):
        """Get only active events"""
        return self.filter(is_active=True)
    
    def with_venues(self):
        """Get events with their venues prefetched"""
        return self.prefetch_related('venues').filter(is_active=True)
    
    def by_venue(self, venue):
        """Get events at a specific venue"""
        return self.filter(venues=venue, is_active=True)
    
    def by_event_type(self, event_type):
        """Get events by type"""
        return self.filter(event_type__iexact=event_type, is_active=True)
    
    def search(self, query):
        """Search events by name"""
        return self.filter(name__icontains=query, is_active=True)
    
    def upcoming_performances(self):
        """Get events with upcoming performances"""
        return self.filter(
            performances__performance_datetime_utc__gte=timezone.now(),
            performances__is_active=True,
            is_active=True
        ).distinct()


class PerformanceManager(models.Manager):
    """Custom manager for Performance model with optimized queries"""
    
    def active(self):
        """Get only active performances"""
        return self.filter(is_active=True)
    
    def upcoming(self):
        """Get upcoming performances"""
        return self.filter(
            performance_datetime_utc__gte=timezone.now(),
            is_active=True
        ).order_by('performance_datetime_utc')
    
    def past(self):
        """Get past performances"""
        return self.filter(
            performance_datetime_utc__lt=timezone.now(),
            is_active=True
        ).order_by('-performance_datetime_utc')
    
    def today(self):
        """Get today's performances"""
        today = timezone.now().date()
        return self.filter(
            performance_datetime_utc__date=today,
            is_active=True
        ).order_by('performance_datetime_utc')
    
    def this_week(self):
        """Get this week's performances"""
        now = timezone.now()
        week_start = now - timedelta(days=now.weekday())
        week_end = week_start + timedelta(days=7)
        return self.filter(
            performance_datetime_utc__range=[week_start, week_end],
            is_active=True
        ).order_by('performance_datetime_utc')
    
    def by_venue(self, venue):
        """Get performances at a specific venue"""
        return self.filter(venue_id=venue, is_active=True)
    
    def by_event(self, event):
        """Get performances for a specific event"""
        return self.filter(event_id=event, is_active=True)
    
    def with_availability(self):
        """Get performances with seat availability data"""
        return self.prefetch_related(
            'levels__sections__seats',
            'zones__seats'
        ).filter(is_active=True)


class SeatManager(models.Manager):
    """Custom manager for Seat model with optimized queries"""
    
    def available(self):
        """Get available seats"""
        return self.filter(current_status='available', is_active=True)
    
    def sold(self):
        """Get sold seats"""
        return self.filter(current_status='sold', is_active=True)
    
    def by_performance(self, performance):
        """Get seats for a specific performance"""
        return self.filter(
            section_id__level_id__performance_id=performance,
            is_active=True
        )
    
    def by_zone(self, zone):
        """Get seats in a specific zone"""
        return self.filter(zone_id=zone, is_active=True)
    
    def by_section(self, section):
        """Get seats in a specific section"""
        return self.filter(section_id=section, is_active=True)
    
    def by_price_range(self, min_price=None, max_price=None):
        """Get seats within a price range"""
        queryset = self.filter(is_active=True, current_price__isnull=False)
        if min_price is not None:
            queryset = queryset.filter(current_price__gte=min_price)
        if max_price is not None:
            queryset = queryset.filter(current_price__lte=max_price)
        return queryset
    
    def cheapest(self, limit=10):
        """Get cheapest available seats"""
        return self.filter(
            current_status='available',
            current_price__isnull=False,
            is_active=True
        ).order_by('current_price')[:limit]
    
    def best_available(self, limit=10):
        """Get best available seats (premium zones, good view)"""
        return self.filter(
            current_status='available',
            is_active=True
        ).select_related('zone_id').filter(
            zone_id__zone_type__in=['premium', 'vip'],
            zone_id__view_type__in=['excellent', 'clear', 'premium']
        ).order_by('zone_id__display_order', 'current_price')[:limit]


class ScraperDefinitionManager(models.Manager):
    """Custom manager for ScraperDefinition model"""
    
    def active(self):
        """Get active scrapers"""
        return self.filter(is_enabled=True, status='active')
    
    def enabled(self):
        """Get enabled scrapers"""
        return self.filter(is_enabled=True)
    
    def by_priority(self, priority):
        """Get scrapers by priority level"""
        return self.filter(priority=priority, is_enabled=True)
    
    def high_priority(self):
        """Get high and critical priority scrapers"""
        return self.filter(
            priority__in=['high', 'critical'],
            is_enabled=True
        ).order_by('priority')
    
    def with_proxy(self):
        """Get scrapers that use proxies"""
        return self.filter(use_proxy=True, is_enabled=True)
    
    def by_browser_engine(self, engine):
        """Get scrapers using specific browser engine"""
        return self.filter(browser_engine=engine, is_enabled=True)
    
    def schedulable(self):
        """Get scrapers that can be scheduled"""
        return self.filter(can_be_scheduled=True, is_enabled=True)
    
    def with_optimization_rules(self):
        """Get scrapers with optimization rules"""
        return self.prefetch_related('optimization_rules').filter(
            optimization_enabled=True,
            is_enabled=True
        )


class ScrapeJobManager(models.Manager):
    """Custom manager for ScrapeJob model"""
    
    def successful(self):
        """Get successful scrape jobs"""
        return self.filter(scrape_success=True)
    
    def failed(self):
        """Get failed scrape jobs"""
        return self.filter(scrape_success=False)
    
    def recent(self, hours=24):
        """Get recent scrape jobs"""
        since = timezone.now() - timedelta(hours=hours)
        return self.filter(scraped_at_utc__gte=since)
    
    def by_scraper(self, scraper_name):
        """Get jobs for a specific scraper"""
        return self.filter(scraper_name=scraper_name)
    
    def by_website(self, website):
        """Get jobs for a specific website"""
        return self.filter(source_website=website)
    
    def with_performance_data(self):
        """Get jobs with related performance data"""
        return self.select_related('performance_id__event_id', 'performance_id__venue_id')


class ProxyConfigurationManager(models.Manager):
    """Custom manager for ProxyConfiguration model"""
    
    def active(self):
        """Get active proxy configurations"""
        return self.filter(is_active=True, status='active')
    
    def healthy(self):
        """Get healthy proxy configurations"""
        return self.filter(
            is_active=True,
            status='active',
            consecutive_failures__lt=5,
            success_rate__gte=70
        )
    
    def by_type(self, proxy_type):
        """Get proxies by type"""
        return self.filter(proxy_type=proxy_type, is_active=True)
    
    def by_provider(self, provider):
        """Get proxies by provider"""
        return self.filter(provider=provider, is_active=True)
    
    def by_country(self, country_code):
        """Get proxies by country"""
        return self.filter(country_code=country_code, is_active=True)
    
    def best_performing(self, limit=10):
        """Get best performing proxies"""
        return self.filter(
            is_active=True,
            status='active',
            success_rate__gte=80
        ).order_by('-success_rate', 'response_time_ms')[:limit]
    
    def available_for_scraper(self, scraper_name):
        """Get proxies available for a specific scraper"""
        return self.filter(
            is_active=True,
            status='active',
            current_connections__lt=models.F('max_concurrent_connections')
        ).exclude(
            scraper_assignments__scraper_name=scraper_name,
            scraper_assignments__is_active=False
        )


# Add managers to models (this would typically be done in the model definitions)
"""
Usage in models:

class Venue(models.Model):
    # ... fields ...
    objects = VenueManager()

class Event(models.Model):
    # ... fields ...
    objects = EventManager()

# etc.
"""