# scrapers/admin.py
from django.contrib import admin
from django.contrib.admin import AdminSite
from django.db.models import Count, Q, Avg
from django.utils import timezone
from django.template.response import TemplateResponse
from datetime import datetime, timedelta
import json
from .models import (
    Venue, Event, Performance, Level, Zone, Section, Seat,
    ScrapeJob, SeatSnapshot, LevelPriceSnapshot, ZonePriceSnapshot,
    SectionPriceSnapshot, SeatPack, ScraperStatus,
    ResourceMonitor, ScraperConfiguration, ScraperMetrics,
    ProxyProvider, ProxyConfiguration, ScraperProxyAssignment, ProxyUsageLog,
    ScraperDefinition, ScraperExecution, ScraperSchedule, POSListing
)

# Try to import Unfold admin first
SKIP_MAIN_REGISTRATIONS = False
try:
    from unfold.admin import ModelAdmin, TabularInline
    from .admin_unfold import *
    UNFOLD_ADMINS_LOADED = True
    SKIP_MAIN_REGISTRATIONS = True
    print("✅ Loaded Unfold admin configurations")
except ImportError:
    # Fallback to standard Django admin - comment out to avoid double registration
    # try:
    #     from .admin_fallback import *
    #     print("✅ Loaded fallback admin configurations (standard Django admin)")
    #     SKIP_MAIN_REGISTRATIONS = True  # Fallback admins handle registration
    # except ImportError:
    print("⚠️ Using basic Django admin configurations")
    from django.contrib.admin import ModelAdmin, TabularInline
    UNFOLD_ADMINS_LOADED = False
    SKIP_MAIN_REGISTRATIONS = False

from django.utils.html import format_html
from django.utils.safestring import mark_safe
from django.urls import reverse

# Only register admin classes if not already handled by Unfold or fallback
# The specialized admin configurations (Unfold/fallback) handle the scraper models
# This section only handles the basic event/venue models if needed
if not SKIP_MAIN_REGISTRATIONS:
    print("⚠️ Loading basic admin configurations for event models")
    
    @admin.register(Venue)
    class VenueAdmin(ModelAdmin):
        list_display = ['internal_venue_id', 'name', 'city', 'state', 'country', 'source_website', 'is_active', 'created_at']
        list_filter = ['country', 'state', 'source_website', 'is_active', 'created_at']
        search_fields = ['name', 'city', 'source_venue_id', 'internal_venue_id']
        ordering = ['name']
        readonly_fields = ['created_at', 'updated_at']

        fieldsets = (
            ('Basic Information', {
                'fields': ('internal_venue_id', 'name', 'source_venue_id', 'source_website')
            }),
            ('Location', {
                'fields': ('address', 'city', 'state', 'country', 'postal_code', 'venue_timezone')
            }),
            ('Details', {
                'fields': ('url', 'is_active')
            }),
            ('Timestamps', {
                'fields': ('created_at', 'updated_at'),
                'classes': ('collapse',)
            }),
        )

    class EventVenueInline(TabularInline):
        model = EventVenue
        extra = 1
        readonly_fields = ['created_at']

    @admin.register(Event)
    class EventAdmin(ModelAdmin):
        list_display = ['internal_event_id', 'name', 'venue_count', 'source_event_id', 'source_website', 'currency', 'event_type', 'is_active', 'created_at']
        list_filter = ['currency', 'event_type', 'source_website', 'is_active', 'created_at']
        search_fields = ['name', 'source_event_id', 'venues__name', 'internal_event_id']
        ordering = ['-created_at']
        readonly_fields = ['created_at', 'updated_at']
        inlines = [EventVenueInline]

        def venue_count(self, obj):
            count = obj.venues.count()
            if count == 1:
                return obj.venues.first().name
            return f"{count} venues"
        venue_count.short_description = 'Venues'

        fieldsets = (
            ('Basic Information', {
                'fields': ('internal_event_id', 'name', 'source_event_id', 'source_website')
            }),
            ('Details', {
                'fields': ('url', 'currency', 'event_type', 'is_active')
            }),
            ('Timestamps', {
                'fields': ('created_at', 'updated_at'),
                'classes': ('collapse',)
            }),
        )

    @admin.register(Performance)
    class PerformanceAdmin(ModelAdmin):
        list_display = ['event_name', 'venue_name', 'performance_datetime_utc', 'source_website', 'is_active', 'created_at']
        list_filter = ['performance_datetime_utc', 'source_website', 'is_active', 'created_at']
        search_fields = ['event_id__name', 'venue_id__name', 'source_performance_id', 'internal_performance_id']
        ordering = ['-performance_datetime_utc']
        readonly_fields = ['created_at', 'updated_at']

        def event_name(self, obj):
            return obj.event_id.name
        event_name.short_description = 'Event'

        def venue_name(self, obj):
            return obj.venue_id.name
        venue_name.short_description = 'Venue'

        fieldsets = (
            ('Basic Information', {
                'fields': ('internal_performance_id', 'event_id', 'venue_id', 'source_performance_id', 'source_website')
            }),
            ('Performance Details', {
                'fields': ('performance_datetime_utc', 'seat_map_url', 'map_width', 'map_height', 'is_active')
            }),
            ('Timestamps', {
                'fields': ('created_at', 'updated_at'),
                'classes': ('collapse',)
            }),
        )

    @admin.register(EventVenue)
    class EventVenueAdmin(ModelAdmin):
        list_display = ['event_name', 'venue_name', 'source_website', 'is_active', 'created_at']
        list_filter = ['source_website', 'is_active', 'created_at']
        search_fields = ['event_id__name', 'venue_id__name']
        ordering = ['-created_at']
        readonly_fields = ['created_at']

        def event_name(self, obj):
            return obj.event_id.name
        event_name.short_description = 'Event'

        def venue_name(self, obj):
            return obj.venue_id.name
        venue_name.short_description = 'Venue'

else:
    print("✅ Skipping main admin registrations - using specialized configurations")



# Monkey patch the default admin site to include dashboard functionality
def custom_index(self, request, extra_context=None):
    """
    Custom admin index with dashboard metrics
    """
    extra_context = extra_context or {}
    
    try:
        # Get current date and calculate date ranges
        now = timezone.now()
        today = now.date()
        yesterday = today - timedelta(days=1)
        week_ago = today - timedelta(days=7)
        
        # Basic counts
        total_scrapers = ScraperDefinition.objects.count()
        active_scrapers = ScraperDefinition.objects.filter(is_enabled=True).count()
        total_scrapes = ScrapeJob.objects.count()
        recent_scrapes = ScrapeJob.objects.filter(scraped_at_utc__date=today).count()
        
        # Success rate calculations
        successful_scrapes = ScrapeJob.objects.filter(scrape_success=True).count()
        failed_scrapes = ScrapeJob.objects.filter(scrape_success=False).count()
        total_completed = successful_scrapes + failed_scrapes
        success_rate = round((successful_scrapes / total_completed * 100) if total_completed > 0 else 0, 1)
        
        # Yesterday's success rate for comparison
        yesterday_successful = ScrapeJob.objects.filter(
            scrape_success=True, 
            scraped_at_utc__date=yesterday
        ).count()
        yesterday_failed = ScrapeJob.objects.filter(
            scrape_success=False, 
            scraped_at_utc__date=yesterday
        ).count()
        yesterday_total = yesterday_successful + yesterday_failed
        yesterday_success_rate = (yesterday_successful / yesterday_total * 100) if yesterday_total > 0 else 0
        success_change = round(success_rate - yesterday_success_rate, 1)
        success_trend = 'positive' if success_change >= 0 else 'negative'
        
        # Proxy counts
        total_proxies = ProxyConfiguration.objects.count()
        active_proxies = ProxyConfiguration.objects.filter(is_active=True).count()
        
        # Data counts
        total_events = Event.objects.count()
        total_venues = Venue.objects.count()
        total_performances = Performance.objects.count()
        total_seats = Seat.objects.count()
        
        # Chart data for last 7 days
        scrape_activity_labels = []
        scrape_activity_success = []
        scrape_activity_failed = []
        
        for i in range(6, -1, -1):
            date = today - timedelta(days=i)
            scrape_activity_labels.append(date.strftime('%m/%d'))
            
            day_successful = ScrapeJob.objects.filter(
                scrape_success=True,
                scraped_at_utc__date=date
            ).count()
            day_failed = ScrapeJob.objects.filter(
                scrape_success=False,
                scraped_at_utc__date=date
            ).count()
            
            scrape_activity_success.append(day_successful)
            scrape_activity_failed.append(day_failed)
        
        # Recent activities
        recent_activities = []
        
        # Get recent scrape jobs
        recent_jobs = ScrapeJob.objects.order_by('-scraped_at_utc')[:5]
        for job in recent_jobs:
            if job.scrape_success:
                icon_class = 'icon-success'
                icon = '✅'
                title = f"Scrape completed: {job.scraper_name}"
            else:
                icon_class = 'icon-error'
                icon = '❌'
                title = f"Scrape failed: {job.scraper_name}"
            
            recent_activities.append({
                'icon_class': icon_class,
                'icon': icon,
                'title': title,
                'description': f"Job ID: {job.scrape_job_key}",
                'timestamp': job.scraped_at_utc
            })
        
        # Add to context
        extra_context.update({
            'total_scrapers': total_scrapers,
            'active_scrapers': active_scrapers,
            'total_scrapes': total_scrapes,
            'recent_scrapes': recent_scrapes,
            'success_rate': success_rate,
            'success_change': success_change,
            'success_trend': success_trend,
            'active_proxies': active_proxies,
            'total_proxies': total_proxies,
            'total_events': total_events,
            'total_venues': total_venues,
            'total_performances': total_performances,
            'total_seats': total_seats,
            'success_count': successful_scrapes,
            'failed_count': failed_scrapes,
            'scrape_activity_labels': json.dumps(scrape_activity_labels),
            'scrape_activity_success': json.dumps(scrape_activity_success),
            'scrape_activity_failed': json.dumps(scrape_activity_failed),
            'recent_activities': recent_activities,
        })
    except Exception as e:
        # Fallback values if there's any error
        print(f"Dashboard metrics error: {e}")
        extra_context.update({
            'total_scrapers': 0,
            'active_scrapers': 0,
            'total_scrapes': 0,
            'recent_scrapes': 0,
            'success_rate': 0,
            'success_change': 0,
            'success_trend': 'positive',
            'active_proxies': 0,
            'total_proxies': 0,
            'total_events': 0,
            'total_venues': 0,
            'total_performances': 0,
            'total_seats': 0,
            'success_count': 0,
            'failed_count': 0,
            'scrape_activity_labels': json.dumps(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']),
            'scrape_activity_success': json.dumps([0, 0, 0, 0, 0, 0, 0]),
            'scrape_activity_failed': json.dumps([0, 0, 0, 0, 0, 0, 0]),
            'recent_activities': [],
        })
    
    return admin.site.__class__.index(admin.site, request, extra_context)

# Apply the custom index to the default admin site
admin.site.index = custom_index.__get__(admin.site, admin.site.__class__)

# Override the admin site's index template
admin.site.index_template = 'admin/dashboard.html'


def dashboard_callback(request, context):
    """
    Dashboard callback for Unfold admin theme
    """
    print("🎯 Dashboard callback called!")
    try:
        # Get current date and calculate date ranges
        now = timezone.now()
        today = now.date()
        yesterday = today - timedelta(days=1)
        week_ago = today - timedelta(days=7)
        
        # Basic counts
        total_scrapers = ScraperDefinition.objects.count()
        active_scrapers = ScraperDefinition.objects.filter(is_enabled=True).count()
        total_scrapes = ScrapeJob.objects.count()
        
        # Success rate calculations
        successful_scrapes = ScrapeJob.objects.filter(scrape_success=True).count()
        failed_scrapes = ScrapeJob.objects.filter(scrape_success=False).count()
        total_completed = successful_scrapes + failed_scrapes
        success_rate = round((successful_scrapes / total_completed * 100) if total_completed > 0 else 0, 1)
        
        # Proxy counts
        active_proxies = ProxyConfiguration.objects.filter(is_active=True).count()
        
        # Chart data for last 7 days
        activity_labels = []
        activity_data = []
        
        for i in range(6, -1, -1):
            date = today - timedelta(days=i)
            activity_labels.append(date.strftime('%a'))  # Mon, Tue, etc.
            
            day_total = ScrapeJob.objects.filter(scraped_at_utc__date=date).count()
            activity_data.append(day_total)
        
        # Recent scrape jobs
        recent_scrapes = ScrapeJob.objects.select_related('scraper_definition').order_by('-scraped_at_utc')[:5]
        
        # Create dashboard data
        dashboard_data = {
            'total_scrapers': total_scrapers,
            'active_scrapers': active_scrapers,
            'total_scrapes': total_scrapes,
            'success_rate': success_rate,
            'active_proxies': active_proxies,
            'successful_scrapes': successful_scrapes,
            'failed_scrapes': failed_scrapes,
            'activity_labels': json.dumps(activity_labels),
            'activity_data': json.dumps(activity_data),
            'recent_scrapes': recent_scrapes,
        }
        
        context.update({
            'dashboard_data': dashboard_data
        })
        
    except Exception as e:
        print(f"Dashboard callback error: {e}")
        # Fallback data
        context.update({
            'dashboard_data': {
                'total_scrapers': 0,
                'active_scrapers': 0,
                'total_scrapes': 0,
                'success_rate': 0,
                'active_proxies': 0,
                'successful_scrapes': 0,
                'failed_scrapes': 0,
                'activity_labels': json.dumps(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']),
                'activity_data': json.dumps([0, 0, 0, 0, 0, 0, 0]),
                'recent_scrapes': [],
            }
        })
    
    return context


# POSListing Admin with Split Pack tracking
@admin.register(POSListing)
class POSListingAdmin(admin.ModelAdmin):
    """Admin interface for POSListing with split pack admin hold tracking"""
    
    list_display = [
        'pos_listing_id',
        'performance',
        'pos_inventory_id',
        'stubhub_inventory_id',
        'status',
        'admin_hold_applied',
        'admin_hold_date',
        'created_at'
    ]
    
    list_filter = [
        'status',
        'admin_hold_applied',
        'admin_hold_date',
        'created_at',
        'performance__event_id__name',
        'performance__venue_id__name'
    ]
    
    search_fields = [
        'pos_inventory_id',
        'stubhub_inventory_id',
        'performance__event_id__name',
        'performance__venue_id__name',
        'admin_hold_reason'
    ]
    
    readonly_fields = [
        'pos_listing_id',
        'created_at',
        'updated_at',
        'admin_hold_date'
    ]
    
    fieldsets = [
        ('Basic Information', {
            'fields': [
                'pos_listing_id',
                'performance',
                'pos_inventory_id',
                'stubhub_inventory_id',
                'status'
            ]
        }),
        ('Admin Hold Tracking', {
            'fields': [
                'admin_hold_applied',
                'admin_hold_date',
                'admin_hold_reason'
            ],
            'classes': ['collapse']
        }),
        ('Timestamps', {
            'fields': [
                'created_at',
                'updated_at'
            ],
            'classes': ['collapse']
        })
    ]
    
    def get_queryset(self, request):
        return super().get_queryset(request).select_related(
            'performance',
            'performance__event_id',
            'performance__venue_id'
        )


# Custom admin view for Split Pack analysis
class SplitPackAnalysisView:
    """Custom view for analyzing split packs and their admin hold status"""
    
    def get_split_pack_data(self):
        """Get data for split pack analysis"""
        from .models.seat_packs import SeatPack
        
        # Get split packs with their POSListing data
        split_packs = SeatPack.objects.filter(
            delist_reason='transformed',
            is_active=False,
            pos_listing__isnull=False
        ).select_related(
            'pos_listing',
            'zone_id',
            'zone_id__performance_id',
            'zone_id__performance_id__event_id',
            'zone_id__performance_id__venue_id'
        )
        
        # Group by admin hold status
        data = {
            'total_split_packs': split_packs.count(),
            'with_admin_hold': split_packs.filter(pos_listing__admin_hold_applied=True).count(),
            'without_admin_hold': split_packs.filter(pos_listing__admin_hold_applied=False).count(),
            'split_packs': split_packs[:50]  # Limit for performance
        }
        
        return data