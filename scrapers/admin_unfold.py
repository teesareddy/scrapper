"""
Unfold-optimized admin configuration for scrapers
Organized by importance and usage frequency with clear descriptions
"""

from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse, path
from django.utils import timezone
from django.shortcuts import redirect
from unfold.admin import ModelAdmin, TabularInline
from unfold.decorators import display
from django.db.models import Count, Q

from .models import (
    # Core Models (Most Used)
    ScraperDefinition, ProxyProvider, ProxyConfiguration, ScraperProxyAssignment,
    Venue, Event, Performance, ScrapeJob,
    
    # Event Structure Models
    Level, Zone, Section, Seat, SeatPack,
    
    # Snapshot Models (Price Tracking)
    SeatSnapshot, LevelPriceSnapshot, ZonePriceSnapshot, SectionPriceSnapshot,
    
    # Monitoring & Analytics
    ScraperExecution, ScraperMetrics, ScraperStatus, ProxyUsageLog,
    ResourceMonitor,
    
    # Advanced Configuration
    ScraperSchedule, OptimizationRule, ScraperOptimizationSettings,
)

# ============================================================================
# 🔥 CORE MODELS (MOST FREQUENTLY USED)
# ============================================================================

# Inline classes for tabs
class ScraperExecutionInline(TabularInline):
    model = ScraperExecution
    extra = 0
    readonly_fields = ['execution_id', 'status', 'started_at', 'completed_at', 'duration_seconds', 'items_extracted', 'success']
    fields = ['execution_id', 'status', 'started_at', 'duration_seconds', 'items_extracted', 'success']
    
    def has_add_permission(self, request, obj=None):
        return False

class ScraperScheduleInline(TabularInline):
    model = ScraperSchedule
    extra = 0
    readonly_fields = ['schedule_id', 'name', 'schedule_type', 'is_active', 'next_execution_at', 'last_execution_at']
    fields = ['name', 'schedule_type', 'is_active', 'next_execution_at', 'last_execution_at']

class ScraperProxyAssignmentInline(TabularInline):
    model = ScraperProxyAssignment
    extra = 0
    readonly_fields = ['assignment_id']
    fields = ['proxy_configuration', 'is_primary', 'is_fallback', 'is_active']

class ScraperOptimizationSettingsInline(TabularInline):
    model = ScraperOptimizationSettings
    extra = 0
    readonly_fields = ['setting_id']
    fields = ['optimization_rule', 'is_enabled', 'custom_pattern']

@admin.register(ScraperDefinition)
class ScraperDefinitionAdmin(ModelAdmin):
    """
    🤖 SCRAPER DEFINITIONS - Main scraper configurations
    Create and manage different scrapers for various websites.
    Configure scraping parameters, schedules, and optimization settings.
    """
    
    list_display = ['internal_id', 'name', 'display_name', 'target_website', 'optimization_level', 'status_badge', 'success_rate_bar', 'last_run_at']
    list_filter = ['status', 'is_enabled', 'optimization_level', 'created_at']
    search_fields = ['internal_id', 'name', 'target_website', 'description']
    readonly_fields = ['created_at', 'updated_at', 'total_runs', 'successful_runs', 'failed_runs', 'last_run_at', 'last_success_at']
    actions = ['onboard_new_scraper']
    
    # Add inlines to show related data
    inlines = [ScraperProxyAssignmentInline, ScraperScheduleInline, ScraperOptimizationSettingsInline, ScraperExecutionInline]
    
    # Enhanced fieldsets with all available fields organized in logical groups
    fieldsets = [
        ('🤖 Basic Information', {
            'fields': ['internal_id', 'name', 'display_name', 'target_website', 'target_domains', 'description', 'is_enabled']
        }),
        ('⚙️ Core Configuration', {
            'fields': ['status', 'browser_engine', 'headless_mode', 'timeout_seconds', 'retry_attempts', 'priority']
        }),
        ('🌐 Proxy Configuration', {
            'fields': ['proxy_settings', 'use_proxy', 'fail_without_proxy'],
            'classes': ['collapse']
        }),
        ('🔧 Performance & Optimization', {
            'fields': ['max_concurrent_jobs', 'delay_between_requests_ms', 'optimization_enabled', 'optimization_level'],
            'classes': ['collapse']
        }),
        ('🐛 Debug & Monitoring', {
            'fields': ['enable_screenshots', 'enable_detailed_logging', 'log_level'],
            'classes': ['collapse']
        }),
        ('🤖 Captcha Handling', {
            'fields': ['captcha_required', 'captcha_type'],
            'classes': ['collapse']
        }),
        ('⏰ Scheduling', {
            'fields': ['can_be_scheduled', 'schedule_interval_hours'],
            'classes': ['collapse']
        }),
        ('📊 Statistics & Metadata', {
            'fields': ['total_runs', 'successful_runs', 'failed_runs', 'last_run_at', 'last_success_at', 'created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path('onboard/', self.onboard_scraper_view, name='scrapers_onboard_scraper'),
        ]
        return custom_urls + urls
    
    def onboard_scraper_view(self, request):
        """Redirect to the scraper onboarding page"""
        return redirect('scrapers:onboarding')
    
    def onboard_new_scraper(self, request, queryset):
        """Admin action to go to onboarding page"""
        return redirect('scrapers:onboarding')
    onboard_new_scraper.short_description = "🚀 Add New Scraper (Onboarding)"
    
    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context['onboard_url'] = reverse('scrapers:onboarding')
        return super().changelist_view(request, extra_context)
    
    @display(description="Status")
    def status_badge(self, obj):
        if obj.is_enabled:
            return format_html('<span class="badge badge-success">🟢 Enabled</span>')
        return format_html('<span class="badge badge-secondary">⭕ Disabled</span>')
    
    @display(description="Success Rate")
    def success_rate_bar(self, obj):
        # This would need actual success rate calculation
        rate = 85  # Placeholder
        color = 'success' if rate >= 80 else 'warning' if rate >= 60 else 'danger'
        return format_html(
            '<div class="progress" style="width: 100px;"><div class="progress-bar bg-{}" style="width: {}%">{}%</div></div>',
            color, rate, f"{rate:.1f}"
        )

@admin.register(ProxyProvider)
class ProxyProviderAdmin(ModelAdmin):
    """
    🌐 PROXY PROVIDERS - Manage proxy services
    Configure different proxy providers for scraping operations.
    Track provider health, capabilities, and configuration counts.
    """
    
    list_display = ['name', 'display_name', 'status_badge', 'capabilities_display', 'config_count_link', 'is_available']
    list_filter = ['is_active', 'is_available', 'supports_rotation', 'supports_geolocation']
    search_fields = ['name', 'display_name', 'description']
    readonly_fields = ['created_at', 'updated_at', 'last_health_check']
    
    fieldsets = [
        ('📝 Basic Information', {
            'fields': ['name', 'display_name', 'description', 'is_active']
        }),
        ('🎯 Capabilities', {
            'fields': ['supports_rotation', 'supports_geolocation', 'supports_session_persistence']
        }),
        ('📊 Health & Status', {
            'fields': ['is_available', 'last_health_check']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Status")
    def status_badge(self, obj):
        if obj.is_active and obj.is_available:
            return format_html('<span class="badge badge-success">🟢 Active & Available</span>')
        elif obj.is_active:
            return format_html('<span class="badge badge-warning">🟡 Active (Unavailable)</span>')
        return format_html('<span class="badge badge-secondary">⭕ Inactive</span>')
    
    @display(description="Capabilities")
    def capabilities_display(self, obj):
        caps = []
        if obj.supports_rotation:
            caps.append("🔄 Rotation")
        if obj.supports_geolocation:
            caps.append("🌍 Geo")
        if obj.supports_session_persistence:
            caps.append("💾 Sessions")
        return " ".join(caps) if caps else "❌ None"

    @display(description="Configurations")
    def config_count_link(self, obj):
        count = obj.configurations.count()
        url = reverse('admin:scrapers_proxyconfiguration_changelist') + f'?provider__id__exact={obj.pk}'
        return format_html('<a href="{}">{} configs</a>', url, count)

@admin.register(ProxyConfiguration)
class ProxyConfigurationAdmin(ModelAdmin):
    """
    ⚙️ PROXY CONFIGURATIONS - Individual proxy settings
    Configure specific proxy endpoints and credentials.
    Monitor proxy performance and usage statistics.
    """
    
    list_display = ['name', 'provider', 'proxy_type', 'host_port', 'status', 'success_rate', 'is_active']
    list_filter = ['provider', 'proxy_type', 'is_active', 'status']
    search_fields = ['name', 'host', 'provider__name']
    readonly_fields = ['success_rate', 'total_requests', 'last_health_check']
    
    fieldsets = [
        ('📝 Basic Information', {
            'fields': ['name', 'provider', 'proxy_type', 'is_active']
        }),
        ('🌐 Connection Details', {
            'fields': ['host', 'port', 'username', 'password', 'protocol']
        }),
        ('📊 Status & Monitoring', {
            'fields': ['status', 'success_rate', 'total_requests', 'last_health_check']
        }),
    ]
    
    @display(description="Host:Port")
    def host_port(self, obj):
        return f"{obj.host}:{obj.port}"

@admin.register(ScraperProxyAssignment)
class ScraperProxyAssignmentAdmin(ModelAdmin):
    """
    🔗 SCRAPER-PROXY ASSIGNMENTS - Link scrapers to proxies
    Assign specific proxies to scrapers with fallback configurations.
    Manage rate limiting and priority settings.
    """
    
    list_display = ['scraper_name', 'proxy_configuration', 'assignment_type', 'is_active']
    list_filter = ['is_active', 'is_primary', 'is_fallback']
    search_fields = ['scraper_name', 'proxy_configuration__name']
    
    fieldsets = [
        ('🔗 Assignment Details', {
            'fields': ['scraper_name', 'scraper_definition', 'proxy_configuration', 'is_active']
        }),
        ('⚙️ Assignment Type', {
            'fields': ['is_primary', 'is_fallback', 'fallback_order']
        }),
        ('🚦 Rate Limiting', {
            'fields': ['max_requests_per_hour', 'max_concurrent_requests']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Type")
    def assignment_type(self, obj):
        if obj.is_primary:
            return "🎯 Primary"
        elif obj.is_fallback:
            return f"🔄 Fallback #{obj.fallback_order}"
        return "📋 Standard"

# ============================================================================
# 🎭 EVENT & VENUE MANAGEMENT
# ============================================================================

@admin.register(Venue)
class VenueAdmin(ModelAdmin):
    """
    🏛️ VENUES - Physical locations where events take place
    Manage venue information, addresses, and seating configurations.
    Track venue activity and associated events.
    """
    
    list_display = ['name', 'city_state', 'event_count', 'is_active', 'seat_structure']
    list_filter = ['is_active', 'state', 'country', 'seat_structure']
    search_fields = ['name', 'city', 'state', 'address']
    readonly_fields = ['internal_venue_id', 'created_at', 'updated_at']
    
    fieldsets = [
        ('🏛️ Basic Information', {
            'fields': ['name', 'is_active']
        }),
        ('📍 Location Details', {
            'fields': ['address', 'city', 'state', 'country', 'postal_code', 'venue_timezone']
        }),
        ('🪑 Seating Configuration', {
            'fields': ['seat_structure']
        }),
        ('🌐 Online Presence', {
            'fields': ['url']
        }),
        ('🔧 Technical Details', {
            'fields': ['internal_venue_id', 'source_venue_id', 'source_website'],
            'classes': ['collapse']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Location")
    def city_state(self, obj):
        return f"{obj.city}, {obj.state}"
    
    @display(description="Events")
    def event_count(self, obj):
        count = obj.events.count()
        return f"{count} events"

@admin.register(Event)
class EventAdmin(ModelAdmin):
    """
    🎭 EVENTS - Shows, concerts, and performances
    Manage event information and venue associations.
    Track event activity across multiple venues.
    """
    
    list_display = ['name', 'venue_count', 'performance_count', 'is_active', 'event_type']
    list_filter = ['is_active', 'event_type', 'currency', 'created_at']
    search_fields = ['name', 'venues__name']
    readonly_fields = ['internal_event_id', 'created_at', 'updated_at']
    
    fieldsets = [
        ('🎭 Basic Information', {
            'fields': ['name', 'event_type', 'is_active']
        }),
        ('💰 Pricing', {
            'fields': ['currency']
        }),
        ('🌐 Online Presence', {
            'fields': ['url']
        }),
        ('🔧 Technical Details', {
            'fields': ['internal_event_id', 'source_event_id', 'source_website'],
            'classes': ['collapse']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Venues")
    def venue_count(self, obj):
        count = obj.venues.count()
        return f"{count} venues"
    
    @display(description="Performances")
    def performance_count(self, obj):
        count = obj.performances.count()
        return f"{count} performances"

@admin.register(Performance)
class PerformanceAdmin(ModelAdmin):
    """
    🎪 PERFORMANCES - Specific show instances
    Manage individual performance dates and times.
    Track seating maps and performance-specific data.
    """
    
    list_display = ['event_name', 'venue_name', 'performance_datetime_utc', 'seat_data_status', 'is_active']
    list_filter = ['is_active', 'performance_datetime_utc', 'event_id__event_type']
    search_fields = ['internal_performance_id', 'event_id__name', 'venue_id__name']
    readonly_fields = ['internal_performance_id', 'created_at', 'updated_at']
    date_hierarchy = 'performance_datetime_utc'
    
    fieldsets = [
        ('🎪 Performance Details', {
            'fields': ['internal_performance_id', 'event_id', 'venue_id', 'performance_datetime_utc', 'is_active']
        }),
        ('🗺️ Seat Map Configuration', {
            'fields': ['seat_map_url', 'map_width', 'map_height']
        }),
        ('🔧 Technical Details', {
            'fields': ['source_performance_id', 'source_website'],
            'classes': ['collapse']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Event")
    def event_name(self, obj):
        return obj.event_id.name
    
    @display(description="Venue")
    def venue_name(self, obj):
        return obj.venue_id.name
    
    @display(description="Seat Data")
    def seat_data_status(self, obj):
        level_count = obj.levels.count()
        seat_count = sum(level.sections.aggregate(
            total=Count('seats'))['total'] or 0 for level in obj.levels.all())
        return f"{level_count} levels, {seat_count} seats"

# ============================================================================
# 🪑 SEATING STRUCTURE MODELS
# ============================================================================

@admin.register(Level)
class LevelAdmin(ModelAdmin):
    """
    🏢 LEVELS - Venue levels (Orchestra, Mezzanine, Balcony)
    Manage venue levels and their hierarchical organization.
    Track sections and seating within each level.
    """
    
    list_display = ['name', 'level_type', 'section_count', 'display_order']
    list_filter = ['level_type', 'is_active']
    search_fields = ['internal_level_id', 'name', 'raw_name']
    readonly_fields = ['internal_level_id', 'created_at', 'updated_at']
    
    fieldsets = [
        ('🏢 Level Information', {
            'fields': ['internal_level_id', 'name', 'raw_name', 'level_type', 'level_number']
        }),
        ('📊 Organization', {
            'fields': ['display_order', 'is_active']
        }),
        ('🔧 Technical Details', {
            'fields': ['source_level_id', 'source_website'],
            'classes': ['collapse']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Sections")
    def section_count(self, obj):
        count = obj.sections.count()
        return f"{count} sections"

@admin.register(Zone)
class ZoneAdmin(ModelAdmin):
    """
    🎯 ZONES - Pricing and organizational zones
    Manage pricing zones that can span across levels and sections.
    Configure view types, accessibility, and zone characteristics.
    """
    
    list_display = ['name', 'performance_info', 'zone_type', 'view_type_display', 'accessibility_badge', 'seat_count']
    list_filter = ['zone_type', 'view_type', 'wheelchair_accessible', 'is_active']
    search_fields = ['internal_zone_id', 'name', 'raw_identifier', 'performance_id__event_id__name']
    readonly_fields = ['internal_zone_id', 'created_at', 'updated_at']
    
    fieldsets = [
        ('🎯 Zone Information', {
            'fields': ['internal_zone_id', 'performance_id', 'name', 'raw_identifier', 'zone_type']
        }),
        ('👁️ View & Accessibility', {
            'fields': ['view_type', 'wheelchair_accessible']
        }),
        ('🎨 Display Settings', {
            'fields': ['color_code', 'display_order', 'is_active']
        }),
        ('📝 Additional Information', {
            'fields': ['miscellaneous']
        }),
        ('🔧 Technical Details', {
            'fields': ['source_zone_id', 'source_website'],
            'classes': ['collapse']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Performance")
    def performance_info(self, obj):
        return f"{obj.performance_id.event_id.name} @ {obj.performance_id.venue_id.name}"
    
    @display(description="View Type")
    def view_type_display(self, obj):
        if obj.view_type:
            icons = {
                'clear': '👁️ Clear',
                'partial': '👁️‍🗨️ Partial',
                'obstructed': '🚫 Obstructed',
                'premium': '⭐ Premium',
                'excellent': '🌟 Excellent'
            }
            return icons.get(obj.view_type, f"👁️ {obj.get_view_type_display()}")
        return "❓ Unknown"
    
    @display(description="Accessibility")
    def accessibility_badge(self, obj):
        if obj.wheelchair_accessible:
            return format_html('<span class="badge badge-success">♿ Accessible</span>')
        return format_html('<span class="badge badge-secondary">❌ Not Accessible</span>')
    
    @display(description="Seats")
    def seat_count(self, obj):
        count = obj.seats.count()
        return f"{count} seats"

@admin.register(Section)
class SectionAdmin(ModelAdmin):
    """
    📐 SECTIONS - Sections within levels
    Manage individual sections and their seating arrangements.
    Track section types and organization within levels.
    """
    
    list_display = ['name', 'level_info', 'section_type', 'seat_count', 'display_order']
    list_filter = ['section_type', 'is_active', 'level_id__level_type']
    search_fields = ['internal_section_id', 'name', 'raw_name', 'level_id__name']
    readonly_fields = ['internal_section_id', 'created_at', 'updated_at']
    
    fieldsets = [
        ('📐 Section Information', {
            'fields': ['internal_section_id', 'level_id', 'name', 'raw_name', 'section_type']
        }),
        ('📊 Organization', {
            'fields': ['display_order', 'is_active']
        }),
        ('🔧 Technical Details', {
            'fields': ['source_section_id', 'source_website'],
            'classes': ['collapse']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Level")
    def level_info(self, obj):
        return f"{obj.level_id.name} ({obj.level_id.performance_id.event_id.name})"
    
    @display(description="Seats")
    def seat_count(self, obj):
        count = obj.seats.count()
        return f"{count} seats"

@admin.register(Seat)
class SeatAdmin(ModelAdmin):
    """
    🪑 SEATS - Individual seats
    Manage individual seat information, pricing, and availability.
    Track seat status, coordinates, and current pricing data.
    """
    
    list_display = ['seat_identifier', 'section_zone_info', 'current_status_badge', 'current_pricing', 'seat_type']
    list_filter = ['current_status', 'seat_type', 'is_active', 'section_id__level_id__level_type']
    search_fields = ['internal_seat_id', 'row_label', 'seat_number', 'section_id__name', 'zone_id__name']
    readonly_fields = ['internal_seat_id', 'last_updated', 'created_at', 'updated_at']
    
    fieldsets = [
        ('🪑 Seat Information', {
            'fields': ['internal_seat_id', 'section_id', 'zone_id', 'row_label', 'seat_number', 'seat_type']
        }),
        ('📍 Position & Coordinates', {
            'fields': ['x_coord', 'y_coord']
        }),
        ('💰 Current Status & Pricing', {
            'fields': ['current_status', 'current_price', 'current_fees', 'last_updated', 'last_scrape_job']
        }),
        ('⚙️ Settings', {
            'fields': ['is_active']
        }),
        ('🔧 Technical Details', {
            'fields': ['source_seat_id', 'source_website'],
            'classes': ['collapse']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Seat")
    def seat_identifier(self, obj):
        return f"Row {obj.row_label}, Seat {obj.seat_number}"
    
    @display(description="Section/Zone")
    def section_zone_info(self, obj):
        return f"{obj.section_id.name} / {obj.zone_id.name}"
    
    @display(description="Status")
    def current_status_badge(self, obj):
        status_colors = {
            'available': 'success',
            'sold': 'danger',
            'reserved': 'warning',
            'blocked': 'secondary',
            'unknown': 'secondary'
        }
        color = status_colors.get(obj.current_status, 'secondary')
        return format_html('<span class="badge badge-{}">{}</span>', color, obj.get_current_status_display())
    
    @display(description="Pricing")
    def current_pricing(self, obj):
        if obj.current_price:
            total = obj.get_current_total_price()
            if total != obj.current_price:
                return f"${obj.current_price} (${total} total)"
            return f"${obj.current_price}"
        return "No price"

@admin.register(SeatPack)
class SeatPackAdmin(ModelAdmin):
    """
    📦 SEAT PACKS - Contiguous seat groups
    Manage groups of seats sold together as packages.
    Track pack pricing and seat arrangements.
    """
    
    list_display = ['pack_identifier', 'zone_info', 'pack_size', 'pack_price', 'seat_range', 'scrape_job_info']
    list_filter = ['pack_size', 'zone_id__zone_type', 'created_at']
    search_fields = ['internal_pack_id', 'row_label', 'zone_id__name', 'source_pack_id']
    readonly_fields = ['internal_pack_id', 'created_at']
    
    fieldsets = [
        ('📦 Pack Information', {
            'fields': ['internal_pack_id', 'zone_id', 'scrape_job_key', 'row_label', 'pack_size']
        }),
        ('🪑 Seat Details', {
            'fields': ['start_seat_number', 'end_seat_number', 'seat_keys']
        }),
        ('💰 Pricing', {
            'fields': ['pack_price']
        }),
        ('🔧 Technical Details', {
            'fields': ['source_pack_id', 'source_website'],
            'classes': ['collapse']
        }),
        ('🗓️ Metadata', {
            'fields': ['created_at'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Pack")
    def pack_identifier(self, obj):
        return f"Row {obj.row_label} ({obj.pack_size} seats)"
    
    @display(description="Zone")
    def zone_info(self, obj):
        return f"{obj.zone_id.name} ({obj.zone_id.zone_type})"
    
    @display(description="Seat Range")
    def seat_range(self, obj):
        return f"{obj.start_seat_number}-{obj.end_seat_number}"
    
    @display(description="Scrape Job")
    def scrape_job_info(self, obj):
        return f"Job #{obj.scrape_job_key.pk} ({obj.scrape_job_key.scraped_at_utc.strftime('%m/%d %H:%M')})"

# ============================================================================
# 📊 PRICE TRACKING & SNAPSHOTS
# ============================================================================

@admin.register(SeatSnapshot)
class SeatSnapshotAdmin(ModelAdmin):
    """
    📸 SEAT SNAPSHOTS - Individual seat price history
    Track price changes and availability for individual seats over time.
    Monitor seat-level pricing trends and status changes.
    """
    
    list_display = ['seat_info', 'status_badge', 'price_display', 'snapshot_time', 'scrape_job_link']
    list_filter = ['status', 'snapshot_time', 'scrape_job_key__scraper_name']
    search_fields = ['seat_id__row_label', 'seat_id__seat_number', 'seat_id__section_id__name']
    readonly_fields = ['snapshot_id', 'snapshot_time', 'total_price']
    date_hierarchy = 'snapshot_time'
    
    fieldsets = [
        ('📸 Snapshot Information', {
            'fields': ['scrape_job_key', 'seat_id', 'snapshot_time']
        }),
        ('💰 Pricing & Status', {
            'fields': ['status', 'price', 'fees', 'total_price']
        }),
        ('📝 Raw Data', {
            'fields': ['raw_status_text', 'raw_price_text', 'raw_fees_text'],
            'classes': ['collapse']
        }),
        ('🔧 Technical Details', {
            'fields': ['snapshot_id'],
            'classes': ['collapse']
        }),
    ]
    
    @display(description="Seat")
    def seat_info(self, obj):
        return f"{obj.seat_id.section_id.name} Row {obj.seat_id.row_label}, Seat {obj.seat_id.seat_number}"
    
    @display(description="Status")
    def status_badge(self, obj):
        status_colors = {
            'available': 'success',
            'sold': 'danger',
            'reserved': 'warning',
            'blocked': 'secondary'
        }
        color = status_colors.get(obj.status, 'secondary')
        return format_html('<span class="badge badge-{}">{}</span>', color, obj.status.title())
    
    @display(description="Price")
    def price_display(self, obj):
        if obj.price:
            total = obj.total_price
            if total and total != obj.price:
                return f"${obj.price} (${total} total)"
            return f"${obj.price}"
        return "No price"
    
    @display(description="Scrape Job")
    def scrape_job_link(self, obj):
        url = reverse('admin:scrapers_scrapejob_change', args=[obj.scrape_job_key.pk])
        return format_html('<a href="{}">Job #{}</a>', url, obj.scrape_job_key.pk)

@admin.register(LevelPriceSnapshot)
class LevelPriceSnapshotAdmin(ModelAdmin):
    """
    📊 LEVEL PRICE SNAPSHOTS - Level pricing history
    Track aggregated pricing data at the level granularity.
    Monitor price ranges and availability trends by level.
    """
    
    list_display = ['level_info', 'price_range_display', 'availability_info', 'snapshot_time']
    list_filter = ['snapshot_time', 'level_id__level_type']
    search_fields = ['level_id__name', 'level_id__performance_id__event_id__name']
    readonly_fields = ['snapshot_key', 'snapshot_time', 'availability_percentage']
    date_hierarchy = 'snapshot_time'
    
    @display(description="Level")
    def level_info(self, obj):
        return f"{obj.level_id.name} ({obj.level_id.performance_id.event_id.name})"
    
    @display(description="Availability")
    def availability_info(self, obj):
        if obj.available_seats and obj.total_seats:
            pct = obj.availability_percentage
            return f"{obj.available_seats}/{obj.total_seats} ({pct:.1f}%)"
        return f"{obj.available_seats or 0} available"

@admin.register(ZonePriceSnapshot)
class ZonePriceSnapshotAdmin(ModelAdmin):
    """
    🎯 ZONE PRICE SNAPSHOTS - Zone pricing history
    Track aggregated pricing data at the zone granularity.
    Monitor demand indicators and zone-specific pricing trends.
    """
    
    list_display = ['zone_info', 'price_range_display', 'demand_indicator', 'availability_info', 'snapshot_time']
    list_filter = ['snapshot_time', 'zone_id__zone_type', 'zone_id__view_type']
    search_fields = ['zone_id__name', 'zone_id__performance_id__event_id__name']
    readonly_fields = ['snapshot_key', 'snapshot_time', 'availability_percentage']
    date_hierarchy = 'snapshot_time'
    
    @display(description="Zone")
    def zone_info(self, obj):
        return f"{obj.zone_id.name} ({obj.zone_id.zone_type})"
    
    @display(description="Demand")
    def demand_indicator(self, obj):
        return obj.get_demand_indicator()
    
    @display(description="Availability")
    def availability_info(self, obj):
        if obj.available_seats and obj.total_seats:
            pct = obj.availability_percentage
            return f"{obj.available_seats}/{obj.total_seats} ({pct:.1f}%)"
        return f"{obj.available_seats or 0} available"

@admin.register(SectionPriceSnapshot)
class SectionPriceSnapshotAdmin(ModelAdmin):
    """
    📐 SECTION PRICE SNAPSHOTS - Section pricing history
    Track aggregated pricing data at the section granularity.
    Monitor section-specific pricing and availability patterns.
    """
    
    list_display = ['section_info', 'price_range_display', 'availability_info', 'snapshot_time']
    list_filter = ['snapshot_time', 'section_id__section_type']
    search_fields = ['section_id__name', 'section_id__level_id__name']
    readonly_fields = ['snapshot_key', 'snapshot_time', 'availability_percentage']
    date_hierarchy = 'snapshot_time'
    
    @display(description="Section")
    def section_info(self, obj):
        return f"{obj.section_id.name} ({obj.section_id.level_id.name})"
    
    @display(description="Availability")
    def availability_info(self, obj):
        if obj.available_seats and obj.total_seats:
            pct = obj.availability_percentage
            return f"{obj.available_seats}/{obj.total_seats} ({pct:.1f}%)"
        return f"{obj.available_seats or 0} available"

# ============================================================================
# 📋 MONITORING & OPERATIONS
# ============================================================================

@admin.register(ScrapeJob)
class ScrapeJobAdmin(ModelAdmin):
    """
    📋 SCRAPE JOBS - Monitor scraping tasks
    View current and historical scraping operations.
    Track progress and results of scraping activities.
    """
    
    list_display = ['scraper_name', 'scrape_success', 'scraped_at_utc', 'http_status', 'source_website']
    list_filter = ['scrape_success', 'scraped_at_utc', 'scraper_name', 'source_website']
    readonly_fields = ['scraped_at_utc', 'http_status']
    date_hierarchy = 'scraped_at_utc'

@admin.register(ResourceMonitor)
class ResourceMonitorAdmin(ModelAdmin):
    """
    🖥️ RESOURCE MONITORING - System performance
    Monitor CPU, memory, and network usage.
    Track system health and resource consumption.
    """
    
    list_display = ['scraper_name', 'recorded_at', 'cpu_usage_percent', 'memory_usage_mb']
    list_filter = ['recorded_at', 'scraper_name']
    date_hierarchy = 'recorded_at'

@admin.register(ProxyUsageLog)
class ProxyUsageLogAdmin(ModelAdmin):
    """
    📝 PROXY USAGE LOGS - Track proxy performance
    Monitor which proxies are being used and their success rates.
    Identify problematic proxies and usage patterns.
    """
    
    list_display = ['proxy_configuration', 'scraper_name', 'started_at', 'was_successful', 'response_time_ms']
    list_filter = ['was_successful', 'started_at', 'proxy_configuration']
    readonly_fields = ['started_at', 'response_time_ms']
    date_hierarchy = 'started_at'

# ============================================================================
# ⚙️ ADVANCED CONFIGURATION (Less Frequently Used)
# ============================================================================

@admin.register(ScraperSchedule)
class ScraperScheduleAdmin(ModelAdmin):
    """
    ⏰ SCRAPER SCHEDULES - Automated scraping schedules
    Configure when and how often scrapers should run.
    Set up recurring schedules and time-based triggers.
    """
    
    list_display = ['scraper', 'schedule_type', 'is_active', 'next_execution_at', 'last_execution_at']
    list_filter = ['schedule_type', 'is_active', 'created_at']
    search_fields = ['scraper__name']

@admin.register(ScraperExecution)
class ScraperExecutionAdmin(ModelAdmin):
    """
    🔄 SCRAPER EXECUTIONS - Execution history
    Track individual scraper execution attempts.
    Monitor execution status, duration, and results.
    """
    
    list_display = ['scraper', 'status', 'started_at', 'duration_seconds', 'items_extracted']
    list_filter = ['status', 'started_at', 'scraper']
    readonly_fields = ['started_at', 'completed_at', 'duration_seconds']
    date_hierarchy = 'started_at'

@admin.register(ScraperStatus)
class ScraperStatusAdmin(ModelAdmin):
    """
    📊 SCRAPER STATUS - Current scraper states
    Monitor the current operational status of all scrapers.
    Track health, errors, and performance metrics.
    """
    
    list_display = ['scraper_name', 'current_status', 'health_status', 'success_rate', 'consecutive_failures']
    list_filter = ['current_status', 'health_status', 'is_active']
    search_fields = ['scraper_name']

# Remove unused models from admin to keep it clean
# These models are either legacy or rarely used in day-to-day operations