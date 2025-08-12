"""
Fallback admin configuration for scrapers (works without Unfold)
This file contains admin configurations for standard Django admin
"""

from django.contrib import admin
from django.utils.html import format_html
from django.urls import reverse
from django.utils import timezone

from .models import (
    ScraperDefinition, ProxyProvider, ProxyConfiguration, 
    ScraperProxyAssignment, ProxyUsageLog, ScraperExecution,
    ScraperSchedule
)


# ====================
# STANDARD DJANGO ADMIN (NO UNFOLD)
# ====================

@admin.register(ScraperDefinition)
class ScraperDefinitionStandardAdmin(admin.ModelAdmin):
    """Standard Django admin for scraper definitions."""
    
    list_display = [
        'display_name', 'name', 'status_display', 'proxy_status',
        'success_rate_display', 'last_run_display', 'is_enabled'
    ]
    
    list_filter = [
        'status', 'is_enabled', 'use_proxy', 'proxy_type_required',
        'optimization_level', 'can_be_scheduled', 'created_at'
    ]
    
    search_fields = ['name', 'display_name', 'description', 'target_website']
    list_editable = ['is_enabled']
    
    readonly_fields = [
        'created_at', 'updated_at', 'total_runs', 'successful_runs', 
        'failed_runs', 'last_run_at', 'last_success_at', 'success_rate'
    ]
    
    fieldsets = [
        ("Basic Information", {
            'fields': [
                'name', 'display_name', 'description', 'status', 'is_enabled'
            ],
        }),
        ("Website Configuration", {
            'fields': [
                'target_website', 'target_domains'
            ],
        }),
        ("Proxy Settings", {
            'fields': [
                'use_proxy', 'proxy_type_required', 'fail_without_proxy'
            ],
            'description': 'Configure proxy requirements for this scraper'
        }),
        ("Performance & Optimization", {
            'fields': [
                'optimization_level', 'optimization_enabled', 'timeout_seconds',
                'retry_attempts', 'retry_delay_seconds'
            ],
        }),
        ("Rate Limiting", {
            'fields': [
                'max_concurrent_jobs', 'delay_between_requests_ms'
            ],
        }),
        ("Browser Configuration", {
            'fields': [
                'headless_mode', 'user_agent', 'viewport_width', 'viewport_height'
            ],
            'classes': ['collapse'],
        }),
        ("Debug & Monitoring", {
            'fields': [
                'enable_screenshots', 'enable_detailed_logging', 'log_level'
            ],
            'classes': ['collapse'],
        }),
        ("Scheduling", {
            'fields': [
                'can_be_scheduled', 'schedule_interval_hours'
            ],
            'classes': ['collapse'],
        }),
        ("Advanced Configuration", {
            'fields': [
                'custom_settings'
            ],
            'classes': ['collapse'],
        }),
        ("Statistics", {
            'fields': [
                'total_runs', 'successful_runs', 'failed_runs', 
                'last_run_at', 'last_success_at', 'success_rate'
            ],
            'classes': ['collapse'],
        }),
        ("Metadata", {
            'fields': [
                'created_at', 'updated_at', 'created_by'
            ],
            'classes': ['collapse'],
        }),
    ]
    
    actions = ['enable_scrapers', 'disable_scrapers', 'reset_statistics']
    
    def status_display(self, obj):
        """Display status with color."""
        colors = {
            'active': '#28a745',
            'inactive': '#6c757d',
            'maintenance': '#ffc107',
            'disabled': '#dc3545',
            'testing': '#17a2b8'
        }
        color = colors.get(obj.status, '#6c757d')
        return format_html(
            '<span style="color: {}; font-weight: bold;">{}</span>',
            color, obj.get_status_display()
        )
    status_display.short_description = 'Status'
    
    def proxy_status(self, obj):
        """Display proxy status."""
        if not obj.use_proxy:
            return "No Proxy"
        
        proxy = obj.assigned_proxy
        if proxy:
            return format_html('<span style="color: #28a745;">✓ {}</span>', proxy.name)
        else:
            return format_html('<span style="color: #dc3545;">✗ No Proxy</span>')
    proxy_status.short_description = 'Proxy'
    
    def success_rate_display(self, obj):
        """Display success rate."""
        try:
            rate = obj.success_rate or 0
            color = '#28a745' if rate >= 90 else '#ffc107' if rate >= 70 else '#dc3545'
            return format_html(
                '<span style="color: {}; font-weight: bold;">{:.1f}%</span>',
                color, rate
            )
        except:
            return "N/A"
    success_rate_display.short_description = 'Success Rate'
    
    def last_run_display(self, obj):
        """Display last run time."""
        if obj.last_run_at:
            from django.utils.timesince import timesince
            return f"{timesince(obj.last_run_at)} ago"
        return "Never"
    last_run_display.short_description = 'Last Run'
    
    def enable_scrapers(self, request, queryset):
        """Enable selected scrapers."""
        updated = queryset.update(is_enabled=True, status='active')
        self.message_user(request, f"Enabled {updated} scrapers.")
    enable_scrapers.short_description = "Enable selected scrapers"
    
    def disable_scrapers(self, request, queryset):
        """Disable selected scrapers."""
        updated = queryset.update(is_enabled=False, status='inactive')
        self.message_user(request, f"Disabled {updated} scrapers.")
    disable_scrapers.short_description = "Disable selected scrapers"
    
    def reset_statistics(self, request, queryset):
        """Reset statistics for selected scrapers."""
        updated = queryset.update(
            total_runs=0, successful_runs=0, failed_runs=0,
            last_run_at=None, last_success_at=None
        )
        self.message_user(request, f"Reset statistics for {updated} scrapers.")
    reset_statistics.short_description = "Reset statistics"


@admin.register(ProxyProvider)
class ProxyProviderStandardAdmin(admin.ModelAdmin):
    """Standard Django admin for proxy providers."""
    
    list_display = [
        'display_name', 'name', 'is_active', 
        'configuration_count', 'capabilities_display', 'updated_at'
    ]
    list_filter = ['is_active', 'supports_rotation', 'supports_geolocation', 'created_at']
    search_fields = ['name', 'display_name', 'description']
    list_editable = ['is_active']
    readonly_fields = ['created_at', 'updated_at']
    
    fieldsets = [
        ("Basic Information", {
            'fields': ['name', 'display_name', 'description', 'is_active'],
        }),
        ("Configuration", {
            'fields': ['base_url', 'auth_method'],
        }),
        ("Capabilities", {
            'fields': [
                'supports_rotation', 'supports_geolocation', 
                'supports_session_persistence'
            ],
        }),
        ("Metadata", {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse'],
        }),
    ]
    
    def configuration_count(self, obj):
        """Display configuration count."""
        count = obj.configurations.filter(is_active=True).count()
        if count > 0:
            url = reverse('admin:scrapers_proxyconfiguration_changelist')
            return format_html(
                '<a href="{}?provider__id__exact={}">{} configs</a>',
                url, obj.pk, count
            )
        return '0 configs'
    configuration_count.short_description = 'Configurations'
    
    def capabilities_display(self, obj):
        """Display capabilities."""
        capabilities = []
        if obj.supports_rotation:
            capabilities.append('Rotation')
        if obj.supports_geolocation:
            capabilities.append('Geo')
        if obj.supports_session_persistence:
            capabilities.append('Session')
        return ', '.join(capabilities) if capabilities else 'None'
    capabilities_display.short_description = 'Capabilities'


@admin.register(ProxyConfiguration)
class ProxyConfigurationStandardAdmin(admin.ModelAdmin):
    """Standard Django admin for proxy configurations."""
    
    list_display = [
        'name_with_provider', 'proxy_type', 'endpoint_display',
        'status', 'success_rate_display', 'usage_count', 'is_active'
    ]
    list_filter = [
        'provider', 'proxy_type', 'status', 'is_active', 'country_code', 'updated_at'
    ]
    search_fields = ['name', 'host', 'username', 'provider__name']
    list_editable = ['is_active']
    readonly_fields = [
        'created_at', 'updated_at', 'last_health_check', 
        'last_success', 'last_failure', 'success_rate'
    ]
    
    fieldsets = [
        ("Basic Information", {
            'fields': ['provider', 'name', 'proxy_type', 'is_active'],
        }),
        ("Connection Details", {
            'fields': ['host', 'port', 'username', 'password', 'protocol'],
        }),
        ("Geographic Info", {
            'fields': ['country_code', 'region', 'city'],
        }),
        ("Configuration", {
            'fields': [
                'max_concurrent_connections', 'timeout_seconds', 
                'retry_attempts', 'priority'
            ],
        }),
        ("Health Monitoring", {
            'fields': [
                'last_health_check', 'last_success', 'last_failure',
                'consecutive_failures', 'success_rate'
            ],
            'classes': ['collapse'],
        }),
        ("Metadata", {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse'],
        }),
    ]
    
    actions = ['test_selected_proxies', 'activate_proxies', 'deactivate_proxies']
    
    def name_with_provider(self, obj):
        """Display name with provider."""
        return f"{obj.provider.display_name} - {obj.name}"
    name_with_provider.short_description = 'Configuration'
    
    def endpoint_display(self, obj):
        """Display proxy endpoint."""
        return f"{obj.host}:{obj.port}"
    endpoint_display.short_description = 'Endpoint'
    
    def success_rate_display(self, obj):
        """Display success rate."""
        if obj.success_rate is not None:
            rate = float(obj.success_rate)
            color = '#28a745' if rate >= 90 else '#ffc107' if rate >= 70 else '#dc3545'
            return format_html(
                '<span style="color: {}; font-weight: bold;">{:.1f}%</span>',
                color, rate
            )
        return 'N/A'
    success_rate_display.short_description = 'Success Rate'
    
    def usage_count(self, obj):
        """Display usage count."""
        return f"{obj.total_requests} requests"
    usage_count.short_description = 'Usage'
    
    def test_selected_proxies(self, request, queryset):
        """Test selected proxy configurations."""
        count = queryset.count()
        self.message_user(request, f"Testing {count} proxy configurations...")
    test_selected_proxies.short_description = "Test selected proxies"
    
    def activate_proxies(self, request, queryset):
        """Activate selected proxies."""
        updated = queryset.update(is_active=True)
        self.message_user(request, f"Activated {updated} proxy configurations.")
    activate_proxies.short_description = "Activate proxies"
    
    def deactivate_proxies(self, request, queryset):
        """Deactivate selected proxies."""
        updated = queryset.update(is_active=False)
        self.message_user(request, f"Deactivated {updated} proxy configurations.")
    deactivate_proxies.short_description = "Deactivate proxies"


@admin.register(ScraperProxyAssignment)
class ScraperProxyAssignmentStandardAdmin(admin.ModelAdmin):
    """Standard Django admin for scraper proxy assignments."""
    
    list_display = [
        'scraper_display', 'proxy_display', 'assignment_type',
        'is_active', 'usage_count_display', 'created_at'
    ]
    list_filter = [
        'is_active', 'is_primary', 'is_fallback', 
        'proxy_configuration__provider', 'created_at'
    ]
    search_fields = [
        'scraper_name', 'proxy_configuration__name', 
        'scraper_definition__display_name'
    ]
    list_editable = ['is_active']
    readonly_fields = ['created_at', 'updated_at']
    
    fieldsets = [
        ("Assignment Details", {
            'fields': [
                'scraper_name', 'scraper_definition', 
                'proxy_configuration', 'is_active'
            ],
        }),
        ("Configuration", {
            'fields': [
                'is_primary', 'is_fallback', 'fallback_order',
                'max_requests_per_hour', 'max_concurrent_requests'
            ],
        }),
        ("Metadata", {
            'fields': ['created_at', 'updated_at'],
            'classes': ['collapse'],
        }),
    ]
    
    def scraper_display(self, obj):
        """Display scraper with link."""
        if obj.scraper_definition:
            url = reverse('admin:scrapers_scraperdefinition_change', 
                         args=[obj.scraper_definition.pk])
            return format_html(
                '<a href="{}">{}</a>',
                url, obj.scraper_definition.display_name
            )
        return obj.scraper_name
    scraper_display.short_description = 'Scraper'
    
    def proxy_display(self, obj):
        """Display proxy configuration."""
        proxy = obj.proxy_configuration
        return f"{proxy.provider.display_name} - {proxy.name}"
    proxy_display.short_description = 'Proxy'
    
    def assignment_type(self, obj):
        """Display assignment type."""
        if obj.is_primary:
            return "Primary"
        elif obj.is_fallback:
            return f"Fallback #{obj.fallback_order}"
        else:
            return "Standard"
    assignment_type.short_description = 'Type'
    
    def usage_count_display(self, obj):
        """Display usage count."""
        count = obj.proxy_configuration.usage_logs.filter(
            scraper_name=obj.scraper_name
        ).count()
        return f"{count} uses"
    usage_count_display.short_description = 'Usage'


# Update the admin site configuration
admin.site.site_header = "🎭 Ticket Scraper Management System"
admin.site.site_title = "Scraper Admin"
admin.site.index_title = "Welcome to the Scraper Management Dashboard"