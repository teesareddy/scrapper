# scrapers/models/proxy.py
"""
Proxy management models for scraper configuration.
Handles proxy providers, configurations, and assignments.
"""

from django.db import models
from django.utils import timezone


class ProxyProvider(models.Model):
    """
    Represents different proxy providers (e.g., Webshare, Bright Data, etc.)
    This allows for easy extension to support multiple providers.
    """
    
    provider_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True, help_text="Provider name (e.g., 'webshare', 'brightdata')")
    display_name = models.CharField(max_length=200, help_text="Human-readable provider name")
    description = models.TextField(blank=True, help_text="Provider description and features")
    
    # Provider configuration
    base_url = models.URLField(blank=True, help_text="Provider's base URL or API endpoint")
    auth_method = models.CharField(
        max_length=50, 
        choices=[
            ('basic', 'Basic Authentication'),
            ('bearer', 'Bearer Token'),
            ('api_key', 'API Key'),
            ('custom', 'Custom Authentication'),
        ],
        default='basic',
        help_text="Authentication method used by this provider"
    )
    
    # Provider capabilities
    supports_rotation = models.BooleanField(default=True, help_text="Provider supports automatic rotation")
    supports_geolocation = models.BooleanField(default=False, help_text="Provider supports geo-location selection")
    supports_session_persistence = models.BooleanField(default=False, help_text="Provider supports session persistence")
    
    # Status and health
    is_active = models.BooleanField(default=True)
    is_available = models.BooleanField(default=True, help_text="Provider is currently available")
    last_health_check = models.DateTimeField(null=True, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.display_name

    class Meta:
        db_table = 'proxy_provider'
        verbose_name = 'Proxy Provider'
        verbose_name_plural = 'Proxy Providers'
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['is_active']),
            models.Index(fields=['is_available']),
        ]


class ProxyConfiguration(models.Model):
    """
    Represents individual proxy configurations from different providers.
    Each configuration can have different proxy types (residential, datacenter).
    """
    
    PROXY_TYPE_CHOICES = [
        ('residential', 'Rotating Residential'),
        ('datacenter', 'Rotating Datacenter'),
        ('static_residential', 'Static Residential'),
        ('static_datacenter', 'Static Datacenter'),
        ('mobile', 'Mobile'),
    ]
    
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('testing', 'Testing'),
        ('failed', 'Failed'),
        ('banned', 'Banned'),
        ('maintenance', 'Maintenance'),
    ]
    
    config_id = models.AutoField(primary_key=True)
    provider = models.ForeignKey(
        ProxyProvider, 
        on_delete=models.CASCADE, 
        related_name='configurations',
        help_text="Proxy provider for this configuration"
    )
    
    # Configuration identification
    name = models.CharField(max_length=100, help_text="Unique name for this proxy configuration")
    description = models.TextField(blank=True, help_text="Description of this proxy configuration")
    proxy_type = models.CharField(
        max_length=20, 
        choices=PROXY_TYPE_CHOICES,
        help_text="Type of proxy (residential, datacenter, etc.)"
    )
    
    # Connection details
    host = models.CharField(max_length=255, help_text="Proxy server hostname or IP")
    port = models.PositiveIntegerField(help_text="Proxy server port")
    username = models.CharField(max_length=255, blank=True, help_text="Authentication username")
    password = models.CharField(max_length=255, blank=True, help_text="Authentication password")
    protocol = models.CharField(
        max_length=10,
        choices=[('http', 'HTTP'), ('https', 'HTTPS'), ('socks4', 'SOCKS4'), ('socks5', 'SOCKS5')],
        default='http',
        help_text="Proxy protocol"
    )
    
    # Geographic and routing information
    country_code = models.CharField(max_length=2, blank=True, help_text="ISO country code")
    region = models.CharField(max_length=100, blank=True, help_text="Region or state")
    city = models.CharField(max_length=100, blank=True, help_text="City")
    
    # Configuration and limits
    max_concurrent_connections = models.PositiveIntegerField(default=10, help_text="Maximum concurrent connections")
    timeout_seconds = models.PositiveIntegerField(default=30, help_text="Connection timeout in seconds")
    retry_attempts = models.PositiveIntegerField(default=3, help_text="Number of retry attempts")
    
    # Status and health
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='inactive')
    is_active = models.BooleanField(default=True)
    priority = models.PositiveIntegerField(default=1, help_text="Lower number = higher priority")
    
    # Performance metrics
    response_time_ms = models.PositiveIntegerField(null=True, blank=True, help_text="Average response time")
    success_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0.00, help_text="Success rate percentage")
    total_requests = models.PositiveIntegerField(default=0)
    successful_requests = models.PositiveIntegerField(default=0)
    failed_requests = models.PositiveIntegerField(default=0)
    
    # Health monitoring
    last_health_check = models.DateTimeField(null=True, blank=True)
    last_success = models.DateTimeField(null=True, blank=True)
    last_failure = models.DateTimeField(null=True, blank=True)
    consecutive_failures = models.PositiveIntegerField(default=0)
    
    # Usage tracking
    current_connections = models.PositiveIntegerField(default=0)
    bandwidth_used_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.proxy_type}) - {self.host}:{self.port}"

    @property
    def proxy_url(self):
        """Generate the full proxy URL"""
        if self.username and self.password:
            return f"{self.protocol}://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"

    def calculate_success_rate(self):
        """Calculate and update success rate"""
        if self.total_requests > 0:
            self.success_rate = (self.successful_requests / self.total_requests) * 100
        else:
            self.success_rate = 0
        return self.success_rate

    def is_healthy(self):
        """Check if proxy configuration is healthy"""
        if self.consecutive_failures >= 5:
            return False
        if self.success_rate < 70:
            return False
        if self.status in ['failed', 'banned']:
            return False
        return True

    def get_status_display_with_icon(self):
        """Return status with appropriate icon"""
        icons = {
            'active': '🟢 Active',
            'inactive': '⚪ Inactive', 
            'testing': '🔄 Testing',
            'failed': '❌ Failed',
            'banned': '🚫 Banned',
            'maintenance': '🔧 Maintenance',
        }
        return icons.get(self.status, self.get_status_display())

    class Meta:
        db_table = 'proxy_configuration'
        verbose_name = 'Proxy Configuration'
        verbose_name_plural = 'Proxy Configurations'
        unique_together = ['provider', 'name']
        indexes = [
            models.Index(fields=['provider']),
            models.Index(fields=['proxy_type']),
            models.Index(fields=['status']),
            models.Index(fields=['is_active']),
            models.Index(fields=['priority']),
            models.Index(fields=['success_rate']),
            models.Index(fields=['last_health_check']),
        ]


class ScraperProxyAssignment(models.Model):
    """
    Assigns specific proxy configurations to scrapers.
    This allows different scrapers to use different proxy types or providers.
    """
    
    assignment_id = models.AutoField(primary_key=True)
    scraper_name = models.CharField(max_length=100, help_text="Name of the scraper")
    scraper_definition = models.ForeignKey(
        'scrapers.ScraperDefinition',
        on_delete=models.CASCADE,
        related_name='proxy_assignments',
        null=True,
        blank=True,
        help_text="Link to scraper definition (optional for backward compatibility)"
    )
    proxy_configuration = models.ForeignKey(
        ProxyConfiguration,
        on_delete=models.CASCADE,
        related_name='scraper_assignments',
        help_text="Proxy configuration to use for this scraper"
    )
    
    # Assignment configuration
    is_primary = models.BooleanField(default=False, help_text="Primary proxy for this scraper")
    is_fallback = models.BooleanField(default=False, help_text="Fallback proxy if primary fails")
    fallback_order = models.PositiveIntegerField(default=1, help_text="Order of fallback (1=first fallback)")
    
    # Usage rules
    max_requests_per_hour = models.PositiveIntegerField(null=True, blank=True, help_text="Rate limiting")
    max_concurrent_requests = models.PositiveIntegerField(default=1, help_text="Concurrent request limit")
    
    # Scheduling and conditions
    time_restrictions = models.JSONField(
        default=dict, 
        blank=True,
        help_text="Time-based restrictions (e.g., {'start_hour': 9, 'end_hour': 17})"
    )
    conditions = models.JSONField(
        default=dict,
        blank=True, 
        help_text="Conditions for using this proxy (e.g., {'target_domains': ['example.com']})"
    )
    
    # Status
    is_active = models.BooleanField(default=True)
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        assignment_type = "Primary" if self.is_primary else f"Fallback {self.fallback_order}"
        return f"{self.scraper_name} -> {self.proxy_configuration.name} ({assignment_type})"

    class Meta:
        db_table = 'scraper_proxy_assignment'
        verbose_name = 'Scraper Proxy Assignment'
        verbose_name_plural = 'Scraper Proxy Assignments'
        unique_together = ['scraper_name', 'proxy_configuration']
        indexes = [
            models.Index(fields=['scraper_name']),
            models.Index(fields=['proxy_configuration']),
            models.Index(fields=['is_primary']),
            models.Index(fields=['is_fallback']),
            models.Index(fields=['is_active']),
        ]


class ProxyUsageLog(models.Model):
    """
    Logs proxy usage for monitoring and analytics.
    Tracks which proxy was used for which request and the outcome.
    """
    
    log_id = models.AutoField(primary_key=True)
    proxy_configuration = models.ForeignKey(
        ProxyConfiguration,
        on_delete=models.CASCADE,
        related_name='usage_logs',
        help_text="Proxy configuration that was used"
    )
    scraper_name = models.CharField(max_length=100, help_text="Scraper that used the proxy")
    scrape_job = models.ForeignKey(
        'scrapers.ScrapeJob',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='proxy_usage_logs',
        help_text="Related scrape job"
    )
    
    # Request details
    target_url = models.URLField(max_length=2000, help_text="URL that was requested")
    target_domain = models.CharField(max_length=255, help_text="Domain of the target URL")
    request_method = models.CharField(max_length=10, default='GET', help_text="HTTP method")
    
    # Response details
    response_status_code = models.PositiveIntegerField(null=True, blank=True)
    response_time_ms = models.PositiveIntegerField(null=True, blank=True)
    response_size_bytes = models.PositiveIntegerField(null=True, blank=True)
    
    # Success/failure tracking
    was_successful = models.BooleanField(help_text="Whether the request was successful")
    error_type = models.CharField(
        max_length=50,
        blank=True,
        choices=[
            ('timeout', 'Timeout'),
            ('connection_error', 'Connection Error'),
            ('proxy_error', 'Proxy Error'),
            ('http_error', 'HTTP Error'),
            ('unknown', 'Unknown Error'),
        ],
        help_text="Type of error if request failed"
    )
    error_message = models.TextField(blank=True, help_text="Detailed error message")
    
    # Timing
    started_at = models.DateTimeField(help_text="When the request started")
    completed_at = models.DateTimeField(help_text="When the request completed")
    
    # Metadata
    user_agent = models.TextField(blank=True, help_text="User agent used for the request")
    additional_headers = models.JSONField(default=dict, blank=True, help_text="Additional headers used")

    def __str__(self):
        status = "Success" if self.was_successful else f"Failed ({self.error_type})"
        return f"{self.scraper_name} -> {self.target_domain} - {status}"

    @property 
    def duration_ms(self):
        """Calculate request duration in milliseconds"""
        if self.started_at and self.completed_at:
            delta = self.completed_at - self.started_at
            return int(delta.total_seconds() * 1000)
        return None

    class Meta:
        db_table = 'proxy_usage_log'
        verbose_name = 'Proxy Usage Log'
        verbose_name_plural = 'Proxy Usage Logs'
        indexes = [
            models.Index(fields=['proxy_configuration']),
            models.Index(fields=['scraper_name']),
            models.Index(fields=['target_domain']),
            models.Index(fields=['was_successful']),
            models.Index(fields=['started_at']),
            models.Index(fields=['response_status_code']),
        ]