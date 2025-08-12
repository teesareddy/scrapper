# scrapers/models/legacy.py
"""
Legacy models for backward compatibility.
These models will be deprecated in favor of the new modular structure.
"""

from django.db import models
from django.utils import timezone


class ProxySetting(models.Model):
    """
    DEPRECATED: Legacy proxy settings model.
    Use ProxyConfiguration instead.
    """
    
    PROXY_TYPE_CHOICES = [
        ('http', 'HTTP'),
        ('https', 'HTTPS'),
        ('socks4', 'SOCKS4'),
        ('socks5', 'SOCKS5'),
    ]
    
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('testing', 'Testing'),
        ('failed', 'Failed'),
        ('banned', 'Banned'),
        ('maintenance', 'Maintenance'),
    ]
    
    proxy_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    
    # Proxy configuration
    proxy_type = models.CharField(max_length=10, choices=PROXY_TYPE_CHOICES, default='http')
    host = models.CharField(max_length=255)
    port = models.PositiveIntegerField()
    username = models.CharField(max_length=255, blank=True)
    password = models.CharField(max_length=255, blank=True)
    
    # Provider information
    provider_name = models.CharField(max_length=100, blank=True, help_text="Proxy provider name")
    country_code = models.CharField(max_length=2, blank=True, help_text="ISO country code")
    region = models.CharField(max_length=100, blank=True)
    
    # Status and performance
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='inactive')
    is_active = models.BooleanField(default=True)
    priority = models.PositiveIntegerField(default=1, help_text="Lower number = higher priority")
    
    # Performance metrics
    response_time_ms = models.PositiveIntegerField(null=True, blank=True, help_text="Average response time in milliseconds")
    success_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0.00)
    total_requests = models.PositiveIntegerField(default=0)
    successful_requests = models.PositiveIntegerField(default=0)
    failed_requests = models.PositiveIntegerField(default=0)
    
    # Health checks
    last_health_check = models.DateTimeField(null=True, blank=True)
    last_success = models.DateTimeField(null=True, blank=True)
    last_failure = models.DateTimeField(null=True, blank=True)
    consecutive_failures = models.PositiveIntegerField(default=0)
    
    # Usage tracking
    current_connections = models.PositiveIntegerField(default=0)
    max_connections = models.PositiveIntegerField(default=10)
    bandwidth_used_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.host}:{self.port})"

    def calculate_success_rate(self):
        """Calculate and update success rate"""
        if self.total_requests > 0:
            self.success_rate = (self.successful_requests / self.total_requests) * 100
        else:
            self.success_rate = 0
        return self.success_rate

    def is_healthy(self):
        """Check if proxy is healthy based on metrics"""
        if self.consecutive_failures >= 5:
            return False
        if self.success_rate < 70:
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
        db_table = 'proxy_setting'
        verbose_name = 'Proxy Setting (Legacy)'
        verbose_name_plural = 'Proxy Settings (Legacy)'
        unique_together = ['host', 'port']
        indexes = [
            models.Index(fields=['status']),
            models.Index(fields=['is_active']),
            models.Index(fields=['priority']),
            models.Index(fields=['success_rate']),
            models.Index(fields=['last_health_check']),
        ]


class ScraperConfiguration(models.Model):
    """
    DEPRECATED: Legacy scraper configuration model.
    Use ScraperDefinition instead.
    """
    
    config_id = models.AutoField(primary_key=True)
    scraper_name = models.CharField(max_length=100)
    configuration_name = models.CharField(max_length=100, help_text="Configuration preset name")
    description = models.TextField(blank=True)
    
    # Configuration data
    config_data = models.JSONField(default=dict, help_text="Complete scraper configuration")
    
    # Settings
    is_default = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    
    # Browser settings
    browser_type = models.CharField(max_length=20, default='chrome', help_text="chrome, firefox, safari")
    headless = models.BooleanField(default=True)
    window_size = models.CharField(max_length=20, default='1920x1080')
    user_agent = models.TextField(blank=True)
    
    # Timing settings
    page_timeout_seconds = models.PositiveIntegerField(default=30)
    element_timeout_seconds = models.PositiveIntegerField(default=10)
    delay_between_requests_ms = models.PositiveIntegerField(default=1000)
    
    # Resource settings
    load_images = models.BooleanField(default=False)
    load_css = models.BooleanField(default=True)
    load_javascript = models.BooleanField(default=True)
    
    # Proxy settings
    use_proxy = models.BooleanField(default=False)
    proxy_rotation = models.BooleanField(default=False)
    
    # Retry settings
    max_retries = models.PositiveIntegerField(default=3)
    retry_delay_seconds = models.PositiveIntegerField(default=5)
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.CharField(max_length=100, blank=True)

    def __str__(self):
        return f"{self.scraper_name} - {self.configuration_name}"

    class Meta:
        db_table = 'scraper_configuration'
        verbose_name = 'Scraper Configuration (Legacy)'
        verbose_name_plural = 'Scraper Configurations (Legacy)'
        unique_together = ['scraper_name', 'configuration_name']
        indexes = [
            models.Index(fields=['scraper_name']),
            models.Index(fields=['is_default']),
            models.Index(fields=['is_active']),
        ]