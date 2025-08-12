# scrapers/models/scraper_config.py
"""
Scraper configuration and management models.
Enhanced with comprehensive configuration options for optimization and automation.
"""

from django.db import models
from django.utils import timezone
import uuid


class CaptchaType(models.Model):
    """Available captcha types for scrapers"""
    captcha_type_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    provider = models.CharField(max_length=100, blank=True, help_text="Captcha service provider")
    api_endpoint = models.URLField(max_length=500, blank=True, help_text="API endpoint for solving")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'captcha_type'
        verbose_name = 'Captcha Type'
        verbose_name_plural = 'Captcha Types'
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['is_active']),
        ]


class OptimizationRule(models.Model):
    """URL/resource blocking rules for optimization"""
    
    RULE_TYPE_CHOICES = [
        ('block_url', 'Block Specific URL'),
        ('block_domain', 'Block Domain'),
        ('block_resource_type', 'Block Resource Type'),
        ('block_pattern', 'Block URL Pattern'),
    ]
    
    optimization_rule_id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=200)
    description = models.TextField(blank=True)
    rule_type = models.CharField(max_length=20, choices=RULE_TYPE_CHOICES)
    pattern = models.TextField(help_text="URL pattern, domain, or resource type to block")
    is_active = models.BooleanField(default=True)
    category = models.CharField(
        max_length=50, 
        blank=True,
        help_text="Category like 'social_media', 'analytics', 'ads'"
    )
    priority = models.IntegerField(default=0, help_text="Higher priority rules are applied first")
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.get_rule_type_display()})"

    class Meta:
        db_table = 'optimization_rule'
        verbose_name = 'Optimization Rule'
        verbose_name_plural = 'Optimization Rules'
        ordering = ['-priority', 'name']
        indexes = [
            models.Index(fields=['rule_type']),
            models.Index(fields=['is_active']),
            models.Index(fields=['category']),
            models.Index(fields=['priority']),
        ]


class ScraperDefinition(models.Model):
    """
    Enhanced scraper definition with comprehensive configuration options.
    Central registry for all scrapers with advanced settings.
    """
    
    SCRAPER_STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('maintenance', 'Under Maintenance'),
        ('disabled', 'Disabled'),
        ('testing', 'Testing'),
    ]
    
    BROWSER_ENGINE_CHOICES = [
        ('playwright', 'Playwright'),
        ('selenium', 'Selenium'),
    ]
    
    PRIORITY_CHOICES = [
        ('low', 'Low'),
        ('normal', 'Normal'),
        ('high', 'High'),
        ('critical', 'Critical'),
    ]
    
    # Enhanced identifier
    internal_id = models.CharField(
        max_length=100,
        primary_key=True,
        help_text="Internal identifier for this scraper"
    )
    
    # Basic Information
    name = models.CharField(
        max_length=100, 
        # unique=True,
        help_text="Unique scraper name (e.g., washington_pavilion_scraper)"
    )
    display_name = models.CharField(
        max_length=200,
        help_text="Human-readable name for the scraper"
    )
    description = models.TextField(
        blank=True,
        help_text="Description of what this scraper does"
    )
    
    # Website Information
    target_website = models.URLField(
        max_length=500,
        help_text="Main website this scraper targets"
    )
    target_domains = models.JSONField(
        default=list,
        help_text="List of domains this scraper works with"
    )
    
    # Status and Control
    status = models.CharField(
        max_length=20,
        choices=SCRAPER_STATUS_CHOICES,
        default='active'
    )
    is_enabled = models.BooleanField(
        default=True,
        help_text="Whether this scraper can be used"
    )
    
    # Proxy Configuration
    proxy_settings = models.ForeignKey(
        'scrapers.ProxyConfiguration',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='assigned_scrapers',
        help_text="Proxy configuration to use for this scraper"
    )
    use_proxy = models.BooleanField(
        default=True,
        help_text="Whether this scraper should use proxy"
    )
    fail_without_proxy = models.BooleanField(
        default=True,
        help_text="Fail scraping if use_proxy=True but no proxy is assigned"
    )
    
    # Browser Configuration
    browser_engine = models.CharField(
        max_length=20,
        choices=BROWSER_ENGINE_CHOICES,
        default='playwright',
        help_text="Browser automation engine to use"
    )
    external_chromium_enabled = models.BooleanField(
        default=False,
        help_text="Use external Chromium instance"
    )
    external_chromium_url = models.CharField(
        max_length=500,
        blank=True,
        help_text="URL for external Chromium instance (if enabled)"
    )
    headless_mode = models.BooleanField(
        default=True,
        help_text="Run browser in headless mode"
    )
    user_agent = models.TextField(
        blank=True,
        help_text="Custom user agent string (optional)"
    )
    viewport_width = models.PositiveIntegerField(
        default=1920,
        help_text="Browser viewport width"
    )
    viewport_height = models.PositiveIntegerField(
        default=1080,
        help_text="Browser viewport height"
    )
    
    # Performance and Optimization
    optimization_enabled = models.BooleanField(
        default=True,
        help_text="Enable optimization rules for this scraper"
    )
    optimization_level = models.CharField(
        max_length=20,
        choices=[
            ('balanced', 'Balanced'),
            ('aggressive', 'Aggressive'), 
            ('conservative', 'Conservative'),
            ('minimal', 'Minimal'),
        ],
        default='balanced',
        help_text="Optimization level for performance tuning"
    )
    optimization_rules = models.ManyToManyField(
        OptimizationRule,
        through='ScraperOptimizationSettings',
        related_name='scrapers',
        blank=True,
        help_text="Optimization rules applied to this scraper"
    )
    
    # Timing Configuration
    timeout_seconds = models.PositiveIntegerField(
        default=30,
        help_text="Timeout for scraping operations in seconds"
    )
    retry_attempts = models.PositiveIntegerField(
        default=3,
        help_text="Number of retry attempts on failure"
    )
    retry_delay_seconds = models.PositiveIntegerField(
        default=5,
        help_text="Delay between retry attempts"
    )
    
    # Rate Limiting
    max_concurrent_jobs = models.PositiveIntegerField(
        default=1,
        help_text="Maximum concurrent scraping jobs"
    )
    delay_between_requests_ms = models.PositiveIntegerField(
        default=1000,
        help_text="Delay between requests in milliseconds"
    )
    
    # Captcha Handling
    captcha_required = models.BooleanField(
        default=False,
        help_text="Whether this scraper requires captcha solving"
    )
    captcha_type = models.ForeignKey(
        CaptchaType,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='scrapers',
        help_text="Type of captcha used by this scraper"
    )
    
    # URL and Priority Configuration
    prefix = models.CharField(
        max_length=10,
        unique=True,
        null=True,
        blank=True,
        help_text="Short prefix identifier for this scraper (e.g., 'wp' for Washington Pavilion)"
    )
    url_prefix = models.CharField(
        max_length=200,
        blank=True,
        help_text="URL prefix for this scraper's operations"
    )
    priority = models.CharField(
        max_length=20,
        choices=PRIORITY_CHOICES,
        default='normal',
        help_text="Priority level for this scraper"
    )
    
    # Debug and Monitoring
    enable_screenshots = models.BooleanField(
        default=False,
        help_text="Take screenshots during scraping for debugging"
    )
    enable_detailed_logging = models.BooleanField(
        default=False,
        help_text="Enable detailed logging for this scraper"
    )
    log_level = models.CharField(
        max_length=10,
        choices=[
            ('DEBUG', 'Debug'),
            ('INFO', 'Info'),
            ('WARNING', 'Warning'),
            ('ERROR', 'Error'),
        ],
        default='INFO'
    )
    
    # Scheduling
    can_be_scheduled = models.BooleanField(
        default=True,
        help_text="Whether this scraper can be scheduled to run automatically"
    )
    schedule_interval_hours = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="How often to run this scraper (in hours)"
    )
    
    # Custom Configuration
    custom_settings = models.JSONField(
        default=dict,
        blank=True,
        help_text="Custom settings specific to this scraper"
    )
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.CharField(
        max_length=100,
        default='admin',
        help_text="Who created this scraper configuration"
    )
    
    # Statistics
    total_runs = models.PositiveIntegerField(default=0)
    successful_runs = models.PositiveIntegerField(default=0)
    failed_runs = models.PositiveIntegerField(default=0)
    last_run_at = models.DateTimeField(null=True, blank=True)
    last_success_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.display_name} ({self.name})"

    def save(self, *args, **kwargs):
        if not self.internal_id:
            self.internal_id = str(uuid.uuid4())
        super().save(*args, **kwargs)

    @property
    def success_rate(self):
        """Calculate success rate percentage."""
        if self.total_runs == 0:
            return 0
        return (self.successful_runs / self.total_runs) * 100

    @property
    def assigned_proxy(self):
        """Get the currently assigned proxy configuration."""
        return self.proxy_settings

    def get_optimization_rules(self):
        """Get active optimization rules for this scraper."""
        return self.optimization_rules.filter(
            scraperoptimizationsettings__is_enabled=True,
            is_active=True
        ).order_by('-priority')

    class Meta:
        db_table = 'scraper_definition'
        verbose_name = 'Scraper Definition'
        verbose_name_plural = 'Scraper Definitions'
        indexes = [
            models.Index(fields=['internal_id']),
            models.Index(fields=['name']),
            models.Index(fields=['prefix']),
            models.Index(fields=['status']),
            models.Index(fields=['is_enabled']),
            models.Index(fields=['browser_engine']),
            models.Index(fields=['priority']),
            models.Index(fields=['last_run_at']),
        ]


class ScraperOptimizationSettings(models.Model):
    """Many-to-many through table linking scrapers to optimization rules"""
    setting_id = models.AutoField(primary_key=True)
    scraper_definition = models.ForeignKey(
        ScraperDefinition,
        on_delete=models.CASCADE,
        related_name='optimization_settings'
    )
    optimization_rule = models.ForeignKey(
        OptimizationRule,
        on_delete=models.CASCADE,
        related_name='scraper_settings'
    )
    is_enabled = models.BooleanField(
        default=True,
        help_text="Whether this rule is enabled for this scraper"
    )
    custom_pattern = models.TextField(
        blank=True,
        help_text="Override the rule's default pattern for this scraper"
    )
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        status = "Enabled" if self.is_enabled else "Disabled"
        return f"{self.scraper_definition.name} - {self.optimization_rule.name} ({status})"

    def get_effective_pattern(self):
        """Get the pattern to use (custom override or default)"""
        return self.custom_pattern or self.optimization_rule.pattern

    class Meta:
        db_table = 'scraper_optimization_settings'
        unique_together = ['scraper_definition', 'optimization_rule']
        verbose_name = 'Scraper Optimization Setting'
        verbose_name_plural = 'Scraper Optimization Settings'
        indexes = [
            models.Index(fields=['scraper_definition']),
            models.Index(fields=['optimization_rule']),
            models.Index(fields=['is_enabled']),
        ]


class ScraperExecution(models.Model):
    """
    Tracks individual scraper execution instances with detailed information.
    """
    
    EXECUTION_STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
        ('timeout', 'Timeout'),
    ]
    
    execution_id = models.AutoField(primary_key=True)
    
    # Scraper Information
    scraper = models.ForeignKey(
        ScraperDefinition,
        on_delete=models.CASCADE,
        related_name='executions'
    )
    
    # Execution Details
    status = models.CharField(
        max_length=20,
        choices=EXECUTION_STATUS_CHOICES,
        default='pending'
    )
    target_url = models.URLField(max_length=2000)
    
    # Proxy Information
    proxy_used = models.ForeignKey(
        'scrapers.ProxyConfiguration',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='executions'
    )
    proxy_ip_address = models.GenericIPAddressField(null=True, blank=True)
    
    # Timing
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.DecimalField(
        max_digits=10,
        decimal_places=3,
        null=True,
        blank=True
    )
    
    # Results
    success = models.BooleanField(default=False)
    error_message = models.TextField(blank=True)
    error_type = models.CharField(max_length=100, blank=True)
    
    # Performance Metrics
    response_time_ms = models.PositiveIntegerField(null=True, blank=True)
    memory_usage_mb = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True
    )
    cpu_usage_percent = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        null=True,
        blank=True
    )
    
    # Data Quality
    items_extracted = models.PositiveIntegerField(default=0)
    data_quality_score = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Quality score from 0-100"
    )
    
    # Screenshots and Logs
    screenshot_paths = models.JSONField(
        default=list,
        help_text="Paths to screenshots taken during execution"
    )
    log_file_path = models.CharField(
        max_length=500,
        blank=True,
        help_text="Path to detailed log file"
    )
    
    # Configuration Snapshot
    config_snapshot = models.JSONField(
        default=dict,
        help_text="Configuration used for this execution"
    )

    def __str__(self):
        return f"{self.scraper.name} - {self.status} ({self.started_at})"

    @property
    def duration_display(self):
        """Human-readable duration."""
        if self.duration_seconds:
            return f"{self.duration_seconds:.2f}s"
        return "N/A"

    class Meta:
        db_table = 'scraper_execution'
        verbose_name = 'Scraper Execution'
        verbose_name_plural = 'Scraper Executions'
        ordering = ['-started_at']
        indexes = [
            models.Index(fields=['scraper', 'status']),
            models.Index(fields=['started_at']),
            models.Index(fields=['success']),
            models.Index(fields=['proxy_used']),
        ]


class ScraperSchedule(models.Model):
    """
    Manages scheduling for automatic scraper execution.
    """
    
    SCHEDULE_TYPE_CHOICES = [
        ('interval', 'Interval'),
        ('cron', 'Cron Expression'),
        ('once', 'One Time'),
        ('manual', 'Manual Only'),
    ]
    
    schedule_id = models.AutoField(primary_key=True)
    
    # Scraper Reference
    scraper = models.ForeignKey(
        ScraperDefinition,
        on_delete=models.CASCADE,
        related_name='schedules'
    )
    
    # Schedule Configuration
    name = models.CharField(
        max_length=200,
        help_text="Descriptive name for this schedule"
    )
    schedule_type = models.CharField(
        max_length=20,
        choices=SCHEDULE_TYPE_CHOICES,
        default='interval'
    )
    
    # Interval Configuration
    interval_hours = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Run every X hours"
    )
    interval_minutes = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Run every X minutes"
    )
    
    # Cron Configuration
    cron_expression = models.CharField(
        max_length=100,
        blank=True,
        help_text="Cron expression for complex scheduling"
    )
    
    # One-time Configuration
    scheduled_for = models.DateTimeField(
        null=True,
        blank=True,
        help_text="Specific date/time to run (for one-time execution)"
    )
    
    # Control
    is_active = models.BooleanField(
        default=True,
        help_text="Whether this schedule is active"
    )
    max_executions = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Maximum number of executions (optional)"
    )
    
    # URLs to scrape
    urls_to_scrape = models.JSONField(
        default=list,
        help_text="List of URLs to scrape on each execution"
    )
    
    # Execution tracking
    executions_count = models.PositiveIntegerField(default=0)
    last_execution_at = models.DateTimeField(null=True, blank=True)
    next_execution_at = models.DateTimeField(null=True, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.CharField(max_length=100, default='admin')

    def __str__(self):
        return f"{self.scraper.name} - {self.name}"

    class Meta:
        db_table = 'scraper_schedule'
        verbose_name = 'Scraper Schedule'
        verbose_name_plural = 'Scraper Schedules'
        indexes = [
            models.Index(fields=['scraper']),
            models.Index(fields=['is_active']),
            models.Index(fields=['next_execution_at']),
            models.Index(fields=['schedule_type']),
        ]