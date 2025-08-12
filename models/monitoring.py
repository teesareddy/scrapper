# scrapers/models/monitoring.py
"""
Monitoring and performance tracking models for scrapers.
Handles status tracking, metrics collection, and resource monitoring.
"""

from django.db import models
from django.utils import timezone
from datetime import timedelta

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    psutil = None


class ScrapeJob(models.Model):
    """Records of scraping operations"""
    scrape_job_key = models.AutoField(primary_key=True)
    performance_id = models.ForeignKey(
        'scrapers.Performance',
        on_delete=models.CASCADE,
        related_name='scrape_jobs',
        db_column='internal_performance_id'
    )
    scraper_name = models.CharField(max_length=100)
    source_website = models.CharField(max_length=255)
    scraper_version = models.CharField(max_length=50, null=True, blank=True)
    scraped_at_utc = models.DateTimeField(default=timezone.now)
    scrape_success = models.BooleanField(default=True)
    http_status = models.IntegerField(null=True, blank=True)
    error_message = models.TextField(null=True, blank=True)
    raw_payload = models.JSONField(default=dict)
    scraper_config = models.JSONField(default=dict)

    def __str__(self):
        status = "Success" if self.scrape_success else "Failed"
        return f"{self.scraper_name} - {status} at {self.scraped_at_utc}"

    class Meta:
        db_table = 'scrape_job'
        indexes = [
            models.Index(fields=['performance_id']),
            models.Index(fields=['scraper_name']),
            models.Index(fields=['source_website']),
            models.Index(fields=['scraped_at_utc']),
            models.Index(fields=['scrape_success']),
        ]


class ScraperStatus(models.Model):
    """Track the current status and health of each scraper"""
    
    STATUS_CHOICES = [
        ('idle', 'Idle'),
        ('running', 'Running'),
        ('paused', 'Paused'),
        ('error', 'Error'),
        ('maintenance', 'Maintenance'),
        ('disabled', 'Disabled'),
    ]
    
    HEALTH_CHOICES = [
        ('excellent', 'Excellent'),
        ('good', 'Good'),
        ('warning', 'Warning'),
        ('critical', 'Critical'),
        ('unknown', 'Unknown'),
    ]
    
    status_id = models.AutoField(primary_key=True)
    scraper_name = models.CharField(max_length=100, unique=True)
    display_name = models.CharField(max_length=200, help_text="Human-readable scraper name")
    description = models.TextField(blank=True, help_text="Scraper description and purpose")
    
    # Status information
    current_status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='idle')
    health_status = models.CharField(max_length=20, choices=HEALTH_CHOICES, default='unknown')
    is_active = models.BooleanField(default=True)
    is_available = models.BooleanField(default=True, help_text="Whether scraper is available for new jobs")
    
    # Execution statistics
    total_runs = models.PositiveIntegerField(default=0)
    successful_runs = models.PositiveIntegerField(default=0)
    failed_runs = models.PositiveIntegerField(default=0)
    success_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0.00, help_text="Success rate percentage")
    
    # Timing information
    last_run_start = models.DateTimeField(null=True, blank=True)
    last_run_end = models.DateTimeField(null=True, blank=True)
    last_success = models.DateTimeField(null=True, blank=True)
    last_failure = models.DateTimeField(null=True, blank=True)
    average_runtime = models.DurationField(null=True, blank=True, help_text="Average scraping runtime")
    
    # Error tracking
    last_error_message = models.TextField(blank=True)
    consecutive_failures = models.PositiveIntegerField(default=0)
    max_consecutive_failures = models.PositiveIntegerField(default=5, help_text="Max failures before auto-disable")
    
    # Configuration
    scraper_version = models.CharField(max_length=50, blank=True)
    configuration = models.JSONField(default=dict, help_text="Scraper-specific configuration")
    enabled_features = models.JSONField(default=list, help_text="List of enabled features")
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.display_name} ({self.current_status})"

    def calculate_success_rate(self):
        """Calculate and update success rate"""
        if self.total_runs > 0:
            self.success_rate = (self.successful_runs / self.total_runs) * 100
        else:
            self.success_rate = 0
        return self.success_rate

    def update_health_status(self):
        """Automatically determine health status based on metrics"""
        if self.consecutive_failures >= self.max_consecutive_failures:
            self.health_status = 'critical'
        elif self.consecutive_failures >= 3:
            self.health_status = 'warning'
        elif self.success_rate >= 95:
            self.health_status = 'excellent'
        elif self.success_rate >= 80:
            self.health_status = 'good'
        else:
            self.health_status = 'warning'
        
        return self.health_status

    def get_status_display_with_icon(self):
        """Return status with appropriate icon"""
        icons = {
            'idle': '⏸️ Idle',
            'running': '🏃 Running',
            'paused': '⏸️ Paused',
            'error': '❌ Error',
            'maintenance': '🔧 Maintenance',
            'disabled': '🚫 Disabled',
        }
        return icons.get(self.current_status, self.get_current_status_display())

    def get_health_display_with_icon(self):
        """Return health status with appropriate icon"""
        icons = {
            'excellent': '💚 Excellent',
            'good': '🟢 Good',
            'warning': '🟡 Warning',
            'critical': '🔴 Critical',
            'unknown': '❓ Unknown',
        }
        return icons.get(self.health_status, self.get_health_status_display())

    class Meta:
        db_table = 'scraper_status'
        verbose_name = 'Scraper Status'
        verbose_name_plural = 'Scraper Statuses'
        indexes = [
            models.Index(fields=['scraper_name']),
            models.Index(fields=['current_status']),
            models.Index(fields=['health_status']),
            models.Index(fields=['is_active']),
            models.Index(fields=['last_run_start']),
        ]


class ScrapingEvent(models.Model):
    """Track all scraping lifecycle events for dashboard monitoring"""
    
    EVENT_TYPE_CHOICES = [
        ('scrape_started', 'Scrape Started'),
        ('extraction_started', 'Data Extraction Started'),
        ('extraction_completed', 'Data Extraction Completed'),
        ('processing_started', 'Data Processing Started'),
        ('processing_completed', 'Data Processing Completed'),
        ('storage_started', 'Database Storage Started'),
        ('storage_completed', 'Database Storage Completed'),
        ('scrape_completed', 'Scrape Completed'),
        ('scrape_failed', 'Scrape Failed'),
        ('error_occurred', 'Error Occurred'),
        ('status_update', 'Status Update'),
        ('progress_update', 'Progress Update'),
    ]
    
    SEVERITY_CHOICES = [
        ('info', 'Info'),
        ('warning', 'Warning'),
        ('error', 'Error'),
        ('critical', 'Critical'),
    ]
    
    event_id = models.AutoField(primary_key=True)
    external_job_id = models.CharField(max_length=100, help_text="External scrape job ID from NestJS")
    scraper_name = models.CharField(max_length=100)
    event_type = models.CharField(max_length=30, choices=EVENT_TYPE_CHOICES)
    severity = models.CharField(max_length=10, choices=SEVERITY_CHOICES, default='info')
    
    # Event details
    message = models.TextField(help_text="Event description or message")
    url = models.URLField(max_length=2000, null=True, blank=True)
    venue = models.CharField(max_length=200, null=True, blank=True)
    event_title = models.CharField(max_length=300, null=True, blank=True)
    
    # Progress tracking
    items_scraped = models.PositiveIntegerField(null=True, blank=True)
    total_items = models.PositiveIntegerField(null=True, blank=True)
    progress_percentage = models.DecimalField(max_digits=5, decimal_places=2, null=True, blank=True)
    
    # Performance metrics
    processing_time_ms = models.PositiveIntegerField(null=True, blank=True)
    memory_usage_mb = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    response_time_ms = models.PositiveIntegerField(null=True, blank=True)
    
    # Error information
    error_type = models.CharField(max_length=100, null=True, blank=True)
    error_details = models.TextField(null=True, blank=True)
    stack_trace = models.TextField(null=True, blank=True)
    
    # Additional data
    metadata = models.JSONField(default=dict, help_text="Additional event-specific data")
    user_id = models.PositiveIntegerField(null=True, blank=True, help_text="User who initiated the scrape")
    
    # Relationships
    scrape_job = models.ForeignKey(
        ScrapeJob,
        on_delete=models.CASCADE,
        related_name='events',
        null=True,
        blank=True,
        help_text="Related Django scrape job if available"
    )
    
    # Timestamps
    timestamp = models.DateTimeField(default=timezone.now)
    created_at = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"{self.scraper_name} - {self.get_event_type_display()} [{self.timestamp}]"

    @property
    def duration_display(self):
        """Human-readable processing time"""
        if self.processing_time_ms:
            if self.processing_time_ms < 1000:
                return f"{self.processing_time_ms}ms"
            else:
                return f"{self.processing_time_ms / 1000:.2f}s"
        return "N/A"

    @property
    def progress_display(self):
        """Human-readable progress"""
        if self.progress_percentage is not None:
            return f"{self.progress_percentage}%"
        elif self.items_scraped is not None and self.total_items is not None:
            return f"{self.items_scraped}/{self.total_items}"
        elif self.items_scraped is not None:
            return f"{self.items_scraped} items"
        return "N/A"

    def get_severity_icon(self):
        """Return severity with icon"""
        icons = {
            'info': '💡',
            'warning': '⚠️',
            'error': '❌',
            'critical': '🚨',
        }
        return icons.get(self.severity, '📝')

    class Meta:
        db_table = 'scraping_event'
        verbose_name = 'Scraping Event'
        verbose_name_plural = 'Scraping Events'
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['external_job_id']),
            models.Index(fields=['scraper_name']),
            models.Index(fields=['event_type']),
            models.Index(fields=['severity']),
            models.Index(fields=['timestamp']),
            models.Index(fields=['venue']),
            models.Index(fields=['user_id']),
            models.Index(fields=['external_job_id', 'timestamp']),
        ]


class ResourceMonitor(models.Model):
    """Monitor system resource usage during scraping operations"""
    
    monitor_id = models.AutoField(primary_key=True)
    scraper_name = models.CharField(max_length=100)
    scrape_job = models.ForeignKey(
        ScrapeJob, 
        on_delete=models.CASCADE, 
        related_name='resource_monitors',
        null=True, 
        blank=True
    )
    
    # System resources
    cpu_usage_percent = models.DecimalField(max_digits=5, decimal_places=2, help_text="CPU usage percentage")
    memory_usage_mb = models.DecimalField(max_digits=10, decimal_places=2, help_text="Memory usage in MB")
    memory_usage_percent = models.DecimalField(max_digits=5, decimal_places=2, help_text="Memory usage percentage")
    disk_io_read_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    disk_io_write_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    
    # Network resources
    network_download_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    network_upload_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    active_connections = models.PositiveIntegerField(default=0)
    
    # Browser/Process specific
    browser_processes = models.PositiveIntegerField(default=0, help_text="Number of browser processes")
    browser_memory_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    page_load_time_ms = models.PositiveIntegerField(null=True, blank=True)
    
    # Performance metrics
    requests_per_minute = models.DecimalField(max_digits=8, decimal_places=2, default=0.00)
    response_time_avg_ms = models.PositiveIntegerField(null=True, blank=True)
    error_rate_percent = models.DecimalField(max_digits=5, decimal_places=2, default=0.00)
    
    # Timestamps
    recorded_at = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"{self.scraper_name} - {self.recorded_at.strftime('%Y-%m-%d %H:%M:%S')}"

    @classmethod
    def capture_current_stats(cls, scraper_name, scrape_job=None):
        """Capture current system statistics"""
        if not PSUTIL_AVAILABLE:
            print("psutil not available, skipping resource monitoring")
            return None
            
        try:
            # Get current system stats
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk_io = psutil.disk_io_counters()
            network_io = psutil.net_io_counters()
            
            # Create monitoring record
            monitor = cls.objects.create(
                scraper_name=scraper_name,
                scrape_job=scrape_job,
                cpu_usage_percent=cpu_percent,
                memory_usage_mb=memory.used / (1024 * 1024),
                memory_usage_percent=memory.percent,
                disk_io_read_mb=(disk_io.read_bytes / (1024 * 1024)) if disk_io else 0,
                disk_io_write_mb=(disk_io.write_bytes / (1024 * 1024)) if disk_io else 0,
                network_download_mb=(network_io.bytes_recv / (1024 * 1024)) if network_io else 0,
                network_upload_mb=(network_io.bytes_sent / (1024 * 1024)) if network_io else 0,
            )
            return monitor
        except Exception as e:
            # Log error but don't fail the scraping process
            print(f"Error capturing resource stats: {e}")
            return None

    class Meta:
        db_table = 'resource_monitor'
        verbose_name = 'Resource Monitor'
        verbose_name_plural = 'Resource Monitors'
        indexes = [
            models.Index(fields=['scraper_name']),
            models.Index(fields=['recorded_at']),
            models.Index(fields=['scrape_job']),
            models.Index(fields=['cpu_usage_percent']),
            models.Index(fields=['memory_usage_percent']),
        ]


class ScraperMetrics(models.Model):
    """Aggregate metrics and analytics for scrapers"""
    
    metrics_id = models.AutoField(primary_key=True)
    scraper_name = models.CharField(max_length=100)
    
    # Time period for these metrics
    date = models.DateField(help_text="Date for daily metrics")
    hour = models.PositiveIntegerField(null=True, blank=True, help_text="Hour for hourly metrics (0-23)")
    
    # Execution metrics
    total_runs = models.PositiveIntegerField(default=0)
    successful_runs = models.PositiveIntegerField(default=0)
    failed_runs = models.PositiveIntegerField(default=0)
    success_rate = models.DecimalField(max_digits=5, decimal_places=2, default=0.00)
    
    # Performance metrics
    avg_runtime_seconds = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    min_runtime_seconds = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    max_runtime_seconds = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    
    # Data extraction metrics
    total_items_scraped = models.PositiveIntegerField(default=0)
    total_venues_processed = models.PositiveIntegerField(default=0)
    total_events_processed = models.PositiveIntegerField(default=0)
    total_seats_processed = models.PositiveIntegerField(default=0)
    
    # Resource usage metrics
    avg_cpu_usage = models.DecimalField(max_digits=5, decimal_places=2, default=0.00)
    avg_memory_usage_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    total_bandwidth_mb = models.DecimalField(max_digits=10, decimal_places=2, default=0.00)
    
    # Error metrics
    total_errors = models.PositiveIntegerField(default=0)
    timeout_errors = models.PositiveIntegerField(default=0)
    network_errors = models.PositiveIntegerField(default=0)
    parsing_errors = models.PositiveIntegerField(default=0)
    
    # Metadata
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        period = f"{self.date}"
        if self.hour is not None:
            period += f" {self.hour:02d}:00"
        return f"{self.scraper_name} - {period}"

    def calculate_success_rate(self):
        """Calculate and update success rate"""
        if self.total_runs > 0:
            self.success_rate = (self.successful_runs / self.total_runs) * 100
        else:
            self.success_rate = 0
        return self.success_rate

    class Meta:
        db_table = 'scraper_metrics'
        verbose_name = 'Scraper Metrics'
        verbose_name_plural = 'Scraper Metrics'
        unique_together = ['scraper_name', 'date', 'hour']
        indexes = [
            models.Index(fields=['scraper_name']),
            models.Index(fields=['date']),
            models.Index(fields=['success_rate']),
            models.Index(fields=['total_runs']),
        ]