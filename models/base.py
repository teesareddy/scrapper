# scrapers/models/base.py
"""
Core venue and event models with enhanced ID fields.
These models represent the fundamental entities for scraped data.
"""

from django.db import models
from django.utils import timezone


class Venue(models.Model):
    """Venues where events take place"""
    internal_venue_id = models.CharField(
        max_length=200,
        primary_key=True,
        editable=False,
        help_text="Internal identifier for this venue"
    )
    source_venue_id = models.CharField(max_length=255)
    source_website = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    address = models.TextField()
    city = models.CharField(max_length=100)
    state = models.CharField(max_length=50)
    country = models.CharField(max_length=2)
    postal_code = models.CharField(max_length=20, null=True, blank=True)
    venue_timezone = models.CharField(max_length=50, null=True, blank=True)
    url = models.URLField(max_length=2000, null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    # Enhanced seating configuration
    SEAT_STRUCTURE_CHOICES = [
        ('odd_even', 'Odd/Even Seating'),
        ('consecutive', 'Consecutive Seating'),
    ]
    seat_structure = models.CharField(
        max_length=20, choices=SEAT_STRUCTURE_CHOICES, null=True, blank=True
    )
    # Track previous seat structure to detect changes and trigger seat pack synchronization
    # This field enables automatic detection of odd_even ↔ consecutive transitions
    previous_seat_structure = models.CharField(
        max_length=20, choices=SEAT_STRUCTURE_CHOICES, null=True, blank=True,
        help_text="Previous seat structure value for change detection"
    )

    # Price markup configuration
    MARKUP_TYPE_CHOICES = [
        ('dollar', 'Dollar Amount'),
        ('percentage', 'Percentage'),
    ]
    
    price_markup_type = models.CharField(
        max_length=20, choices=MARKUP_TYPE_CHOICES, null=True, blank=True,
        help_text="Type of price markup applied to tickets"
    )
    price_markup_value = models.DecimalField(
        max_digits=10, decimal_places=2, null=True, blank=True,
        help_text="Value of the markup (dollar amount or percentage)"
    )
    price_markup_updated_at = models.DateTimeField(
        null=True, blank=True,
        help_text="When price markup was last updated"
    )

    # POS (Point of Sale) configuration
    pos_enabled = models.BooleanField(
        default=False,
        help_text="Enable POS (Point of Sale) integration for this venue"
    )
    pos_enabled_at = models.DateTimeField(
        null=True, blank=True,
        help_text="When POS was last enabled/disabled for this venue"
    )

    def __str__(self):
        return f"{self.name} ({self.city}, {self.state})"

    class Meta:
        db_table = 'venue'
        unique_together = ['source_venue_id', 'source_website']
        indexes = [
            models.Index(fields=['internal_venue_id']),
            models.Index(fields=['name']),
            models.Index(fields=['city', 'state']),
            models.Index(fields=['is_active']),
            models.Index(fields=['source_website']),
            models.Index(fields=['price_markup_type']),
            models.Index(fields=['price_markup_updated_at']),
            models.Index(fields=['pos_enabled']),
            models.Index(fields=['pos_enabled_at']),
        ]


class Event(models.Model):
    """Events that can happen at multiple venues"""
    internal_event_id = models.CharField(
        max_length=200,
        primary_key=True,
        editable=False,
        help_text="Internal identifier for this event"
    )
    venues = models.ManyToManyField(
        Venue,
        related_name='events',
        through='EventVenue',
        help_text="Venues where this event takes place"
    )
    source_event_id = models.CharField(max_length=255)
    source_website = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    url = models.URLField(max_length=2000, null=True, blank=True)
    currency = models.CharField(max_length=3, default='USD')
    event_type = models.CharField(max_length=100, null=True, blank=True)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        venue_count = self.venues.count()
        if venue_count == 1:
            return f"{self.name} at {self.venues.first().name}"
        elif venue_count > 1:
            return f"{self.name} at {venue_count} venues"
        return f"{self.name}"

    class Meta:
        db_table = 'event'
        unique_together = ['source_event_id', 'source_website']
        indexes = [
            models.Index(fields=['internal_event_id']),
            models.Index(fields=['source_event_id', 'source_website']),
            models.Index(fields=['name']),
            models.Index(fields=['is_active']),
        ]


class EventVenue(models.Model):
    """Intermediate model for Event-Venue many-to-many relationship"""
    event_venue_key = models.AutoField(primary_key=True)
    event_id = models.ForeignKey(
        Event,
        on_delete=models.CASCADE,
        db_column='internal_event_id'
    )
    venue_id = models.ForeignKey(
        Venue,
        on_delete=models.CASCADE,
        db_column='internal_venue_id'
    )
    source_website = models.CharField(max_length=255)
    created_at = models.DateTimeField(default=timezone.now)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.event_id.name} at {self.venue_id.name}"

    class Meta:
        db_table = 'event_venue'
        unique_together = ['event_id', 'venue_id']
        indexes = [
            models.Index(fields=['event_id']),
            models.Index(fields=['venue_id']),
            models.Index(fields=['source_website']),
            models.Index(fields=['is_active']),
        ]


class Performance(models.Model):
    """Specific performance instances of events at venues"""
    internal_performance_id = models.CharField(
        max_length=200,
        primary_key=True,
        editable=False,
        help_text="Internal identifier for this performance"
    )
    event_id = models.ForeignKey(
        Event,
        on_delete=models.CASCADE,
        related_name='performances',
        db_column='internal_event_id'
    )
    venue_id = models.ForeignKey(
        Venue,
        on_delete=models.CASCADE,
        related_name='performances',
        db_column='internal_venue_id',
        help_text="Specific venue where this performance takes place"
    )
    source_performance_id = models.CharField(max_length=255, null=True, blank=True)
    source_website = models.CharField(max_length=255)
    performance_datetime_utc = models.DateTimeField()
    seat_map_url = models.URLField(max_length=2000, null=True, blank=True)
    map_width = models.IntegerField(null=True, blank=True)
    map_height = models.IntegerField(null=True, blank=True)


    pos_enabled = models.BooleanField(
        default=False,
        help_text="Enable POS (Point of Sale) integration for this performance"
    )
    pos_enabled_at = models.DateTimeField(
        null=True, blank=True,
        help_text="When POS was enabled for this performance"
    )
    pos_disabled_at = models.DateTimeField(
        null=True, blank=True,
        help_text="When POS was disabled for this performance"
    )

    pos_sync_enabled = models.BooleanField(
        default=False,
        help_text="Whether POS sync is enabled for this performance"
    )
    pos_sync_disabled_at = models.DateTimeField(
        null=True, blank=True,
        help_text="When POS sync was disabled for this performance"
    )
    pos_sync_disabled_by = models.ForeignKey(
        'auth.User',
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='disabled_performances',
        help_text="User who disabled POS sync for this performance"
    )
    pos_sync_reenabled_at = models.DateTimeField(
        null=True, blank=True,
        help_text="When POS sync was re-enabled for this performance"
    )

    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.event_id.name} at {self.venue_id.name} on {self.performance_datetime_utc}"

    class Meta:
        db_table = 'performance'
        unique_together = ['event_id', 'venue_id', 'performance_datetime_utc']
        indexes = [
            models.Index(fields=['event_id']),
            models.Index(fields=['venue_id']),
            models.Index(fields=['performance_datetime_utc']),
            models.Index(fields=['source_website']),
            models.Index(fields=['is_active']),
            models.Index(fields=['pos_enabled']),
            models.Index(fields=['pos_enabled_at']),
        ]


class Level(models.Model):
    """Physical levels within a performance venue (Orchestra, Mezzanine, Balcony)"""
    internal_level_id = models.CharField(
        max_length=200,
        primary_key=True,
        editable=False,
        help_text="Internal identifier for this level"
    )
    venue_id = models.ForeignKey(
        Venue,
        on_delete=models.CASCADE,
        related_name='levels',
        db_column='internal_venue_id',
        help_text="Venue this level belongs to"
    )
    source_level_id = models.CharField(max_length=255, null=True, blank=True)
    source_website = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    alias = models.CharField(
        max_length=255,
        default='',
        blank=True,
        help_text="User-friendly alias for the level name"
    )
    raw_name = models.CharField(max_length=255, null=True, blank=True)
    level_number = models.IntegerField(null=True, blank=True)
    display_order = models.IntegerField(default=0)
    level_type = models.CharField(max_length=100, null=True, blank=True)  # orchestra, mezzanine, balcony
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.name}"

    class Meta:
        db_table = 'level'
        unique_together = ['venue_id', 'source_level_id', 'source_website', 'name'] # Added venue_id to unique_together
        indexes = [
            models.Index(fields=['venue_id']),
            models.Index(fields=['name']),
            models.Index(fields=['alias']),
            models.Index(fields=['source_website']),
            models.Index(fields=['is_active']),
            models.Index(fields=['display_order']),
        ]

class PerformanceLevel(models.Model):
    """Intermediate model for Performance-Level many-to-many relationship"""
    performance = models.ForeignKey(
        Performance,
        on_delete=models.CASCADE,
        db_column='internal_performance_id'
    )
    level = models.ForeignKey(
        Level,
        on_delete=models.CASCADE,
        db_column='internal_level_id'
    )
    # Add any additional fields specific to this relationship, e.g., display_order for levels within a performance
    display_order = models.IntegerField(default=0)

    class Meta:
        db_table = 'performance_level'
        unique_together = ['performance', 'level']
        indexes = [
            models.Index(fields=['performance']),
            models.Index(fields=['level']),
            models.Index(fields=['display_order']),
        ]


class Zone(models.Model):
    """Pricing/organizational zones that can span across levels and sections"""

    VIEW_TYPE_CHOICES = [
        ('clear', 'Clear View'),
        ('partial', 'Partial View'),
        ('obstructed', 'Obstructed View'),
        ('side', 'Side View'),
        ('limited', 'Limited View'),
        ('excellent', 'Excellent View'),
        ('premium', 'Premium View'),
        ('standard', 'Standard View'),
    ]

    internal_zone_id = models.CharField(
        max_length=200,
        primary_key=True,
        editable=False,
        help_text="Internal identifier for this zone"
    )
    performance_id = models.ForeignKey(
        Performance,
        on_delete=models.CASCADE,
        related_name='zones',
        db_column='internal_performance_id'
    )
    source_zone_id = models.CharField(max_length=255, null=True, blank=True)
    source_website = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    raw_identifier = models.CharField(max_length=255, null=True, blank=True)
    zone_type = models.CharField(max_length=100, null=True, blank=True)  # vip, premium, standard
    color_code = models.CharField(max_length=7, null=True, blank=True)  # hex color
    view_type = models.CharField(
        max_length=20,
        choices=VIEW_TYPE_CHOICES,
        null=True,
        blank=True,
        help_text="View quality from this zone"
    )
    wheelchair_accessible = models.BooleanField(
        default=False,
        help_text="Whether this zone has wheelchair accessible seating"
    )
    miscellaneous = models.JSONField(
        default=dict,
        blank=True,
        help_text="Additional zone information in JSON format"
    )
    display_order = models.IntegerField(default=0)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.name} - {self.performance_id.event_id.name} at {self.performance_id.venue_id.name}"

    def get_view_type_display_with_icon(self):
        """Return view type with appropriate icon"""
        icons = {
            'clear': '👁️ Clear View',
            'partial': '👀 Partial View',
            'obstructed': '🚧 Obstructed View',
            'side': '↗️ Side View',
            'limited': '⚠️ Limited View',
            'excellent': '⭐ Excellent View',
            'premium': '💎 Premium View',
            'standard': '📍 Standard View',
        }
        return icons.get(self.view_type, self.get_view_type_display())

    def accessibility_status(self):
        """Return accessibility status with icon"""
        return "♿ Accessible" if self.wheelchair_accessible else "❌ Not Accessible"

    class Meta:
        db_table = 'zone'
        indexes = [
            models.Index(fields=['performance_id']),
            models.Index(fields=['name']),
            models.Index(fields=['source_website']),
            models.Index(fields=['is_active']),
            models.Index(fields=['display_order']),
            models.Index(fields=['view_type']),
            models.Index(fields=['wheelchair_accessible']),
        ]


class Section(models.Model):
    """Sections within levels"""
    level_id = models.ForeignKey(
        Level,
        on_delete=models.CASCADE,
        related_name='sections',
        db_column='internal_level_id'
    )
    internal_section_id = models.CharField(
        max_length=200,
        primary_key=True,
        editable=False,
        help_text="Internal identifier for this section"
    )
    source_section_id = models.CharField(max_length=255, null=True, blank=True)
    source_website = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    alias = models.CharField(
        max_length=255,
        default='',
        blank=True,
        help_text="User-friendly alias for the section name"
    )
    raw_name = models.CharField(max_length=255, null=True, blank=True)
    section_type = models.CharField(max_length=100, null=True, blank=True)  # regular, accessible, premium
    display_order = models.IntegerField(default=0)
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.name} in {self.level_id.name}"

    class Meta:
        db_table = 'section'
        indexes = [
            models.Index(fields=['level_id']),
            models.Index(fields=['name']),
            models.Index(fields=['alias']),
            models.Index(fields=['source_website']),
            models.Index(fields=['is_active']),
            models.Index(fields=['display_order']),
        ]


class Seat(models.Model):
    """Individual seats within sections, also assigned to zones"""

    STATUS_CHOICES = [
        ('available', 'Available'),
        ('sold', 'Sold'),
        ('reserved', 'Reserved'),
        ('blocked', 'Blocked'),
        ('unknown', 'Unknown'),
    ]

    internal_seat_id = models.CharField(
        max_length=200,
        primary_key=True,
        editable=False,
        help_text="Internal identifier for this seat"
    )
    section_id = models.ForeignKey(
        Section,
        on_delete=models.CASCADE,
        related_name='seats',
        db_column='internal_section_id'
    )
    zone_id = models.ForeignKey(
        Zone,
        on_delete=models.CASCADE,
        related_name='seats',
        db_column='internal_zone_id'
    )
    source_seat_id = models.CharField(max_length=255, null=True, blank=True)
    source_website = models.CharField(max_length=255)
    row_label = models.CharField(max_length=20)
    seat_number = models.CharField(max_length=20)
    seat_type = models.CharField(max_length=50, null=True, blank=True)
    x_coord = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    y_coord = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)

    # Current availability status (updated from latest scrape)
    current_status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='unknown',
        help_text="Current availability status from latest scrape"
    )
    current_price = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Current price from latest scrape"
    )
    current_fees = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Current fees from latest scrape"
    )
    last_updated = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When current status was last updated"
    )
    last_scrape_job = models.ForeignKey(
        'scrapers.ScrapeJob',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='current_seat_statuses',
        help_text="Reference to scrape job that provided current status"
    )

    pos_listing = models.ForeignKey(
        'scrapers.POSListing',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='seats',
        help_text="The POS listing this seat belongs to"
    )
    pos_ticket_id = models.CharField(
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
        help_text="The individual ticket ID from the POS system"
    )

    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    is_active = models.BooleanField(default=True)

    def __str__(self):
        return f"Row {self.row_label} Seat {self.seat_number}"

    def is_available(self):
        """Check if seat is currently available"""
        return self.current_status == 'available'

    def get_current_total_price(self):
        """Get total price including fees"""
        if self.current_price:
            fees = self.current_fees or 0
            return self.current_price + fees
        return None

    def get_status_display_with_icon(self):
        """Return status with appropriate icon"""
        icons = {
            'available': '✅ Available',
            'sold': '❌ Sold',
            'reserved': '🔒 Reserved',
            'blocked': '🚫 Blocked',
            'unknown': '❓ Unknown',
        }
        return icons.get(self.current_status, self.get_current_status_display())

    def get_location_hierarchy(self):
        """Get the full location hierarchy for this seat"""
        return f"{self.section_id.level_id.performance_id.venue_id.name} > {self.section_id.level_id.name} > {self.section_id.name}"

    def update_current_status(self, snapshot):
        """Update current status from a snapshot"""
        self.current_status = snapshot.status
        self.current_price = snapshot.price
        self.current_fees = snapshot.fees
        self.last_updated = snapshot.snapshot_time
        self.last_scrape_job = snapshot.scrape_job_key
        self.save()

    class Meta:
        db_table = 'seat'
        unique_together = ['section_id', 'row_label', 'seat_number']
        indexes = [
            models.Index(fields=['section_id']),
            models.Index(fields=['zone_id']),
            models.Index(fields=['row_label']),
            models.Index(fields=['seat_number']),
            models.Index(fields=['source_website']),
            models.Index(fields=['is_active']),
            models.Index(fields=['current_status']),
        ]


# SeatPack model has been moved to seat_packs.py for better organization
# and to include enhanced fields for seat pack lifecycle management

