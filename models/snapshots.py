# scrapers/models/snapshots.py
"""
Snapshot and historical data models for tracking changes over time.
These models capture point-in-time data for pricing and availability.
"""

from django.db import models
from django.utils import timezone
import uuid


class SeatSnapshot(models.Model):
    """Individual seat availability and pricing snapshots"""
    snapshot_id = models.UUIDField(primary_key=True, default=uuid.uuid4)
    scrape_job_key = models.ForeignKey(
        'scrapers.ScrapeJob',
        on_delete=models.CASCADE,
        related_name='seat_snapshots',
        db_column='scrape_job_key'
    )
    seat_id = models.ForeignKey(
        'scrapers.Seat',
        on_delete=models.CASCADE,
        related_name='snapshots',
        db_column='internal_seat_id'
    )
    status = models.CharField(max_length=50)  # available, sold, reserved, blocked
    price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    fees = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    raw_status_text = models.CharField(max_length=255, null=True, blank=True)
    raw_price_text = models.CharField(max_length=255, null=True, blank=True)
    raw_fees_text = models.CharField(max_length=255, null=True, blank=True)
    snapshot_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"Seat {self.seat_id} - {self.status} at {self.snapshot_time}"

    @property
    def total_price(self):
        """Calculate total price including fees"""
        if self.price:
            fees = self.fees or 0
            return self.price + fees
        return None

    def price_changed_from_previous(self):
        """Check if price changed from previous snapshot"""
        previous = SeatSnapshot.objects.filter(
            seat_id=self.seat_id,
            snapshot_time__lt=self.snapshot_time
        ).order_by('-snapshot_time').first()
        
        if previous:
            return self.price != previous.price
        return False

    class Meta:
        db_table = 'seat_snapshot'
        indexes = [
            models.Index(fields=['scrape_job_key']),
            models.Index(fields=['seat_id']),
            models.Index(fields=['status']),
            models.Index(fields=['price']),
            models.Index(fields=['snapshot_time']),
            models.Index(fields=['seat_id', 'snapshot_time']),  # For time series queries
        ]


class LevelPriceSnapshot(models.Model):
    """Level pricing snapshots over time"""
    snapshot_key = models.AutoField(primary_key=True)
    scrape_job_key = models.ForeignKey(
        'scrapers.ScrapeJob',
        on_delete=models.CASCADE,
        related_name='level_price_snapshots',
        db_column='scrape_job_key'
    )
    level_id = models.ForeignKey(
        'scrapers.Level',
        on_delete=models.CASCADE,
        related_name='price_snapshots',
        db_column='internal_level_id'
    )
    min_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    max_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    avg_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    available_seats = models.IntegerField(null=True, blank=True)
    total_seats = models.IntegerField(null=True, blank=True)
    sold_seats = models.IntegerField(null=True, blank=True)
    raw_price_text = models.TextField(null=True, blank=True)
    raw_availability_text = models.TextField(null=True, blank=True)
    snapshot_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"{self.level_id.name} - ${self.min_price}-${self.max_price} at {self.snapshot_time}"

    @property
    def availability_percentage(self):
        """Calculate availability percentage"""
        if self.total_seats and self.total_seats > 0:
            return (self.available_seats / self.total_seats) * 100
        return None

    @property
    def price_range_display(self):
        """Human-readable price range"""
        if self.min_price and self.max_price:
            if self.min_price == self.max_price:
                return f"${self.min_price}"
            return f"${self.min_price} - ${self.max_price}"
        elif self.min_price:
            return f"From ${self.min_price}"
        elif self.max_price:
            return f"Up to ${self.max_price}"
        return "Price unavailable"

    class Meta:
        db_table = 'level_price_snapshot'
        indexes = [
            models.Index(fields=['scrape_job_key']),
            models.Index(fields=['level_id']),
            models.Index(fields=['snapshot_time']),
            models.Index(fields=['level_id', 'snapshot_time']),  # For time series queries
            models.Index(fields=['min_price']),
            models.Index(fields=['available_seats']),
        ]


class ZonePriceSnapshot(models.Model):
    """Zone pricing snapshots over time"""
    snapshot_key = models.AutoField(primary_key=True)
    scrape_job_key = models.ForeignKey(
        'scrapers.ScrapeJob',
        on_delete=models.CASCADE,
        related_name='zone_price_snapshots',
        db_column='scrape_job_key'
    )
    zone_id = models.ForeignKey(
        'scrapers.Zone',
        on_delete=models.CASCADE,
        related_name='price_snapshots',
        db_column='internal_zone_id'
    )
    min_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    max_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    avg_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    available_seats = models.IntegerField(null=True, blank=True)
    total_seats = models.IntegerField(null=True, blank=True)
    sold_seats = models.IntegerField(null=True, blank=True)
    raw_price_text = models.TextField(null=True, blank=True)
    raw_availability_text = models.TextField(null=True, blank=True)
    snapshot_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"{self.zone_id.name} - ${self.min_price}-${self.max_price} at {self.snapshot_time}"

    @property
    def availability_percentage(self):
        """Calculate availability percentage"""
        if self.total_seats and self.total_seats > 0:
            return (self.available_seats / self.total_seats) * 100
        return None

    @property
    def price_range_display(self):
        """Human-readable price range"""
        if self.min_price and self.max_price:
            if self.min_price == self.max_price:
                return f"${self.min_price}"
            return f"${self.min_price} - ${self.max_price}"
        elif self.min_price:
            return f"From ${self.min_price}"
        elif self.max_price:
            return f"Up to ${self.max_price}"
        return "Price unavailable"

    def get_demand_indicator(self):
        """Get demand indicator based on availability"""
        if self.availability_percentage is not None:
            if self.availability_percentage >= 80:
                return "🟢 Low Demand"
            elif self.availability_percentage >= 50:
                return "🟡 Medium Demand"
            elif self.availability_percentage >= 20:
                return "🟠 High Demand"
            else:
                return "🔴 Very High Demand"
        return "❓ Unknown"

    class Meta:
        db_table = 'zone_price_snapshot'
        indexes = [
            models.Index(fields=['scrape_job_key']),
            models.Index(fields=['zone_id']),
            models.Index(fields=['snapshot_time']),
            models.Index(fields=['zone_id', 'snapshot_time']),  # For time series queries
            models.Index(fields=['min_price']),
            models.Index(fields=['available_seats']),
        ]


class SectionPriceSnapshot(models.Model):
    """Section pricing snapshots over time"""
    snapshot_key = models.AutoField(primary_key=True)
    scrape_job_key = models.ForeignKey(
        'scrapers.ScrapeJob',
        on_delete=models.CASCADE,
        related_name='section_price_snapshots',
        db_column='scrape_job_key'
    )
    section_id = models.ForeignKey(
        'scrapers.Section',
        on_delete=models.CASCADE,
        related_name='price_snapshots',
        db_column='internal_section_id'
    )
    min_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    max_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    avg_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    available_seats = models.IntegerField(null=True, blank=True)
    total_seats = models.IntegerField(null=True, blank=True)
    sold_seats = models.IntegerField(null=True, blank=True)
    raw_price_text = models.TextField(null=True, blank=True)
    raw_availability_text = models.TextField(null=True, blank=True)
    snapshot_time = models.DateTimeField(default=timezone.now)

    def __str__(self):
        return f"{self.section_id.name} - ${self.min_price}-${self.max_price} at {self.snapshot_time}"

    @property
    def availability_percentage(self):
        """Calculate availability percentage"""
        if self.total_seats and self.total_seats > 0:
            return (self.available_seats / self.total_seats) * 100
        return None

    @property
    def price_range_display(self):
        """Human-readable price range"""
        if self.min_price and self.max_price:
            if self.min_price == self.max_price:
                return f"${self.min_price}"
            return f"${self.min_price} - ${self.max_price}"
        elif self.min_price:
            return f"From ${self.min_price}"
        elif self.max_price:
            return f"Up to ${self.max_price}"
        return "Price unavailable"

    def get_best_value_indicator(self):
        """Get value indicator based on price and availability"""
        if self.avg_price and self.availability_percentage is not None:
            # This is a simple heuristic - can be made more sophisticated
            if self.availability_percentage > 70 and self.avg_price < 100:
                return "💚 Great Value"
            elif self.availability_percentage > 50 and self.avg_price < 200:
                return "💛 Good Value"
            elif self.availability_percentage > 20:
                return "🧡 Fair Value"
            else:
                return "❤️ Premium Price"
        return "❓ Unknown"

    class Meta:
        db_table = 'section_price_snapshot'
        indexes = [
            models.Index(fields=['scrape_job_key']),
            models.Index(fields=['section_id']),
            models.Index(fields=['snapshot_time']),
            models.Index(fields=['section_id', 'snapshot_time']),  # For time series queries
            models.Index(fields=['min_price']),
            models.Index(fields=['available_seats']),
        ]