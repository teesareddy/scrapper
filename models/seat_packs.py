import uuid
import time
from typing import Optional

from django.db import models
from django.utils import timezone
from django.core.exceptions import ValidationError
from django.contrib.auth import get_user_model
from datetime import timedelta

User = get_user_model()


class OptimizedSeatPackQuerySet(models.QuerySet):
    """Optimized queryset with pre-built efficient queries."""
    
    def for_pos_sync(self, performance_id: Optional[str] = None):
        """Optimized query for POS sync operations."""
        qs = self.filter(
            pack_status='active',
            pos_status='pending',
            synced_to_pos=False
        ).select_related(
            'performance', 'zone_id', 'pos_listing'
        ).order_by(
            'pos_sync_attempts',
            'created_at'
        )
        
        if performance_id:
            qs = qs.filter(performance=performance_id)
        
        return qs
    
    def for_dashboard(self, user_id: Optional[str] = None):
        """Optimized query for dashboard display."""
        qs = self.filter(
            pack_status='active'
        ).select_related(
            'performance__event',
            'performance__venue_id',
            'zone_id',
            'level',
            'manually_delisted_by',
            'manually_enabled_by'
        ).prefetch_related(
            'pos_listing'
        )
        
        if user_id:
            qs = qs.filter(
                models.Q(manually_delisted_by=user_id) |
                models.Q(manually_enabled_by=user_id)
            )
        
        return qs
    
    def needs_retry(self, max_attempts: int = 5):
        """Find packs that need retry for POS sync."""
        return self.filter(
            pos_status='failed',
            pos_sync_attempts__lt=max_attempts,
            synced_to_pos=False
        ).order_by(
            'pos_sync_attempts',
            'last_pos_sync_attempt'
        )
    
    def locked_by_process(self, process_id: str):
        """Find packs locked by a specific process."""
        return self.filter(
            locked_by=process_id
        ).order_by('locked_at')
    
    def stale_locks(self, minutes: int = 30):
        """Find stale locks for cleanup."""
        threshold = timezone.now() - timedelta(minutes=minutes)
        return self.filter(
            locked_at__lt=threshold
        )
    
    def active_system_packs(self):
        """Get all active packs in our system (regardless of POS status)."""
        return self.filter(pack_status='active')
    
    def performance_packs(self, performance_id: str):
        """Get all packs for a performance (including inactive ones for comparison)."""
        return self.filter(
            performance=performance_id,
            pack_status__in=['active', 'inactive']
        )
    
    def pending_pos_creation(self):
        """Packs that need to be created/updated in StubHub."""
        return self.filter(
            pack_status='active',
            pos_status='pending',
            synced_to_pos=False
        )
    
    def pending_pos_delisting(self):
        """Packs that need to be delisted from StubHub."""
        return self.filter(
            pack_status='inactive',
            pos_status='active',
            synced_to_pos=False
        )
    
    def manually_delistable(self):
        """Packs that can be manually delisted."""
        return self.filter(
            pack_status='active',
            pos_status__in=['active', 'pending'],
            pack_state__in=['create', 'split', 'merge', 'shrink']  # Not already delisted
        )
    
    def manually_reactivatable(self):
        """Packs that can be manually re-enabled."""
        return self.filter(
            pack_status='inactive',
            pack_state='delist',
            delist_reason='manual_delist'
        )
    
    def recent_manual_delists(self, days: int = 7):
        """Recently manually delisted packs."""
        threshold = timezone.now() - timedelta(days=days)
        return self.filter(
            delist_reason='manual_delist',
            manually_delisted_at__gte=threshold
        ).select_related('manually_delisted_by')
    
    def pending_rollbacks(self):
        """Packs with pending POS operations (for rollback)."""
        return self.filter(
            pos_operation_id__isnull=False,
            pos_operation_status='pending'
        )
    
    def failed_operations(self):
        """Packs with failed POS operations."""
        return self.filter(
            pos_operation_id__isnull=False,
            pos_operation_status='failed'
        )
    
    def child_packs(self, parent_pack_id: str):
        """Packs that were transformed from a specific parent."""
        return self.filter(
            source_pack_ids__contains=[parent_pack_id]
        )


class SeatPackManager(models.Manager):
    """Enhanced manager for SeatPack with optimized queries."""
    
    def get_queryset(self):
        return OptimizedSeatPackQuerySet(self.model, using=self._db)
    
    def for_pos_sync(self, performance_id: Optional[str] = None):
        return self.get_queryset().for_pos_sync(performance_id)
    
    def for_dashboard(self, user_id: Optional[str] = None):
        return self.get_queryset().for_dashboard(user_id)
    
    def needs_retry(self, max_attempts: int = 5):
        return self.get_queryset().needs_retry(max_attempts)
    
    def locked_by_process(self, process_id: str):
        return self.get_queryset().locked_by_process(process_id)
    
    def stale_locks(self, minutes: int = 30):
        return self.get_queryset().stale_locks(minutes)

class SeatPack(models.Model):
    """
    Represents a logical grouping of contiguous seats that can be sold together.
    Includes fields for tracking its lifecycle through the diffing algorithm.
    
    Note: Uses internal_pack_id to maintain compatibility with existing database schema,
    but stores the deterministic pack_id as the value for Phase 2 synchronization.
    """
    internal_pack_id = models.CharField(max_length=255, primary_key=True, help_text="Deterministic, hashed pack ID")

    # Foreign Keys for context
    performance = models.ForeignKey('Performance', on_delete=models.CASCADE, related_name='seat_packs', null=True, blank=True, db_column='internal_performance_id')
    event = models.ForeignKey('Event', on_delete=models.CASCADE, related_name='seat_packs', null=True, blank=True, db_column='internal_event_id')
    level = models.ForeignKey('Level', on_delete=models.CASCADE, related_name='seat_packs', null=True, blank=True, db_column='internal_level_id')
    section = models.ForeignKey('Section', on_delete=models.CASCADE, related_name='seat_packs', null=True, blank=True, db_column='internal_section_id')
    
    # Core Pack Data (mirroring SeatPackData dataclass)  
    zone_id = models.ForeignKey('Zone', on_delete=models.CASCADE, db_column='internal_zone_id', related_name='seat_packs')
    scrape_job_key = models.ForeignKey('ScrapeJob', on_delete=models.CASCADE, db_column='scrape_job_key', related_name='seat_packs')
    source_pack_id = models.CharField(max_length=255, blank=True, null=True)
    source_website = models.CharField(max_length=255)
    row_label = models.CharField(max_length=20)
    start_seat_number = models.CharField(max_length=20)
    end_seat_number = models.CharField(max_length=20)
    pack_size = models.IntegerField()
    pack_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    total_price = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    seat_keys = models.JSONField(default=list, help_text="JSON array of seat IDs in this pack")
    
    # ===== TIMESTAMPS =====
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    
    # ===== FOUR-DIMENSIONAL ARCHITECTURE =====
    
    # DIMENSION 1: PACK STATUS - Current operational availability
    PACK_STATUS_CHOICES = [
        ('active', 'Active'),     # Pack is currently available in our system
        ('inactive', 'Inactive'), # Pack is not available in our system
    ]
    pack_status = models.CharField(
        max_length=20,
        choices=PACK_STATUS_CHOICES,
        default='active',
        db_index=True,
        help_text="Current operational status in our system. Controls dashboard visibility and scraper logic."
    )
    
    # DIMENSION 2: POS STATUS - Current state in StubHub inventory system
    POS_STATUS_CHOICES = [
        ('active', 'Active'),              # Listed and available in StubHub
        ('inactive', 'Inactive'),          # Delisted from StubHub
        ('pending', 'Pending'),            # Needs to be created/updated in StubHub
        ('failed', 'Failed'),              # POS sync failed
        ('suspended', 'Suspended'),        # StubHub suspended this listing
        ('under_review', 'Under Review'),  # StubHub is reviewing this listing
    ]
    pos_status = models.CharField(
        max_length=20,
        choices=POS_STATUS_CHOICES,
        default='pending',
        db_index=True,
        help_text="Current status in StubHub inventory system. Controls POS sync behavior."
    )
    
    # DIMENSION 3: PACK STATE - Lifecycle state (how did this pack come to be?)
    PACK_STATE_CHOICES = [
        ('create', 'Create'),           # Originally created pack
        ('split', 'Split'),             # Created by splitting another pack
        ('merge', 'Merge'),             # Created by merging other packs
        ('shrink', 'Shrink'),           # Created by shrinking another pack
        ('delist', 'Delist'),           # Explicitly delisted
        ('transformed', 'Transformed'), # Transformed into other packs (terminal state)
    ]
    pack_state = models.CharField(
        max_length=20,
        choices=PACK_STATE_CHOICES,
        default='create',
        db_index=True,
        help_text="Lifecycle state indicating how this pack came to be or what happened to it."
    )
    
    # DIMENSION 4: DELIST REASON - Why was this pack delisted?
    DELIST_REASON_CHOICES = [
        ('manual_delist', 'Manual Delist'),                    # User manually delisted
        ('performance_disabled', 'Performance POS Disabled'),  # Performance-level POS disable
        ('transformed', 'Transformed'),                        # Pack was transformed
        ('vanished', 'Vanished'),                             # Pack disappeared from source
        ('structure_change', 'Structure Change'),              # Venue structure changed
        ('admin_hold', 'Admin Hold'),                         # Administrative hold
    ]
    delist_reason = models.CharField(
        max_length=30,
        choices=DELIST_REASON_CHOICES,
        null=True, blank=True,
        db_index=True,
        help_text="Specific reason for delisting. Required when pack_state is 'delist' or 'transformed'."
    )

    manually_delisted = models.BooleanField(default=False)


    source_pack_ids = models.JSONField(
        default=list, 
        blank=True, 
        help_text="JSON array of parent pack IDs this pack originated from"
    )

    # ===== POS SYNC TRACKING =====
    synced_to_pos = models.BooleanField(
        default=False,
        db_index=True,
        help_text="Whether this pack's current state is synchronized with StubHub."
    )
    
    pos_sync_attempts = models.IntegerField(
        default=0,
        help_text="Number of times POS sync has been attempted for this pack."
    )
    
    last_pos_sync_attempt = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp of last POS sync attempt."
    )
    
    pos_sync_error = models.TextField(
        null=True, blank=True,
        help_text="Last error message from POS sync attempt."
    )
    
    # ===== AUDIT TRAIL =====
    # Manual Actions Audit Trail
    manually_delisted_by = models.ForeignKey(
        User,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='manually_delisted_packs',
        help_text="User who manually delisted this pack."
    )
    
    manually_delisted_at = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp when this pack was manually delisted."
    )
    
    manually_delisted_reason = models.TextField(
        null=True, blank=True,
        help_text="User-provided reason for manual delisting."
    )
    
    manually_enabled_by = models.ForeignKey(
        User,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name='manually_enabled_packs',
        help_text="User who manually re-enabled this pack."
    )
    
    manually_enabled_at = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp when this pack was manually re-enabled."
    )
    
    # ===== CONCURRENCY CONTROL =====
    # Optimistic locking for concurrent operations
    version = models.IntegerField(
        default=0,
        help_text="Version number for optimistic locking. Incremented on each update."
    )
    
    locked_by = models.CharField(
        max_length=100,
        null=True, blank=True,
        help_text="Process ID or identifier that currently has this pack locked."
    )
    
    locked_at = models.DateTimeField(
        null=True, blank=True,
        help_text="Timestamp when this pack was locked."
    )
    
    # ===== ROLLBACK SUPPORT =====
    # Transaction rollback for partial POS sync failures
    pos_operation_id = models.CharField(
        max_length=100,
        null=True, blank=True,
        help_text="Unique identifier for POS operation (for rollback if needed)."
    )
    
    pos_operation_status = models.CharField(
        max_length=20,
        null=True, blank=True,
        help_text="Status of current POS operation (for rollback tracking)."
    )
    
    # POS Integration
    pos_listing = models.ForeignKey(
        'POSListing',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='seat_packs',
        help_text="The POS listing this seat pack belongs to"
    )
    
    # Timestamps
    # updated_at = models.DateTimeField(auto_now=True)

    # Optional: Link to ScrapeJob if needed for auditing specific scrapes
    # scrape_job = models.ForeignKey(
    #     'ScrapeJob', 
    #     on_delete=models.SET_NULL, 
    #     null=True, blank=True, 
    #     related_name='seat_packs'
    # )

    def clean(self):
        """Validation rules for the four-dimensional model."""
        super().clean()
        
        # Validate state combinations
        if self.pack_state == 'transformed' and self.pack_status == 'active':
            raise ValidationError("Transformed packs must have pack_status='inactive'")
        
        # Validate delist reason requirements
        if self.pack_state in ['delist', 'transformed'] and not self.delist_reason:
            raise ValidationError(f"delist_reason is required when pack_state is '{self.pack_state}'")
        
        # Validate audit trail consistency
        # if self.delist_reason == 'manual_delist' and not self.manually_delisted_by:
        #     raise ValidationError("manually_delisted_by is required for manual delists")
        
        # Validate POS status consistency
        if self.pack_status == 'inactive' and self.pos_status == 'active':
            raise ValidationError("Inactive packs cannot have active POS status")
    
    def save(self, *args, **kwargs):
        """Enhanced save method with version control and validation."""
        # Increment version for optimistic locking
        if self.pk:
            self.version += 1
        
        # Validate before saving
        self.clean()
        
        super().save(*args, **kwargs)
    
    # Use custom manager for optimized queries
    objects = SeatPackManager()
    
    def __str__(self):
        return f"Pack {self.internal_pack_id} - {self.pack_status}/{self.pos_status}/{self.pack_state}"

    class Meta:
        db_table = 'seat_pack'
        verbose_name = 'Seat Pack'
        verbose_name_plural = 'Seat Packs'
        
        # Strategic indexes for performance optimization
        indexes = [
            # Legacy indexes (keep for backward compatibility)
            models.Index(fields=['zone_id'], name='seat_pack_interna_71fab4_idx'),
            models.Index(fields=['scrape_job_key'], name='seat_pack_scrape__f407a4_idx'),
            models.Index(fields=['row_label'], name='seat_pack_row_lab_f155d4_idx'),
            models.Index(fields=['pack_size'], name='seat_pack_pack_si_362e47_idx'),
            models.Index(fields=['source_website'], name='seat_pack_source__d7a95a_idx'),
            models.Index(fields=['created_at'], name='seat_pack_created_ee9c70_idx'),
            
            # ===== PRIMARY OPERATION INDEXES =====
            # Most common POS sync query
            models.Index(
                fields=['pack_status', 'pos_status', 'synced_to_pos'],
                name='sp_pos_sync_idx'
            ),
            
            # Scraper transformation queries
            models.Index(
                fields=['performance', 'pack_status'],
                name='sp_perf_status_idx'
            ),
            
            # POS sync with performance filter
            models.Index(
                fields=['performance', 'pos_status', 'synced_to_pos'],
                name='sp_perf_pos_sync_idx'
            ),
            
            # ===== AUDIT AND MONITORING INDEXES =====
            # Manual operations tracking
            models.Index(
                fields=['delist_reason', 'manually_delisted_at'],
                name='sp_manual_audit_idx'
            ),
            
            # State transition queries
            models.Index(
                fields=['pack_state', 'delist_reason'],
                name='sp_state_reason_idx'
            ),
            
            # ===== PERFORMANCE OPTIMIZATION INDEXES =====
            # Retry logic queries
            models.Index(
                fields=['pos_sync_attempts', 'last_pos_sync_attempt'],
                name='sp_retry_idx'
            ),
            
            # Time-based queries
            models.Index(
                fields=['created_at', 'updated_at'],
                name='sp_time_idx'
            ),
            
            # ===== CONCURRENCY CONTROL INDEXES =====
            # Lock management
            models.Index(
                fields=['locked_by', 'locked_at'],
                name='sp_lock_idx'
            ),
            
            # Version control
            models.Index(
                fields=['version'],
                name='sp_version_idx'
            ),
            
            # ===== COMPOSITE BUSINESS LOGIC INDEXES =====
            # Complex dashboard queries
            models.Index(
                fields=['pack_status', 'pack_state', 'pos_status'],
                name='sp_dashboard_idx'
            ),
            
            # Rollback operation tracking
            models.Index(
                fields=['pos_operation_id', 'pos_operation_status'],
                name='sp_rollback_idx'
            ),
            
            # Legacy compatibility indexes
            # models.Index(fields=['is_active'], name='seat_pack_is_acti_sync_idx'),
            models.Index(fields=['manually_delisted'], name='seat_pack_manual_delist_idx'),

            models.Index(fields=['updated_at'], name='seat_pack_updated_at_idx'),
            # models.Index(fields=['is_active', 'manually_delisted'], name='seat_pack_sync_status_idx'),
        ]
        
        # Unique constraints
        constraints = [
            models.UniqueConstraint(
                fields=['internal_pack_id'],
                name='sp_unique_pack_id'
            ),
            
            # Prevent duplicate locks
            models.UniqueConstraint(
                fields=['locked_by'],
                condition=models.Q(locked_by__isnull=False),
                name='sp_unique_lock_per_process'
            ),
        ]
        
        unique_together = [('zone_id', 'row_label', 'start_seat_number', 'end_seat_number', 'scrape_job_key', 'level', 'section')]
