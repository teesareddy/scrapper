# scrapers/models/pos.py
from django.db import models
from django.utils import timezone
from django.contrib.auth import get_user_model

User = get_user_model()
from .base import Performance

class POSListing(models.Model):
    """Represents an active inventory listing in the POS system."""
    pos_listing_id = models.AutoField(primary_key=True)
    performance = models.ForeignKey(
        Performance,
        on_delete=models.CASCADE,
        related_name='pos_listings',
        db_column='internal_performance_id'
    )
    # The master inventory ID from the POS for a block of tickets
    pos_inventory_id = models.CharField(max_length=255, unique=True, db_index=True)
    # StubHub API returned inventory ID
    stubhub_inventory_id = models.CharField(
        max_length=255, 
        null=True, 
        blank=True, 
        unique=True,
        help_text="Inventory ID returned from StubHub POS API"
    )
    status = models.CharField(
        max_length=20,
        choices=[('ACTIVE', 'Active'), ('INACTIVE', 'Inactive'), ('SPLIT', 'Split')],
        default='ACTIVE',
        db_index=True
    )
    
    # Admin hold tracking fields
    admin_hold_applied = models.BooleanField(
        default=False,
        help_text="Whether an admin hold has been applied to this StubHub inventory"
    )
    admin_hold_date = models.DateTimeField(
        null=True,
        blank=True,
        help_text="When the admin hold was applied"
    )
    admin_hold_reason = models.TextField(
        null=True,
        blank=True,
        help_text="Reason for applying the admin hold"
    )
    
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"POS Inventory {self.pos_inventory_id} for {self.performance.event_id.name}"

    class Meta:
        db_table = 'pos_listing'


class FailedRollback(models.Model):
    """Model for tracking failed rollback operations requiring manual intervention."""
    
    failed_rollback_id = models.AutoField(primary_key=True)
    operation_id = models.CharField(
        max_length=100,
        help_text="Unique identifier for the failed POS operation"
    )
    action_type = models.CharField(
        max_length=50,
        help_text="Type of rollback action that failed (e.g., 'delete_stubhub_inventory')"
    )
    action_data = models.JSONField(
        help_text="Data required to perform the rollback action"
    )
    error_message = models.TextField(
        help_text="Error message from the failed rollback attempt"
    )
    
    # Resolution tracking
    resolved_at = models.DateTimeField(
        null=True, blank=True,
        help_text="When this rollback was manually resolved"
    )
    resolved_by = models.ForeignKey(
        User,
        null=True, blank=True,
        on_delete=models.SET_NULL,
        help_text="User who manually resolved this rollback"
    )
    resolution_notes = models.TextField(
        null=True, blank=True,
        help_text="Notes about how this rollback was resolved"
    )
    
    created_at = models.DateTimeField(default=timezone.now)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        status = "Resolved" if self.resolved_at else "Pending"
        return f"Failed Rollback {self.operation_id} - {self.action_type} ({status})"
    
    class Meta:
        db_table = 'failed_rollback'
        indexes = [
            models.Index(fields=['operation_id']),
            models.Index(fields=['action_type']),
            models.Index(fields=['resolved_at']),
            models.Index(fields=['created_at']),
        ]
