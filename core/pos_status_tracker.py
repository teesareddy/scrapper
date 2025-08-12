"""
POS Status Tracker for Enhanced Database Updates and Event Messages

This module handles enhanced status tracking, database updates, and event messages
for the POS sync workflow with user-friendly notifications.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from django.db import transaction
from django.utils import timezone

from ..models.seat_packs import SeatPack
from .seat_pack_comparator import SeatPackComparison
from .pos_sync_service import POSSyncResult
from .pos_config_handler import POSConfiguration

logger = logging.getLogger(__name__)


class POSStatusUpdate:
    """Container for POS status update information"""
    
    def __init__(self):
        self.performance_id: str = ""
        self.source_website: str = ""
        self.operation_id: str = ""
        self.operation_type: str = ""  # 'sync', 'push', 'delist'
        self.timestamp: datetime = timezone.now()
        
        # Status counts
        self.total_packs: int = 0
        self.active_packs: int = 0
        self.inactive_packs: int = 0
        self.pending_sync_packs: int = 0
        self.failed_sync_packs: int = 0
        
        # Operation results
        self.new_packs_created: int = 0
        self.packs_updated: int = 0
        self.packs_delisted: int = 0
        self.sync_errors: int = 0
        
        # User-friendly messages
        self.summary_message: str = ""
        self.detailed_messages: List[str] = []
        self.warning_messages: List[str] = []
        self.error_messages: List[str] = []
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'performance_id': self.performance_id,
            'source_website': self.source_website,
            'operation_id': self.operation_id,
            'operation_type': self.operation_type,
            'timestamp': self.timestamp.isoformat(),
            'status_counts': {
                'total_packs': self.total_packs,
                'active_packs': self.active_packs,
                'inactive_packs': self.inactive_packs,
                'pending_sync_packs': self.pending_sync_packs,
                'failed_sync_packs': self.failed_sync_packs
            },
            'operation_results': {
                'new_packs_created': self.new_packs_created,
                'packs_updated': self.packs_updated,
                'packs_delisted': self.packs_delisted,
                'sync_errors': self.sync_errors
            },
            'messages': {
                'summary': self.summary_message,
                'detailed': self.detailed_messages,
                'warnings': self.warning_messages,
                'errors': self.error_messages
            }
        }


class POSStatusTracker:
    """
    Tracks POS status changes and generates user-friendly event messages
    """
    
    def __init__(self, performance_id: str, source_website: str, enriched_data: Dict[str, Any]):
        """
        Initialize the POS status tracker
        
        Args:
            performance_id: Internal performance ID
            source_website: Source website identifier
            enriched_data: Enriched data from NestJS for context
        """
        self.performance_id = performance_id
        self.source_website = source_website
        self.enriched_data = enriched_data
        
        # Extract user-friendly names from enriched data
        event_info = enriched_data.get('event', {})
        venue_info = enriched_data.get('venue', {})
        performance_info = enriched_data.get('performance', {})
        
        self.event_name = event_info.get('name', 'Unknown Event')
        self.venue_name = venue_info.get('name', 'Unknown Venue')
        self.performance_date = performance_info.get('event_date', 'Unknown Date')
        
    def track_seat_pack_generation(
        self, 
        comparison: SeatPackComparison,
        save_results: Dict[str, Any]
    ) -> POSStatusUpdate:
        """
        Track seat pack generation and comparison results
        
        Args:
            comparison: SeatPackComparison with categorized results
            save_results: Results from seat pack save operation
            
        Returns:
            POSStatusUpdate with generation tracking information
        """
        logger.info(f"Tracking seat pack generation for performance {self.performance_id}")
        
        update = POSStatusUpdate()
        update.performance_id = self.performance_id
        update.source_website = self.source_website
        update.operation_type = 'generation'
        update.operation_id = save_results.get('operation_id', 'unknown')
        
        # Extract counts from comparison and save results
        update.new_packs_created = save_results.get('saved_count', 0)
        update.packs_updated = save_results.get('removed_count', 0)
        update.total_packs = len(comparison.new_packs) + len(comparison.unchanged_packs)
        
        # Generate user-friendly messages
        self._generate_generation_messages(update, comparison, save_results)
        
        # Update database status
        self._update_generation_status(update)
        
        return update
    
    def track_pos_sync_operation(
        self, 
        sync_result: POSSyncResult,
        config: POSConfiguration
    ) -> POSStatusUpdate:
        """
        Track POS sync operation results
        
        Args:
            sync_result: POSSyncResult with sync operation details
            config: POSConfiguration used for the sync
            
        Returns:
            POSStatusUpdate with sync tracking information
        """
        logger.info(f"Tracking POS sync operation {sync_result.operation_id}")
        
        update = POSStatusUpdate()
        update.performance_id = self.performance_id
        update.source_website = self.source_website
        update.operation_type = 'pos_sync'
        update.operation_id = sync_result.operation_id
        
        # Extract counts from sync result
        update.packs_updated = sync_result.pushed_count
        update.packs_delisted = sync_result.delisted_count
        update.sync_errors = sync_result.failed_count
        update.total_packs = sync_result.processed_count
        
        # Generate user-friendly messages
        self._generate_sync_messages(update, sync_result, config)
        
        # Update database status
        self._update_sync_status(update, sync_result)
        
        return update
    
    def get_current_performance_status(self) -> POSStatusUpdate:
        """
        Get current status of all seat packs for the performance
        
        Returns:
            POSStatusUpdate with current performance status
        """
        logger.info(f"Getting current performance status for {self.performance_id}")
        
        update = POSStatusUpdate()
        update.performance_id = self.performance_id
        update.source_website = self.source_website
        update.operation_type = 'status_check'
        
        # Query current pack status
        packs = SeatPack.objects.filter(
            performance=self.performance_id
        ).values('pack_status', 'pos_status').distinct()
        
        # Count packs by status
        for pack_data in packs:
            update.total_packs += 1
            
            if pack_data['pack_status'] == 'active':
                update.active_packs += 1
            else:
                update.inactive_packs += 1
            
            if pack_data['pos_status'] == 'pending':
                update.pending_sync_packs += 1
            elif pack_data['pos_status'] == 'failed':
                update.failed_sync_packs += 1
        
        # Generate status messages
        self._generate_status_messages(update)
        
        return update
    
    def _generate_generation_messages(
        self, 
        update: POSStatusUpdate, 
        comparison: SeatPackComparison,
        save_results: Dict[str, Any]
    ):
        """Generate user-friendly messages for seat pack generation"""
        
        # Summary message
        if update.new_packs_created > 0:
            update.summary_message = (
                f"Generated {update.new_packs_created} new seat packs for "
                f"{self.event_name} at {self.venue_name}"
            )
        else:
            update.summary_message = (
                f"No new seat packs generated for {self.event_name} at {self.venue_name}"
            )
        
        # Detailed messages
        if comparison.new_packs:
            update.detailed_messages.append(
                f"✓ Created {len(comparison.new_packs)} new seat pack(s)"
            )
        
        if comparison.unchanged_packs:
            update.detailed_messages.append(
                f"→ {len(comparison.unchanged_packs)} seat pack(s) unchanged"
            )
        
        if comparison.removed_packs:
            update.detailed_messages.append(
                f"✗ {len(comparison.removed_packs)} seat pack(s) removed/delisted"
            )
        
        if comparison.transformations:
            update.detailed_messages.append(
                f"🔄 {len(comparison.transformations)} pack transformation(s) detected"
            )
        
        # Warning messages
        if save_results.get('errors'):
            update.warning_messages.extend([
                f"⚠️ Generation warning: {error}" 
                for error in save_results['errors']
            ])
        
        # Error messages
        if save_results.get('failed_count', 0) > 0:
            update.error_messages.append(
                f"❌ Failed to save {save_results['failed_count']} seat pack(s)"
            )
    
    def _generate_sync_messages(
        self, 
        update: POSStatusUpdate, 
        sync_result: POSSyncResult,
        config: POSConfiguration
    ):
        """Generate user-friendly messages for POS sync operations"""
        
        # Summary message based on sync mode
        if not config.pos_enabled:
            update.summary_message = (
                f"POS sync disabled for {self.event_name} at {self.venue_name}"
            )
        elif config.sync_mode == 'immediate':
            if sync_result.success:
                update.summary_message = (
                    f"Successfully synced {sync_result.processed_count} seat pack(s) "
                    f"with StubHub for {self.event_name}"
                )
            else:
                update.summary_message = (
                    f"POS sync completed with {sync_result.failed_count} error(s) "
                    f"for {self.event_name}"
                )
        else:  # on_demand
            update.summary_message = (
                f"Marked {sync_result.processed_count} seat pack(s) for POS sync "
                f"for {self.event_name}"
            )
        
        # Detailed messages
        if sync_result.pushed_count > 0:
            action = "pushed to" if config.sync_mode == 'immediate' else "marked for push to"
            update.detailed_messages.append(
                f"📤 {sync_result.pushed_count} seat pack(s) {action} StubHub"
            )
        
        if sync_result.delisted_count > 0:
            action = "delisted from" if config.sync_mode == 'immediate' else "marked for delist from"
            update.detailed_messages.append(
                f"📥 {sync_result.delisted_count} seat pack(s) {action} StubHub"
            )
        
        # Warning messages
        if config.admin_hold_enabled:
            update.warning_messages.append(
                f"⚠️ Admin hold active: {config.admin_hold_reason}"
            )
        
        if sync_result.errors:
            update.warning_messages.extend([
                f"⚠️ Sync warning: {error}" 
                for error in sync_result.errors[:5]  # Limit to first 5 errors
            ])
        
        # Error messages
        if sync_result.failed_count > 0:
            update.error_messages.append(
                f"❌ {sync_result.failed_count} seat pack(s) failed to sync"
            )
    
    def _generate_status_messages(self, update: POSStatusUpdate):
        """Generate user-friendly messages for status check"""
        
        # Summary message
        update.summary_message = (
            f"Current status: {update.active_packs} active, "
            f"{update.pending_sync_packs} pending sync for {self.event_name}"
        )
        
        # Detailed messages
        if update.active_packs > 0:
            update.detailed_messages.append(
                f"✅ {update.active_packs} active seat pack(s) available"
            )
        
        if update.pending_sync_packs > 0:
            update.detailed_messages.append(
                f"⏳ {update.pending_sync_packs} seat pack(s) pending POS sync"
            )
        
        if update.failed_sync_packs > 0:
            update.detailed_messages.append(
                f"❌ {update.failed_sync_packs} seat pack(s) with sync failures"
            )
        
        if update.inactive_packs > 0:
            update.detailed_messages.append(
                f"🚫 {update.inactive_packs} inactive seat pack(s)"
            )
    
    def _update_generation_status(self, update: POSStatusUpdate):
        """Update database status after seat pack generation"""
        logger.debug(f"Updating generation status: {update.new_packs_created} new packs")
        # Database updates are handled by the seat pack saver
        # This method can be extended for additional status tracking if needed
    
    def _update_sync_status(self, update: POSStatusUpdate, sync_result: POSSyncResult):
        """Update database status after POS sync operation"""
        logger.debug(f"Updating sync status: {sync_result.processed_count} packs processed")
        # Database updates are handled by the POS sync service
        # This method can be extended for additional status tracking if needed


def track_pos_workflow_completion(
    performance_id: str,
    source_website: str,
    enriched_data: Dict[str, Any],
    comparison: SeatPackComparison,
    save_results: Dict[str, Any],
    sync_result: POSSyncResult,
    config: POSConfiguration
) -> Dict[str, POSStatusUpdate]:
    """
    Track complete POS workflow from generation to sync
    
    Args:
        performance_id: Internal performance ID
        source_website: Source website identifier
        enriched_data: Enriched data from NestJS
        comparison: SeatPackComparison results
        save_results: Seat pack save results
        sync_result: POS sync results
        config: POS configuration used
        
    Returns:
        Dictionary with generation and sync status updates
    """
    tracker = POSStatusTracker(performance_id, source_website, enriched_data)
    
    # Track generation phase
    generation_update = tracker.track_seat_pack_generation(comparison, save_results)
    
    # Track sync phase
    sync_update = tracker.track_pos_sync_operation(sync_result, config)
    
    return {
        'generation': generation_update,
        'sync': sync_update,
        'final_status': tracker.get_current_performance_status()
    }