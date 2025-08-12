"""
Database-First POS Sync Service

This service synchronizes seat pack states with StubHub POS API.
It operates independently of the scraping process using a query-based approach.
"""

import time
import logging
import uuid
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from django.db import transaction, IntegrityError
from django.utils import timezone
from datetime import timedelta

from ..models.seat_packs import SeatPack
from ..models.pos import POSListing, FailedRollback
from ..models.base import Performance
from .seat_pack_manager import SeatPackManager

# Import notification helpers
try:
    from consumer.notification_helpers import notify_pos_sync_success, notify_pos_sync_error
except ImportError:
    logger.warning("Notification helpers not available - notifications will be disabled")
    notify_pos_sync_success = lambda *args, **kwargs: False
    notify_pos_sync_error = lambda *args, **kwargs: False

logger = logging.getLogger(__name__)


class POSSyncService:
    """
    External service for synchronizing seat pack states with StubHub POS API.
    Operates independently of the scraping process.
    """
    
    def __init__(self, batch_size: int = 50, rate_limit_delay: float = 1.0):
        self.batch_size = batch_size
        self.rate_limit_delay = rate_limit_delay
        self.seat_pack_manager = SeatPackManager()
        
        # Import API components dynamically to avoid circular imports
        try:
            from consumer.services.pos_api_client import POSAPIClient
            from consumer.services.pos_data_transformer import POSDataTransformer
            self.api_client = POSAPIClient()
            self.transformer = POSDataTransformer()
        except ImportError as e:
            logger.warning(f"POS API components not available: {e}")
            self.api_client = None
            self.transformer = None
    
    def sync_pending_packs(self, performance_id: Optional[str] = None) -> Dict[str, int]:
        """
        Sync all packs that need POS updates.
        
        Args:
            performance_id: Optional performance ID to limit sync scope
            
        Returns:
            Dictionary with sync statistics
        """
        if not self._check_api_availability():
            return {'error': 'API components not available'}
        
        operation_id = str(uuid.uuid4())
        venue_name, event_title = self._get_venue_event_info(performance_id)
        
        logger.info(f"Starting POS sync for {'all performances' if not performance_id else f'performance {performance_id}'}")
        
        # Send POS sync started notification
        notify_pos_sync_success(
            operation_id=operation_id,
            performance_id=performance_id or "all",
            venue=venue_name,
            sync_results={'message': 'POS sync started'},
            sync_type="started",
            event_title=event_title
        )
        
        # Get packs that need syncing
        query = SeatPack.objects.filter(synced_to_pos=False)
        if performance_id:
            query = query.filter(performance=performance_id)
        
        pending_packs = query.order_by('pos_sync_attempts', 'created_at')
        total_packs = pending_packs.count()
        
        if total_packs == 0:
            logger.info("No packs need POS sync")
            # Send completed notification
            notify_pos_sync_success(
                operation_id=operation_id,
                performance_id=performance_id or "all",
                venue=venue_name,
                sync_results={'total_packs': 0, 'processed': 0, 'errors': 0},
                sync_type="ended",
                event_title=event_title
            )
            return {'total_packs': 0, 'processed': 0, 'errors': 0}
        
        logger.info(f"Found {total_packs} packs needing POS sync")
        
        # Process in batches
        processed = 0
        errors = 0
        
        try:
            for batch in self._batch_packs(pending_packs):
                batch_results = self._process_batch(batch)
                processed += batch_results['processed']
                errors += batch_results['errors']
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
                logger.info(f"Processed {processed}/{total_packs} packs, {errors} errors")
            
            logger.info(f"POS sync completed: {processed} processed, {errors} errors")
            
            # Send success notification
            sync_results = {
                'total_packs': total_packs,
                'processed': processed,
                'errors': errors
            }
            
            notify_pos_sync_success(
                operation_id=operation_id,
                performance_id=performance_id or "all",
                venue=venue_name,
                sync_results=sync_results,
                sync_type="ended",
                event_title=event_title
            )
            
            return sync_results
            
        except Exception as e:
            # Send error notification
            notify_pos_sync_error(
                operation_id=operation_id,
                performance_id=performance_id or "all",
                venue=venue_name,
                error_message=str(e),
                sync_type="ended",
                event_title=event_title,
                packs_attempted=total_packs,
                packs_successful=processed
            )
            raise
    
    def sync_performance_toggle(self, performance_id: str, enabled: bool) -> Dict[str, Any]:
        """
        Handle performance-level POS toggle operations.
        
        Args:
            performance_id: Performance ID to toggle
            enabled: Whether to enable or disable POS sync
            
        Returns:
            Dictionary with operation results
        """
        if not self._check_api_availability():
            return {'success': False, 'error': 'API components not available'}
        
        try:
            # Update performance POS settings
            performance = Performance.objects.get(internal_performance_id=performance_id)
            performance.pos_sync_enabled = enabled
            performance.save(update_fields=['pos_sync_enabled'])
            
            if enabled:
                return self._handle_performance_enable(performance_id)
            else:
                return self._handle_performance_disable(performance_id)
                
        except Performance.DoesNotExist:
            return {'success': False, 'error': f'Performance {performance_id} not found'}
        except Exception as e:
            logger.error(f"Error toggling performance POS: {e}")
            return {'success': False, 'error': str(e)}
    
    def _handle_performance_enable(self, performance_id: str) -> Dict[str, Any]:
        """Handle performance POS re-enable after being disabled."""
        logger.info(f"Enabling POS sync for performance {performance_id}")
        
        # Mark all performance packs as needing sync
        updated = SeatPack.objects.filter(
            performance=performance_id,
            pack_status='active'
        ).update(
            synced_to_pos=False,
            pos_status='pending'
        )
        
        logger.info(f"Marked {updated} packs for POS sync after enabling")
        
        # Trigger immediate sync
        sync_results = self.sync_pending_packs(performance_id)
        
        return {
            'success': True,
            'action': 'enable',
            'packs_marked': updated,
            'sync_results': sync_results
        }
    
    def _handle_performance_disable(self, performance_id: str) -> Dict[str, Any]:
        """Handle performance POS disable by delisting all packs."""
        logger.info(f"Disabling POS sync for performance {performance_id}")
        
        # Mark all active packs as performance_disabled
        updated = SeatPack.objects.filter(
            performance=performance_id,
            pack_status='active',
            pos_status='active'
        ).update(
            pos_status='inactive',
            delist_reason='performance_disabled',
            synced_to_pos=False  # Trigger StubHub deletion
        )
        
        logger.info(f"Marked {updated} packs for POS delisting due to performance disable")
        
        # Trigger immediate sync to delist
        sync_results = self.sync_pending_packs(performance_id)
        
        return {
            'success': True,
            'action': 'disable',
            'packs_marked': updated,
            'sync_results': sync_results
        }
    
    def _process_batch(self, packs: List[SeatPack]) -> Dict[str, int]:
        """Process a batch of packs for POS sync."""
        processed = 0
        errors = 0
        
        for pack in packs:
            try:
                result = self.sync_pack_with_rollback(pack)
                if result['success']:
                    processed += 1
                else:
                    errors += 1
                    logger.error(f"Failed to sync pack {pack.internal_pack_id}: {result.get('error')}")
                    
            except Exception as e:
                errors += 1
                logger.error(f"Unexpected error syncing pack {pack.internal_pack_id}: {e}")
        
        return {'processed': processed, 'errors': errors}
    
    def sync_pack_with_rollback(self, pack: SeatPack) -> Dict[str, Any]:
        """
        Sync a pack with full rollback support.
        
        Returns:
            dict: Result with success status and rollback info
        """
        operation_id = str(uuid.uuid4())
        rollback_actions = []
        process_id = f"pos_sync_{operation_id[:8]}"
        
        # Acquire lock on the pack
        locked_pack = self.seat_pack_manager.acquire_pack_lock(pack.internal_pack_id, process_id)
        if not locked_pack:
            return {'success': False, 'error': 'Could not acquire pack lock'}
        
        try:
            # Step 1: Mark operation as started
            pack.pos_operation_id = operation_id
            pack.pos_operation_status = 'started'
            pack.save(update_fields=['pos_operation_id', 'pos_operation_status'])
            
            # Step 2: Perform StubHub operation based on pack status
            if pack.pack_status == 'active' and pack.pos_status == 'pending':
                # Create in StubHub
                result = self._create_or_update_in_stubhub(pack, rollback_actions)
                if not result['success']:
                    raise Exception(result['error'])
                    
            elif pack.pack_status == 'inactive' and pack.pos_status == 'active':
                # Delete from StubHub
                result = self._delist_from_stubhub(pack, rollback_actions)
                if not result['success']:
                    raise Exception(result['error'])
                    
            elif pack.delist_reason == 'performance_disabled':
                # Performance-level disable
                result = self._delist_from_stubhub(pack, rollback_actions, ignore_not_found=True)
                if not result['success']:
                    raise Exception(result['error'])
            
            # Step 3: Mark as synced on success
            pack.synced_to_pos = True
            pack.pos_sync_error = None
            pack.pos_operation_status = 'completed'
            pack.pos_operation_id = None
            pack.save(update_fields=[
                'synced_to_pos', 'pos_sync_error', 'pos_operation_status', 'pos_operation_id'
            ])
            
            # Release lock
            self.seat_pack_manager.release_pack_lock(pack.internal_pack_id, process_id)
            
            logger.info(f"Successfully synced pack {pack.internal_pack_id}")
            return {
                'success': True,
                'operation_id': operation_id,
                'rollback_actions': rollback_actions
            }
            
        except Exception as e:
            # Rollback on any failure
            self._perform_rollback(operation_id, rollback_actions)
            
            # Mark operation as failed
            pack.pos_operation_status = 'failed'
            pack.pos_sync_error = str(e)
            pack.pos_sync_attempts += 1
            pack.last_pos_sync_attempt = timezone.now()
            pack.save(update_fields=[
                'pos_operation_status', 'pos_sync_error', 'pos_sync_attempts', 'last_pos_sync_attempt'
            ])
            
            # Release lock
            self.seat_pack_manager.release_pack_lock(pack.internal_pack_id, process_id)
            
            logger.error(f"POS sync failed for pack {pack.internal_pack_id}: {e}")
            return {
                'success': False,
                'operation_id': operation_id,
                'error': str(e)
            }
    
    def _create_or_update_in_stubhub(self, pack: SeatPack, rollback_actions: List[Dict]) -> Dict[str, Any]:
        """Create or update pack in StubHub."""
        try:
            inventory_data = self.transformer.transform_seat_pack(pack)
            
            if pack.pos_listing:
                # Update existing inventory
                response = self.api_client.update_inventory(
                    pack.pos_listing.stubhub_inventory_id,
                    inventory_data
                )
                
                rollback_actions.append({
                    'action': 'restore_stubhub_inventory',
                    'inventory_data': self._get_current_inventory_data(pack.pos_listing)
                })
                
            else:
                # Create new inventory
                response = self.api_client.create_inventory(inventory_data)
                
                rollback_actions.append({
                    'action': 'delete_stubhub_inventory',
                    'inventory_id': response['inventory_id']
                })
                
                # Create POS listing record
                pos_listing = POSListing.objects.create(
                    performance=pack.performance,
                    pos_inventory_id=response['inventory_id'],
                    stubhub_inventory_id=response['stubhub_inventory_id'],
                    status='ACTIVE'
                )
                
                rollback_actions.append({
                    'action': 'delete_pos_listing',
                    'listing_id': pos_listing.pos_listing_id
                })
                
                pack.pos_listing = pos_listing
                pack.save(update_fields=['pos_listing'])
            
            # Update pack status
            pack.pos_status = 'active'
            pack.save(update_fields=['pos_status'])
            
            return {'success': True}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _delist_from_stubhub(self, pack: SeatPack, rollback_actions: List[Dict], ignore_not_found: bool = False) -> Dict[str, Any]:
        """Remove pack from StubHub."""
        try:
            if not pack.pos_listing:
                if ignore_not_found:
                    return {'success': True}
                return {'success': False, 'error': f'Pack {pack.internal_pack_id} has no POS listing'}
            
            # Store current data for rollback
            rollback_actions.append({
                'action': 'restore_stubhub_inventory',
                'inventory_data': self._get_current_inventory_data(pack.pos_listing)
            })
            
            # Delete from StubHub
            self.api_client.delete_inventory(pack.pos_listing.stubhub_inventory_id)
            
            # Update POS listing status
            pack.pos_listing.status = 'INACTIVE'
            pack.pos_listing.save(update_fields=['status'])
            
            return {'success': True}
            
        except Exception as e:
            if ignore_not_found and 'not found' in str(e).lower():
                # Already deleted - this is OK for performance disable
                if pack.pos_listing:
                    pack.pos_listing.status = 'INACTIVE'
                    pack.pos_listing.save(update_fields=['status'])
                return {'success': True}
            return {'success': False, 'error': str(e)}
    
    def _perform_rollback(self, operation_id: str, rollback_actions: List[Dict]) -> None:
        """Perform compensating actions to rollback a failed operation."""
        logger.info(f"Performing rollback for operation {operation_id}")
        
        for action in reversed(rollback_actions):  # Reverse order for rollback
            try:
                if action['action'] == 'delete_stubhub_inventory':
                    self.api_client.delete_inventory(action['inventory_id'])
                    
                elif action['action'] == 'delete_pos_listing':
                    POSListing.objects.filter(
                        pos_listing_id=action['listing_id']
                    ).delete()
                    
                elif action['action'] == 'restore_stubhub_inventory':
                    self.api_client.create_inventory(action['inventory_data'])
                
                logger.info(f"Rollback action completed: {action['action']}")
                
            except Exception as rollback_error:
                logger.error(f"Rollback action failed: {action['action']}: {rollback_error}")
                # Store failed rollback for manual intervention
                self._store_failed_rollback(operation_id, action, rollback_error)
    
    def _store_failed_rollback(self, operation_id: str, action: Dict, error: Exception) -> None:
        """Store failed rollback actions for manual intervention."""
        try:
            FailedRollback.objects.create(
                operation_id=operation_id,
                action_type=action['action'],
                action_data=action,
                error_message=str(error),
                created_at=timezone.now()
            )
            logger.error(f"Stored failed rollback for manual intervention: {operation_id}")
        except Exception as e:
            logger.error(f"Failed to store rollback failure: {e}")
    
    def _get_current_inventory_data(self, pos_listing: POSListing) -> Dict[str, Any]:
        """Get current inventory data for rollback purposes."""
        try:
            return self.api_client.get_inventory(pos_listing.stubhub_inventory_id)
        except Exception as e:
            logger.warning(f"Could not get current inventory data for rollback: {e}")
            return {}
    
    def _batch_packs(self, packs_queryset) -> List[List[SeatPack]]:
        """Split packs queryset into batches."""
        packs_list = list(packs_queryset)
        for i in range(0, len(packs_list), self.batch_size):
            yield packs_list[i:i + self.batch_size]
    
    def _get_venue_event_info(self, performance_id: Optional[str] = None) -> Tuple[str, Optional[str]]:
        """Get venue and event information for notifications."""
        try:
            if performance_id:
                # Get specific performance info
                performance = Performance.objects.select_related('event', 'venue').get(
                    internal_performance_id=performance_id
                )
                venue_name = performance.venue.name if performance.venue else "Unknown Venue"
                event_title = performance.event.name if performance.event else "Unknown Event"
                return venue_name, event_title
            else:
                # For bulk operations, use generic info
                return "Multiple Venues", "Bulk POS Sync"
        except Performance.DoesNotExist:
            logger.warning(f"Performance {performance_id} not found for venue/event info")
            return "Unknown Venue", "Unknown Event"
        except Exception as e:
            logger.error(f"Error getting venue/event info: {e}")
            return "Unknown Venue", "Unknown Event"
    
    def _check_api_availability(self) -> bool:
        """Check if API components are available."""
        if not self.api_client or not self.transformer:
            logger.error("POS API components not available")
            return False
        return True
    
    def get_sync_health_metrics(self) -> Dict[str, int]:
        """Get health metrics for POS sync operations."""
        return {
            'unsynced_count': SeatPack.objects.filter(synced_to_pos=False).count(),
            'failed_count': SeatPack.objects.filter(pos_status='failed').count(),
            'pending_count': SeatPack.objects.filter(pos_status='pending').count(),
            'high_retry_count': SeatPack.objects.filter(pos_sync_attempts__gte=3).count(),
            'active_locks': SeatPack.objects.filter(locked_by__isnull=False).count(),
            'pending_rollbacks': SeatPack.objects.filter(pos_operation_status='failed').count(),
            'failed_rollbacks': FailedRollback.objects.filter(resolved_at__isnull=True).count(),
        }
    
    def cleanup_stale_operations(self) -> Dict[str, int]:
        """Clean up stale POS operations and locks."""
        stale_threshold = timezone.now() - timedelta(hours=1)
        
        # Clean up stale locks
        stale_locks = self.seat_pack_manager.cleanup_stale_locks(max_age_minutes=30)
        
        # Clean up stale operations
        stale_operations = SeatPack.objects.filter(
            pos_operation_status='started',
            last_pos_sync_attempt__lt=stale_threshold
        ).update(
            pos_operation_status='failed',
            pos_sync_error='Operation timed out',
            pos_operation_id=None
        )
        
        logger.info(f"Cleaned up {stale_locks} stale locks and {stale_operations} stale operations")
        
        return {
            'stale_locks_cleaned': stale_locks,
            'stale_operations_cleaned': stale_operations
        }


def execute_pos_sync(performance_id: Optional[str] = None, batch_size: int = 50) -> Dict[str, Any]:
    """
    Convenience function to execute POS sync with default settings.
    
    Args:
        performance_id: Optional performance ID to limit sync scope
        batch_size: Number of packs to process in each batch
        
    Returns:
        Dictionary with sync results
    """
    service = POSSyncService(batch_size=batch_size)
    return service.sync_pending_packs(performance_id)


def execute_performance_pos_toggle(performance_id: str, enabled: bool) -> Dict[str, Any]:
    """
    Convenience function to toggle performance POS sync.
    
    Args:
        performance_id: Performance ID to toggle
        enabled: Whether to enable or disable POS sync
        
    Returns:
        Dictionary with operation results
    """
    service = POSSyncService()
    return service.sync_performance_toggle(performance_id, enabled)