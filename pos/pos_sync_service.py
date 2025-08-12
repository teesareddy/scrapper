import logging
from typing import Dict, List, Any
from dataclasses import dataclass
from django.db import transaction
from django.utils import timezone
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class POSSyncResult:
    success: bool
    performance_id: str
    total_packs_processed: int
    packs_synced: int
    packs_failed: int
    execution_time_seconds: float
    error_messages: List[str]
    details: Dict[str, Any]


class POSSyncService:
    def __init__(self):
        self.logger = logger
        try:
            from consumer.notification_helpers import notify_pos_sync_success, notify_pos_sync_error
            self.notify_success = notify_pos_sync_success
            self.notify_error = notify_pos_sync_error
        except ImportError:
            self.notify_success = lambda *args, **kwargs: False
            self.notify_error = lambda *args, **kwargs: False
    
    def sync_performance(self, performance_id: str) -> POSSyncResult:
        start_time = timezone.now()
        try:
            self.notify_success(
                operation_id=f"pos_sync_{performance_id}",
                performance_id=performance_id,
                venue="Unknown",  # TODO: Get venue name from performance
                sync_results={"status": "started"},
                sync_type="sync"
            )
        except Exception as e:
            self.logger.warning(f"Failed to send start notification: {e}")
        
        error_messages = []
        packs_synced = 0
        packs_failed = 0
        
        try:
            # Step 1: Get performance and validate POS is enabled
            performance = self._get_performance(performance_id)
            if not performance:
                error_msg = f"Performance {performance_id} not found"
                self.logger.error(error_msg)
                return self._create_error_result(performance_id, start_time, error_msg)
            
            if not performance.pos_sync_enabled:
                error_msg = f"POS sync is disabled for performance {performance_id}"
                self.logger.warning(error_msg)
                return self._create_error_result(performance_id, start_time, error_msg)
            
            # Step 2: Get all seat packs for this performance that need sync
            seat_packs = self._get_seat_packs_for_sync(performance_id)
            
            self.logger.info(f"📦 Found {len(seat_packs)} seat packs to process for performance {performance_id}")
            
            if not seat_packs:
                self.logger.info(f"✅ No seat packs to sync for performance {performance_id}")
                execution_time = (timezone.now() - start_time).total_seconds()
                return POSSyncResult(
                    success=True,
                    performance_id=performance_id,
                    total_packs_processed=0,
                    packs_synced=0,
                    packs_failed=0,
                    execution_time_seconds=execution_time,
                    error_messages=[],
                    details={"message": "No packs to sync"}
                )
            
            # Step 3: Process each seat pack
            for seat_pack in seat_packs:
                try:
                    if self._sync_single_pack(seat_pack, performance):
                        packs_synced += 1
                    else:
                        packs_failed += 1
                except Exception as e:
                    packs_failed += 1
                    error_msg = f"Failed to sync pack {seat_pack.internal_pack_id}: {str(e)}"
                    error_messages.append(error_msg)
                    self.logger.error(error_msg)
            
            # Calculate execution time
            execution_time = (timezone.now() - start_time).total_seconds()
            
            # Determine overall success
            success = packs_failed == 0
            
            result = POSSyncResult(
                success=success,
                performance_id=performance_id,
                total_packs_processed=len(seat_packs),
                packs_synced=packs_synced,
                packs_failed=packs_failed,
                execution_time_seconds=execution_time,
                error_messages=error_messages,
                details={
                    "performance_pos_enabled": performance.pos_sync_enabled,
                    "sync_completed_at": timezone.now().isoformat()
                }
            )
            
            # Send notification: POS sync finished
            try:
                if success:
                    self.notify_success(
                        operation_id=f"pos_sync_{performance_id}",
                        performance_id=performance_id,
                        venue="Unknown",  # TODO: Get venue name from performance
                        sync_results={
                            "status": "completed",
                            "packs_synced": packs_synced,
                            "execution_time": execution_time
                        },
                        sync_type="sync",
                        processing_time_ms=int(execution_time * 1000)
                    )
                    self.logger.info(f"✅ POS SYNC COMPLETED for performance {performance_id}: "
                                   f"{packs_synced} synced, {packs_failed} failed")
                else:
                    self.notify_error(
                        operation_id=f"pos_sync_{performance_id}",
                        performance_id=performance_id,
                        venue="Unknown",  # TODO: Get venue name from performance
                        error_message=f"POS sync completed with {packs_failed} errors",
                        sync_type="sync",
                        packs_attempted=packs_synced + packs_failed
                    )
                    self.logger.warning(f"⚠️ POS SYNC COMPLETED WITH ERRORS for performance {performance_id}: "
                                      f"{packs_synced} synced, {packs_failed} failed")
            except Exception as e:
                self.logger.warning(f"Failed to send completion notification: {e}")
            
            return result
            
        except Exception as e:
            error_msg = f"POS sync failed for performance {performance_id}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            # Send error notification
            try:
                self.notify_error(
                    operation_id=f"pos_sync_{performance_id}",
                    performance_id=performance_id,
                    venue="Unknown",  # TODO: Get venue name from performance
                    error_message=str(e),
                    sync_type="sync",
                    error_type="sync_failure"
                )
            except Exception as notify_e:
                self.logger.warning(f"Failed to send error notification: {notify_e}")
            
            return self._create_error_result(performance_id, start_time, error_msg)
    
    def _get_performance(self, performance_id: str):
        """Get performance record from database"""
        try:
            from scrapers.models import Performance
            return Performance.objects.get(internal_performance_id=performance_id)
        except Performance.DoesNotExist:
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving performance {performance_id}: {e}")
            return None
    
    def _get_seat_packs_for_sync(self, performance_id: str) -> List:
        """
        Get all seat packs for a performance that need POS sync.
        
        This includes:
        - Active packs that haven't been synced yet
        - Packs that need updates
        - Packs that need to be delisted
        """
        try:
            from scrapers.models import SeatPack
            
            # Get seat packs through zone relationship to performance
            seat_packs = SeatPack.objects.filter(
                internal_performance_id=performance_id,
            ).select_related(
                'zone_id__performance_id',
                'zone_id',
                'pos_listing'
            )
            
            return list(seat_packs)
            
        except Exception as e:
            self.logger.error(f"Error retrieving seat packs for performance {performance_id}: {e}")
            return []
    
    def _sync_single_pack(self, seat_pack, performance) -> bool:
        """
        Sync a single seat pack with StubHub.
        
        This is pure API communication - no business logic.
        Just reads the current state from database and makes appropriate API calls.
        
        Args:
            seat_pack: SeatPack model instance
            performance: Performance model instance
            
        Returns:
            bool: True if sync succeeded, False otherwise
        """
        try:
            # Import StubHub inventory creator
            from ..core.stubhub_inventory_creator import StubHubInventoryCreator
            
            creator = StubHubInventoryCreator()
            
            # Check if pack already has POS listing
            if hasattr(seat_pack, 'pos_listing') and seat_pack.pos_listing:
                # Pack already exists in StubHub - check if it needs updates
                self.logger.debug(f"Pack {seat_pack.internal_pack_id} already exists in StubHub")
                # For now, just mark as synced (could add update logic later)
                return True
            else:
                # Pack needs to be created in StubHub
                self.logger.debug(f"Creating pack {seat_pack.internal_pack_id} in StubHub")
                
                # Use StubHubInventoryCreator to process the pack
                creation_result = creator._process_pack_for_creation(seat_pack)
                
                if creation_result.get('success'):
                    self.logger.info(f"✅ Successfully created StubHub inventory for pack {seat_pack.internal_pack_id}")
                    return True
                else:
                    self.logger.error(f"❌ Failed to create StubHub inventory for pack {seat_pack.internal_pack_id}: {creation_result}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Error syncing pack {seat_pack.internal_pack_id}: {e}", exc_info=True)
            return False


    def delete_performance_packs(self, performance_id: str, seat_pack_ids: List[str] = None) -> POSSyncResult:
        """
        Delete seat packs from StubHub POS for a performance.
        
        This method handles deletion of seat packs that have been marked for delisting
        in the database and need to be removed from StubHub as well.
        
        Args:
            performance_id: Internal performance ID
            seat_pack_ids: Optional list of specific seat pack IDs to delete.
                         If None, will delete all packs marked for delisting.
            
        Returns:
            POSSyncResult with detailed deletion results
        """
        start_time = timezone.now()
        
        self.logger.info(f"🗑️ POS DELETE STARTED for performance {performance_id}")
        
        # Send notification: POS delete started
        try:
            self.notify_success(
                operation_id=f"pos_delete_{performance_id}",
                performance_id=performance_id,
                venue="Unknown",  # TODO: Get venue name from performance
                sync_results={"status": "delete_started"},
                sync_type="delete"
            )
        except Exception as e:
            self.logger.warning(f"Failed to send delete start notification: {e}")
        
        error_messages = []
        packs_deleted = 0
        packs_failed = 0
        
        try:
            # Step 1: Get performance and validate POS is enabled
            performance = self._get_performance(performance_id)
            if not performance:
                error_msg = f"Performance {performance_id} not found"
                self.logger.error(error_msg)
                return self._create_error_result(performance_id, start_time, error_msg)
            
            if not performance.pos_sync_enabled:
                error_msg = f"POS sync is disabled for performance {performance_id}"
                self.logger.warning(error_msg)
                return self._create_error_result(performance_id, start_time, error_msg)
            
            # Step 2: Get seat packs that need deletion
            seat_packs = self._get_seat_packs_for_deletion(performance_id, seat_pack_ids)
            
            self.logger.info(f"🗑️ Found {len(seat_packs)} seat packs to delete for performance {performance_id}")
            
            if not seat_packs:
                self.logger.info(f"✅ No seat packs to delete for performance {performance_id}")
                execution_time = (timezone.now() - start_time).total_seconds()
                return POSSyncResult(
                    success=True,
                    performance_id=performance_id,
                    total_packs_processed=0,
                    packs_synced=0,  # Using packs_synced for deleted count
                    packs_failed=0,
                    execution_time_seconds=execution_time,
                    error_messages=[],
                    details={"message": "No packs to delete"}
                )
            
            # Step 3: Delete each seat pack from StubHub
            for seat_pack in seat_packs:
                try:
                    if self._delete_single_pack(seat_pack, performance):
                        packs_deleted += 1
                    else:
                        packs_failed += 1
                except Exception as e:
                    packs_failed += 1
                    error_msg = f"Failed to delete pack {seat_pack.internal_pack_id}: {str(e)}"
                    error_messages.append(error_msg)
                    self.logger.error(error_msg)
            
            # Calculate execution time
            execution_time = (timezone.now() - start_time).total_seconds()
            
            # Determine overall success
            success = packs_failed == 0
            
            result = POSSyncResult(
                success=success,
                performance_id=performance_id,
                total_packs_processed=len(seat_packs),
                packs_synced=packs_deleted,  # Using packs_synced for deleted count
                packs_failed=packs_failed,
                execution_time_seconds=execution_time,
                error_messages=error_messages,
                details={
                    "performance_pos_enabled": performance.pos_sync_enabled,
                    "delete_completed_at": timezone.now().isoformat(),
                    "operation_type": "delete"
                }
            )
            
            # Send notification: POS delete finished
            try:
                if success:
                    self.notify_success(
                        operation_id=f"pos_delete_{performance_id}",
                        performance_id=performance_id,
                        venue="Unknown",  # TODO: Get venue name from performance
                        sync_results={
                            "status": "delete_completed",
                            "packs_deleted": packs_deleted,
                            "execution_time": execution_time
                        },
                        sync_type="delete",
                        processing_time_ms=int(execution_time * 1000)
                    )
                    self.logger.info(f"✅ POS DELETE COMPLETED for performance {performance_id}: "
                                   f"{packs_deleted} deleted, {packs_failed} failed")
                else:
                    self.notify_error(
                        operation_id=f"pos_delete_{performance_id}",
                        performance_id=performance_id,
                        venue="Unknown",  # TODO: Get venue name from performance
                        error_message=f"POS delete completed with {packs_failed} errors",
                        sync_type="delete",
                        packs_attempted=packs_deleted + packs_failed
                    )
                    self.logger.warning(f"⚠️ POS DELETE COMPLETED WITH ERRORS for performance {performance_id}: "
                                      f"{packs_deleted} deleted, {packs_failed} failed")
            except Exception as e:
                self.logger.warning(f"Failed to send delete completion notification: {e}")
            
            return result
            
        except Exception as e:
            error_msg = f"POS delete failed for performance {performance_id}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            
            # Send error notification
            try:
                self.notify_error(
                    operation_id=f"pos_delete_{performance_id}",
                    performance_id=performance_id,
                    venue="Unknown",  # TODO: Get venue name from performance
                    error_message=str(e),
                    sync_type="delete",
                    error_type="delete_failure"
                )
            except Exception as notify_e:
                self.logger.warning(f"Failed to send delete error notification: {notify_e}")
            
            return self._create_error_result(performance_id, start_time, error_msg)
    
    def _get_seat_packs_for_deletion(self, performance_id: str, seat_pack_ids: List[str] = None) -> List:
        """
        Get seat packs that need to be deleted from StubHub POS.
        
        This includes:
        - Packs that are marked for delisting (pack_state='delist')
        - Packs that have POS listings that need to be removed
        
        Args:
            performance_id: Internal performance ID
            seat_pack_ids: Optional list of specific seat pack IDs to filter
        """
        try:
            from scrapers.models import SeatPack
            
            # Base query: get seat packs for the performance that need deletion
            query = SeatPack.objects.filter(
                zone_id__performance_id__internal_performance_id=performance_id,
                pack_state='delist'  # Only packs marked for delisting
            ).select_related(
                'zone_id__performance_id',
                'zone_id',
                'pos_listing'
            )
            
            # Filter by specific pack IDs if provided
            if seat_pack_ids:
                query = query.filter(internal_pack_id__in=seat_pack_ids)
            
            # Only include packs that have POS listings to delete
            query = query.filter(pos_listing__isnull=False)
            
            return list(query)
            
        except Exception as e:
            self.logger.error(f"Error retrieving seat packs for deletion for performance {performance_id}: {e}")
            return []
    
    def _delete_single_pack(self, seat_pack, performance) -> bool:
        """
        Delete a single seat pack from StubHub POS.
        
        Args:
            seat_pack: SeatPack model instance
            performance: Performance model instance
            
        Returns:
            bool: True if deletion succeeded, False otherwise
        """
        try:
            # Import POSAPIClient for deletion
            from consumer.services.pos_api_client import POSAPIClient
            
            api_client = POSAPIClient()
            
            # Check if pack has POS listing to delete
            if not hasattr(seat_pack, 'pos_listing') or not seat_pack.pos_listing:
                self.logger.warning(f"Pack {seat_pack.internal_pack_id} has no POS listing to delete")
                return True  # Consider this successful since there's nothing to delete
            
            # Get the StubHub inventory ID from the POS listing
            stubhub_inventory_id = seat_pack.pos_listing.stubhub_inventory_id
            if not stubhub_inventory_id:
                self.logger.warning(f"Pack {seat_pack.internal_pack_id} has no StubHub inventory ID")
                return True  # Consider this successful since there's nothing to delete
            
            self.logger.debug(f"Deleting pack {seat_pack.internal_pack_id} from StubHub (ID: {stubhub_inventory_id})")
            
            # Call StubHub API to delete the inventory
            delete_result = api_client.delete_inventory_listing(stubhub_inventory_id)
            
            if delete_result.is_successful:
                self.logger.info(f"✅ Successfully deleted StubHub inventory for pack {seat_pack.internal_pack_id}")
                
                # Update the seat pack and POS listing to reflect successful deletion
                with transaction.atomic():
                    # Update POS listing status to INACTIVE (preserve for audit trail)
                    seat_pack.pos_listing.status = 'INACTIVE'
                    seat_pack.pos_listing.save()
                    
                    # Update seat pack POS status and sync status
                    seat_pack.pos_status = 'inactive'  # Reflects StubHub state
                    seat_pack.synced_to_pos = True     # Mark as synced (successfully deleted)
                    seat_pack.save()
                
                return True
            else:
                self.logger.error(f"❌ Failed to delete StubHub inventory for pack {seat_pack.internal_pack_id}: {delete_result.error}")
                return False
                    
        except Exception as e:
            self.logger.error(f"Error deleting pack {seat_pack.internal_pack_id}: {e}", exc_info=True)
            return False

    def _create_error_result(self, performance_id: str, start_time: datetime, error_msg: str) -> POSSyncResult:
        """Create a POSSyncResult for error cases"""
        execution_time = (timezone.now() - start_time).total_seconds()
        return POSSyncResult(
            success=False,
            performance_id=performance_id,
            total_packs_processed=0,
            packs_synced=0,
            packs_failed=0,
            execution_time_seconds=execution_time,
            error_messages=[error_msg],
            details={"error": error_msg}
        )



def sync_performance_pos(performance_id: str) -> POSSyncResult:
    service = POSSyncService()
    return service.sync_performance(performance_id)


def delete_performance_pos(performance_id: str, seat_pack_ids: List[str] = None) -> POSSyncResult:
    """
    Convenience function to delete POS listings for a performance.
    
    Usage:
        from scrapers.pos.pos_sync_service import delete_performance_pos
        result = delete_performance_pos("BSF_PERF_123")
    
    Args:
        performance_id: Internal performance ID to delete packs for
        seat_pack_ids: Optional list of specific seat pack IDs to delete
        
    Returns:
        POSSyncResult with deletion details
    """
    service = POSSyncService()
    return service.delete_performance_packs(performance_id, seat_pack_ids)