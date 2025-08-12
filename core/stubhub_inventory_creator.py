"""
StubHub Inventory Creator - Database-Driven POS Sync

This module handles StubHub inventory operations by querying seat packs 
from the database and performing create/delete operations based on 
pack status flags, completely decoupled from the scraping process.
"""

import logging
from typing import List, Dict, Optional, Any
from django.db import transaction
from django.utils import timezone
from django.conf import settings

from ..models.seat_packs import SeatPack
from ..models.pos import POSListing
from ..models.base import Performance
from consumer.services.pos_api_client import POSAPIClient
from consumer.services.pos_data_transformer import POSDataTransformer
from consumer.rabbitmq_producer import producer

logger = logging.getLogger(__name__)


class StubHubInventoryCreator:
    """
    Database-driven StubHub inventory sync service.
    
    Queries seat packs that need POS operations (synced_to_pos=False) and 
    performs create/delete operations in StubHub based on pack status.
    """
    
    def __init__(self, source_website: str = None, pos_enabled: bool = None):
        """
        Initialize the inventory creator
        
        Args:
            source_website: Source website identifier (optional, can process all if None)
            pos_enabled: Override POS integration status from message data (optional)
        """
        self.source_website = source_website
        self.api_client = POSAPIClient()
        self.transformer = POSDataTransformer()
        
        # Use message-based POS status if provided, otherwise check settings
        if pos_enabled is not None:
            self.pos_integration_enabled = pos_enabled
            logger.info(f"POS integration status set from message data: {pos_enabled}")
        else:
            self.pos_integration_enabled = self._check_pos_integration_enabled()
            logger.info(f"POS integration status from settings: {self.pos_integration_enabled}")
        
    def _check_pos_integration_enabled(self) -> bool:
        """Check if POS integration is enabled in settings"""
        return (
            hasattr(settings, 'STUBHUB_POS_BASE_URL') and 
            settings.STUBHUB_POS_BASE_URL and
            hasattr(settings, 'STUBHUB_POS_AUTH_TOKEN')
        )
    
    def sync_pending_packs(self, performance_id: str = None) -> Dict[str, Any]:
        """
        Sync all pending packs that need POS operations (create or delete)
        
        Args:
            performance_id: Optional performance ID to limit sync scope
            
        Returns:
            Dictionary with results of sync operations
        """
        if not self.pos_integration_enabled:
            logger.info("POS integration not enabled, skipping sync")
            return {
                'created': 0,
                'deleted': 0,
                'failed': 0,
                'errors': []
            }
        
        # Query packs that need sync operations
        query = SeatPack.objects.filter(synced_to_pos=False).select_related(
            'pos_listing', 'performance', 'level', 'zone_id'
        )
        

        # Filter by performance if specified
        if performance_id:
            query = query.filter(performance=performance_id)
        
        # Debug logging to understand query results
        logger.info(f"POS Sync Query: source_website='{self.source_website}', performance_id='{performance_id}'")
        
        # Check total pending packs without filters first
        total_pending = SeatPack.objects.filter(synced_to_pos=False).count()
        logger.info(f"Total pending packs (no filters): {total_pending}")
        
        # Check with just source website filter
        # if self.source_website:
        #     website_filtered = SeatPack.objects.filter(
        #         synced_to_pos=False,
        #         source_website=self.source_website
        #     ).count()
        #     logger.info(f"Pending packs for source '{self.source_website}': {website_filtered}")
        #
        pending_packs = list(query.order_by('pos_sync_attempts', 'created_at'))
        
        if not pending_packs:
            logger.warning(f"❌ No packs found with current filters!")
            logger.info(f"Query details: synced_to_pos=False, source_website='{self.source_website}', performance='{performance_id}'")
            
            # Show what source websites exist for pending packs
            existing_sources = SeatPack.objects.filter(
                synced_to_pos=False
            ).values_list('source_website', flat=True).distinct()
            logger.info(f"Available source websites for pending packs: {list(existing_sources)}")
            
            # Show what performance IDs exist for this source
            if self.source_website:
                existing_performances = SeatPack.objects.filter(
                    synced_to_pos=False,
                    source_website=self.source_website
                ).values_list('performance', flat=True).distinct()
                logger.info(f"Available performance IDs for source '{self.source_website}': {list(existing_performances)}")
            else:
                # Show all performance IDs for pending packs
                all_performances = SeatPack.objects.filter(
                    synced_to_pos=False
                ).values_list('performance', flat=True).distinct()
                logger.info(f"All performance IDs with pending packs: {list(all_performances)}")
            
            # IMPORTANT: Still send sync notifications even when no packs found
            # This ensures proper status transitions from "scraping" to final status
            if performance_id:
                # Generate operation ID for tracking
                operation_id = f"sync_{performance_id}_{timezone.now().strftime('%Y%m%d_%H%M%S')}"
                
                # Send start notification
                self._send_pos_sync_start_notification(performance_id, 0, operation_id)
                
                # Send completion notification immediately
                results = {
                    'created': 0,
                    'deleted': 0, 
                    'failed': 0,
                    'errors': []
                }
                
                sync_results = {
                    **results,
                    'total_packs': 0,
                    'inventory_created': 0,
                    'inventory_failed': 0
                }
                
                try:
                    message_sent = self._send_pos_sync_completion_message(
                        performance_id=performance_id,
                        success=True,  # Success because no work was needed
                        sync_results=sync_results,
                        error_message=None,
                        operation_id=operation_id
                    )
                    
                    if message_sent:
                        logger.info(f"Sent POS sync completion message for performance {performance_id} (no packs to process)")
                    else:
                        logger.warning(f"Failed to send POS sync completion message for performance {performance_id}")
                        
                except Exception as e:
                    logger.error(f"Error sending sync completion message for no-packs case: {str(e)}", exc_info=True)
            
            return {
                'created': 0,
                'deleted': 0, 
                'failed': 0,
                'errors': []
            }
        
        logger.info(f"Starting POS sync for {len(pending_packs)} packs" + 
                   (f" (performance {performance_id})" if performance_id else ""))
        
        # Generate operation ID for tracking (consistent across start/completion messages)
        operation_id = None
        if performance_id:
            operation_id = f"sync_{performance_id}_{timezone.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Send sync start notification - ALWAYS send if performance_id exists
        if performance_id:
            self._send_pos_sync_start_notification(performance_id, len(pending_packs), operation_id)
        
        results = {
            'created': 0,
            'deleted': 0,
            'failed': 0,
            'errors': []
        }
        
        for pack in pending_packs:
            try:
                # Determine operation based on pack state - Enhanced for four-dimensional states
                if pack.pack_state in ['create', 'split', 'merge', 'shrink'] and pack.pos_status == 'pending':
                    # Create new inventory in StubHub
                    logger.debug(f"Pack {pack.internal_pack_id}: Creating new inventory (state={pack.pack_state}, pos_status=pending)")
                    operation_result = self._process_pack_for_creation(pack)
                    if operation_result['success']:
                        results['created'] += 1
                        logger.info(f"✓ Created inventory for pack {pack.internal_pack_id}")
                    else:
                        results['failed'] += 1
                        results['errors'].append({
                            'pack_id': pack.internal_pack_id,
                            'error': operation_result['error']
                        })
                        logger.error(f"✗ Failed to create inventory for pack {pack.internal_pack_id}: {operation_result['error']}")
                        
                elif pack.pack_state == 'delist' and pack.pos_status in ['active', 'pending']:
                    # Delete from StubHub - This handles manual delists and other delisting scenarios
                    logger.debug(f"Pack {pack.internal_pack_id}: Deleting inventory (state=delist, pos_status={pack.pos_status}) "
                               f"delist_reason={pack.delist_reason}")
                    operation_result = self._process_pack_for_deletion(pack)
                    if operation_result['success']:
                        results['deleted'] += 1
                        logger.info(f"✓ Deleted inventory for pack {pack.internal_pack_id}")
                    else:
                        results['failed'] += 1
                        results['errors'].append({
                            'pack_id': pack.internal_pack_id,
                            'error': operation_result['error']
                        })
                        logger.error(f"✗ Failed to delete inventory for pack {pack.internal_pack_id}: {operation_result['error']}")
                        
                elif pack.pack_state == 'transformed' and pack.pos_status in ['active', 'pending']:
                    # Delete from StubHub - Pack has been transformed into other packs
                    logger.debug(f"Pack {pack.internal_pack_id}: Deleting inventory (state=transformed, pos_status={pack.pos_status})")
                    operation_result = self._process_pack_for_deletion(pack)
                    if operation_result['success']:
                        results['deleted'] += 1
                        logger.info(f"✓ Deleted transformed pack inventory {pack.internal_pack_id}")
                    else:
                        results['failed'] += 1
                        results['errors'].append({
                            'pack_id': pack.internal_pack_id,
                            'error': operation_result['error']
                        })
                        logger.error(f"✗ Failed to delete transformed pack inventory {pack.internal_pack_id}: {operation_result['error']}")
                        
                elif pack.pos_status == 'failed' and pack.pack_status == 'active':
                    # Retry failed operations based on pack state
                    if pack.pack_state in ['create', 'split', 'merge', 'shrink']:
                        logger.debug(f"Pack {pack.internal_pack_id}: Retrying failed creation (state={pack.pack_state})")
                        operation_result = self._process_pack_for_creation(pack)
                        if operation_result['success']:
                            results['created'] += 1
                            logger.info(f"✓ Retry successful for pack {pack.internal_pack_id}")
                        else:
                            results['failed'] += 1
                            results['errors'].append({
                                'pack_id': pack.internal_pack_id,
                                'error': operation_result['error']
                            })
                            logger.error(f"✗ Retry failed for pack {pack.internal_pack_id}: {operation_result['error']}")
                    elif pack.pack_state in ['delist', 'transformed']:
                        logger.debug(f"Pack {pack.internal_pack_id}: Retrying failed deletion (state={pack.pack_state})")
                        operation_result = self._process_pack_for_deletion(pack)
                        if operation_result['success']:
                            results['deleted'] += 1
                            logger.info(f"✓ Retry deletion successful for pack {pack.internal_pack_id}")
                        else:
                            results['failed'] += 1
                            results['errors'].append({
                                'pack_id': pack.internal_pack_id,
                                'error': operation_result['error']
                            })
                            logger.error(f"✗ Retry deletion failed for pack {pack.internal_pack_id}: {operation_result['error']}")
                        
                else:
                    # Mark as synced if no action needed
                    logger.debug(f"Pack {pack.internal_pack_id}: No action needed "
                               f"(pack_status={pack.pack_status}, pack_state={pack.pack_state}, pos_status={pack.pos_status})")
                    pack.synced_to_pos = True
                    pack.save(update_fields=['synced_to_pos'])
                
            except Exception as e:
                error_msg = f"Unexpected error syncing pack {pack.internal_pack_id}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                results['failed'] += 1
                results['errors'].append({
                    'pack_id': pack.internal_pack_id,
                    'error': error_msg
                })
        
        # Log comprehensive sync summary
        total_processed = results['created'] + results['deleted'] + results['failed']
        success_rate = ((results['created'] + results['deleted']) / total_processed * 100) if total_processed > 0 else 0
        
        logger.info(f"📊 POS Sync Summary:")
        logger.info(f"  • Total packs processed: {total_processed}")
        logger.info(f"  • Successfully created: {results['created']}")
        logger.info(f"  • Successfully deleted: {results['deleted']}")
        logger.info(f"  • Failed operations: {results['failed']}")
        logger.info(f"  • Success rate: {success_rate:.1f}%")
        logger.info(f"  • Errors encountered: {len(results['errors'])}")
        
        if results['errors']:
            logger.warning(f"🚨 Error breakdown (first 5 errors):")
            for error in results['errors'][:5]:
                logger.warning(f"  • Pack {error.get('pack_id', 'Unknown')}: {error.get('error', 'Unknown error')}")
            if len(results['errors']) > 5:
                logger.warning(f"  • ... and {len(results['errors']) - 5} more errors")
        
        successful_ops = results['created'] + results['deleted']
        if successful_ops > 0:
            logger.info(f"✅ Successfully synced {successful_ops} seat packs to POS")
        
        if results['failed'] > 0:
            logger.error(f"❌ Failed to sync {results['failed']} seat packs to POS")
        
        # Send sync completion message - ALWAYS send if performance_id exists
        if performance_id:
            try:
                # Determine if sync was successful - always success if no packs to process or no failures
                sync_success = results['failed'] == 0 or (total_processed > 0 and successful_ops > results['failed'])
                
                # Prepare sync results
                sync_results = {
                    **results,
                    'total_packs': total_processed,
                    'inventory_created': results['created'],
                    'inventory_failed': results['failed']
                }
                
                # Send completion message
                error_message = None
                if not sync_success and results['errors']:
                    error_message = f"POS sync failed with {results['failed']} errors: " + \
                                  "; ".join([error.get('error', 'Unknown error') for error in results['errors'][:3]])
                elif total_processed == 0:
                    # When no packs to process, provide informative message
                    error_message = None  # This is actually success - no work needed
                
                message_sent = self._send_pos_sync_completion_message(
                    performance_id=performance_id,
                    success=sync_success,
                    sync_results=sync_results,
                    error_message=error_message,
                    operation_id=operation_id
                )
                
                if message_sent:
                    logger.info(f"Sent POS sync completion message for performance {performance_id}")
                else:
                    logger.warning(f"Failed to send POS sync completion message for performance {performance_id}")
                    
            except Exception as e:
                logger.error(f"Error sending sync completion message: {str(e)}", exc_info=True)
                # Don't fail the entire operation due to messaging issues
        
        return results
    
    def mark_pack_for_pos_deletion(self, pack_id: str, delist_reason: str = 'transformed') -> Dict[str, Any]:
        """
        Mark a seat pack for deletion from StubHub POS system.
        
        This method is used when packs get "shrunk" from other packs or need to be
        delisted for other reasons. It properly sets the four-dimensional state
        to trigger the POS sync process to delete the pack from StubHub.
        
        Args:
            pack_id: Internal pack ID of the seat pack to delete
            delist_reason: Reason for delisting (default: 'transformed')
            
        Returns:
            Dictionary with success status and details
        """
        try:
            # Get the pack to be deleted
            pack = SeatPack.objects.get(internal_pack_id=pack_id)
            
            # Validate pack can be delisted
            # Note: pack_status should remain 'active' during delisting process
            # Only check if already marked for delisting
            if pack.pack_state == 'delist':
                logger.warning(f"Pack {pack_id} is already marked for delisting")
                return {
                    'success': True,
                    'message': 'Pack already marked for delisting',
                    'already_delisting': True
                }
            
            logger.info(f"🗑️ Marking pack {pack_id} for POS deletion (reason: {delist_reason})")
            logger.debug(f"Current state: pack_status={pack.pack_status}, pos_status={pack.pos_status}, "
                        f"pack_state={pack.pack_state}, synced_to_pos={pack.synced_to_pos}")
            
            # Set the four-dimensional state for deletion
            # NOTE: pack_status stays 'active' - only our business logic should change pack_status
            # StubHub integration only manages pos_status, pack_state, and sync flags
            pack.pos_status = 'pending'    # Needs sync (for deletion)  
            pack.synced_to_pos = False     # Triggers sync process
            pack.pack_state = 'delist'     # Lifecycle state
            pack.delist_reason = delist_reason  # Why it's being delisted
            
            # Save the changes (excluding pack_status since we're not changing it)
            pack.save(update_fields=[
                'pos_status', 'synced_to_pos', 
                'pack_state', 'delist_reason'
            ])
            
            logger.info(f"✅ Pack {pack_id} marked for deletion - will be picked up by next POS sync")
            logger.debug(f"New state: pack_status={pack.pack_status}, pos_status=pending, pack_state=delist, "
                        f"delist_reason={delist_reason}, synced_to_pos=False")
            
            return {
                'success': True,
                'pack_id': pack_id,
                'message': 'Pack marked for POS deletion',
                'state_transition': {
                    'pack_status': f'{pack.pack_status} (unchanged)',
                    'pos_status': f'previous → pending',
                    'pack_state': f'previous → delist',
                    'delist_reason': delist_reason,
                    'synced_to_pos': 'True → False'
                }
            }
            
        except SeatPack.DoesNotExist:
            error_msg = f"Pack {pack_id} not found"
            logger.error(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg,
                'pack_id': pack_id
            }
            
        except Exception as e:
            error_msg = f"Error marking pack {pack_id} for deletion: {str(e)}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            return {
                'success': False,
                'error': error_msg,
                'pack_id': pack_id
            }
    
    def mark_multiple_packs_for_pos_deletion(self, pack_ids: List[str], delist_reason: str = 'transformed') -> Dict[str, Any]:
        """
        Mark multiple seat packs for deletion from StubHub POS system in batch.
        
        Args:
            pack_ids: List of internal pack IDs to delete
            delist_reason: Reason for delisting (default: 'transformed')
            
        Returns:
            Dictionary with batch results and summary
        """
        results = {
            'success': True,
            'total_packs': len(pack_ids),
            'successful_marks': 0,
            'failed_marks': 0,
            'already_handled': 0,
            'details': [],
            'errors': []
        }
        
        logger.info(f"🗑️ Marking {len(pack_ids)} packs for POS deletion (reason: {delist_reason})")
        
        for pack_id in pack_ids:
            result = self.mark_pack_for_pos_deletion(pack_id, delist_reason)
            results['details'].append(result)
            
            if result['success']:
                if result.get('already_inactive') or result.get('already_delisting'):
                    results['already_handled'] += 1
                else:
                    results['successful_marks'] += 1
            else:
                results['failed_marks'] += 1
                results['errors'].append(result['error'])
        
        # Summary logging
        logger.info(f"📊 Batch deletion marking complete:")
        logger.info(f"  • Total packs: {results['total_packs']}")
        logger.info(f"  • Successfully marked: {results['successful_marks']}")
        logger.info(f"  • Already handled: {results['already_handled']}")
        logger.info(f"  • Failed: {results['failed_marks']}")
        
        if results['errors']:
            logger.warning(f"🚨 Errors encountered:")
            for error in results['errors'][:5]:  # Show first 5 errors
                logger.warning(f"  • {error}")
            if len(results['errors']) > 5:
                logger.warning(f"  • ... and {len(results['errors']) - 5} more errors")
        
        # Overall success if no failures
        results['success'] = results['failed_marks'] == 0
        
        return results
    
    def _process_pack_for_creation(self, pack: SeatPack) -> Dict[str, Any]:
        """
        Process a seat pack for creation in StubHub
        
        Args:
            pack: SeatPack to create in StubHub
            
        Returns:
            Dictionary with success status and details
        """
        try:
            # Validate seat pack before creating inventory
            validation_result = self._validate_seat_pack_for_pos(pack)
            if not validation_result['is_valid']:
                return {
                    'success': False,
                    'error': f"Validation failed: {validation_result['reason']}"
                }
            
            # Transform seat pack to POS payload
            pos_payload = self.transformer.transform_seat_pack_to_pos_payload(pack)
            
            # Validate payload
            is_valid, error_msg = self.transformer.validate_pos_payload(pos_payload)
            if not is_valid:
                return {
                    'success': False,
                    'error': f"Payload validation failed: {error_msg}"
                }
            
            # Add creation context to payload
            pos_payload['internalNotes'] = (
                f"Auto-created via database sync. "
                f"Generated on {timezone.now().strftime('%Y-%m-%d %H:%M')} (Validation passed)"
            )
            
            # Log the payload being sent to StubHub API
            logger.info(f"Creating StubHub inventory for pack {pack.internal_pack_id} with payload: {pos_payload}")
            
            # Make actual POS API call
            pos_response = self.api_client.create_inventory_listing(pos_payload)
            
            # Log the API response from StubHub
            logger.info(f"StubHub create inventory API response for pack {pack.internal_pack_id}: {pos_response}")
            
            inventory_id = self._extract_inventory_id_from_response(pos_response)
            
            if not inventory_id:
                return {
                    'success': False,
                    'error': 'Failed to extract inventory ID from POS API response'
                }
            
            # Create POSListing record with the real inventory ID
            pos_listing = self._create_pos_listing(pack, inventory_id)
            
            # Update pack status after successful POS operation  
            pack.pos_status = 'active'
            pack.synced_to_pos = True
            pack.pos_listing = pos_listing
            pack.save(update_fields=['pos_status', 'synced_to_pos', 'pos_listing'])
            
            logger.debug(f"Updated pack {pack.internal_pack_id} pos_status to 'active' after successful POS sync")

            return {
                'success': True, 
                'inventory_id': inventory_id, 
                'pos_listing_id': pos_listing.pos_listing_id if pos_listing else None
            }
                
        except Exception as e:
            # Update sync attempt counter on failure
            pack.pos_sync_attempts = (pack.pos_sync_attempts or 0) + 1
            pack.pos_sync_error = str(e)
            pack.save(update_fields=['pos_sync_attempts', 'pos_sync_error'])
            
            logger.error(f"Error creating inventory for pack {pack.internal_pack_id}: {str(e)}", exc_info=True)
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}"
            }
    
    def _process_pack_for_deletion(self, pack: SeatPack) -> Dict[str, Any]:
        """
        Process a seat pack for deletion from StubHub with enhanced error handling
        
        Args:
            pack: SeatPack to delete from StubHub
            
        Returns:
            Dictionary with success status and details
        """
        operation_id = f"del_{pack.internal_pack_id}_{timezone.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            logger.info(f"🗑️ Processing pack {pack.internal_pack_id} for deletion (operation: {operation_id})")
            logger.debug(f"Pack state: pack_status={pack.pack_status}, pos_status={pack.pos_status}, "
                        f"pack_state={pack.pack_state}, delist_reason={pack.delist_reason}")
            
            # Validate pack is in correct state for deletion
            if pack.pack_state != 'delist':
                logger.warning(f"Pack {pack.internal_pack_id} has pack_state={pack.pack_state}, expected 'delist'")
            
            # Check if pack has POS listing to delete
            if not pack.pos_listing:
                logger.info(f"Pack {pack.internal_pack_id} has no POS listing, skipping StubHub API call")
                # Pack wasn't in StubHub anyway, just mark as synced
                pack.pos_status = 'inactive'
                pack.synced_to_pos = True
                pack.pos_operation_id = None  # Clear any pending operation
                pack.pos_operation_status = None
                pack.save(update_fields=['pos_status', 'synced_to_pos', 'pos_operation_id', 'pos_operation_status'])
                return {
                    'success': True,
                    'message': 'Pack had no POS listing, marked as inactive',
                    'operation_id': operation_id
                }
            
            # Set operation tracking for rollback support
            pack.pos_operation_id = operation_id
            pack.pos_operation_status = 'started'
            pack.save(update_fields=['pos_operation_id', 'pos_operation_status'])
            
            stubhub_inventory_id = pack.pos_listing.stubhub_inventory_id
            logger.info(f"Deleting StubHub inventory {stubhub_inventory_id} for pack {pack.internal_pack_id}")
            
            try:
                # Log the inventory ID being deleted
                logger.info(f"Calling StubHub delete API for inventory ID: {stubhub_inventory_id} (pack: {pack.internal_pack_id})")
                
                # Make actual POS API call to delete inventory
                delete_response = self.api_client.delete_inventory_listing(stubhub_inventory_id)
                logger.debug(f"StubHub delete API response: {delete_response}")
                logger.info(f"Successfully called StubHub delete API for inventory {stubhub_inventory_id} (pack: {pack.internal_pack_id})")
                
                # Update POSListing status
                pack.pos_listing.status = 'INACTIVE'
                pack.pos_listing.save(update_fields=['status'])
                
                # Update pack status after successful deletion
                pack.pos_status = 'inactive'
                pack.synced_to_pos = True
                pack.pos_operation_status = 'completed'
                pack.pos_sync_error = None  # Clear any previous errors
                
                update_fields = ['pos_status', 'synced_to_pos', 'pos_operation_status', 'pos_sync_error']
                pack.save(update_fields=update_fields)
                
                # Clear operation tracking after success
                pack.pos_operation_id = None
                pack.pos_operation_status = None
                pack.save(update_fields=['pos_operation_id', 'pos_operation_status'])
                
                logger.info(f"✅ Successfully deleted pack {pack.internal_pack_id} from StubHub")
                
                return {
                    'success': True,
                    'message': f'Successfully deleted from StubHub (inventory_id: {stubhub_inventory_id})',
                    'operation_id': operation_id,
                    'inventory_id': stubhub_inventory_id
                }
                
            except Exception as api_error:
                error_msg = str(api_error)
                
                # Check for specific error types that should be handled gracefully
                if any(phrase in error_msg.lower() for phrase in [
                    'not found', '404', 'does not exist', 'already deleted', 'inventory not found'
                ]):
                    logger.warning(f"StubHub inventory {stubhub_inventory_id} not found, "
                                 f"treating as already deleted for pack {pack.internal_pack_id}")
                    
                    # Mark as successfully deleted since it's already gone from StubHub
                    pack.pos_listing.status = 'INACTIVE'
                    pack.pos_listing.save(update_fields=['status'])
                    
                    pack.pos_status = 'inactive'
                    pack.synced_to_pos = True
                    pack.pos_operation_status = 'completed'
                    pack.pos_sync_error = f"Inventory already deleted: {error_msg}"
                    
                    update_fields = ['pos_status', 'synced_to_pos', 'pos_operation_status', 'pos_sync_error']
                    pack.save(update_fields=update_fields)
                    
                    # Clear operation tracking
                    pack.pos_operation_id = None
                    pack.pos_operation_status = None
                    pack.save(update_fields=['pos_operation_id', 'pos_operation_status'])
                    
                    return {
                        'success': True,
                        'message': f'Inventory already deleted from StubHub (inventory_id: {stubhub_inventory_id})',
                        'operation_id': operation_id,
                        'inventory_id': stubhub_inventory_id
                    }
                else:
                    # Actual API error, fail the operation
                    raise api_error
                
        except Exception as e:
            error_msg = f"Error deleting inventory for pack {pack.internal_pack_id}: {str(e)}"
            logger.error(f"❌ DELETE FAILED: {error_msg}", exc_info=True)
            
            # Update sync attempt counter and error tracking
            pack.pos_sync_attempts = (pack.pos_sync_attempts or 0) + 1
            pack.pos_sync_error = str(e)
            pack.last_pos_sync_attempt = timezone.now()
            pack.pos_operation_status = 'failed'
            
            # Determine retry strategy based on attempt count
            max_attempts = 5
            if pack.pos_sync_attempts >= max_attempts:
                logger.error(f"Pack {pack.internal_pack_id} exceeded max deletion attempts ({max_attempts}), "
                           f"marking as permanently failed")
                pack.pos_status = 'failed'
                pack.synced_to_pos = False  # Keep trying on next sync cycle
            
            pack.save(update_fields=[
                'pos_sync_attempts', 'pos_sync_error', 'last_pos_sync_attempt', 
                'pos_operation_status', 'pos_status', 'synced_to_pos'
            ])
            
            return {
                'success': False,
                'error': error_msg,
                'operation_id': operation_id,
                'attempts': pack.pos_sync_attempts,
                'max_attempts': max_attempts
            }
    
    
    def _validate_seat_pack_for_pos(self, seat_pack: SeatPack) -> Dict[str, Any]:
        """
        Validate seat pack before sending to POS to prevent ghost packs
        
        Args:
            seat_pack: The SeatPack to validate
            
        Returns:
            Dictionary with validation result and reason
        """
        try:
            # Check if seat pack has active pack status
            if seat_pack.pack_status != 'active':
                return {
                    'is_valid': False,
                    'reason': f'Seat pack is not active (status: {seat_pack.pack_status})'
                }
            
            # Check if seat pack has valid pricing
            if not seat_pack.pack_price or seat_pack.pack_price <= 0:
                return {
                    'is_valid': False,
                    'reason': f'Invalid pack price: {seat_pack.pack_price}'
                }
            
            # Check if seat pack has valid size
            if not seat_pack.pack_size or seat_pack.pack_size <= 0:
                return {
                    'is_valid': False,
                    'reason': f'Invalid pack size: {seat_pack.pack_size}'
                }
            
            # Check if seat pack has valid seat keys
            if not seat_pack.seat_keys or len(seat_pack.seat_keys) == 0:
                return {
                    'is_valid': False,
                    'reason': 'No seat keys found'
                }
            
            # Check if seat pack has valid row and seat numbers
            if not seat_pack.row_label or not seat_pack.start_seat_number:
                return {
                    'is_valid': False,
                    'reason': 'Missing row or seat number information'
                }
            
            # Validate seat pack has associated zone and performance
            if not seat_pack.zone_id or not seat_pack.zone_id.performance_id:
                return {
                    'is_valid': False,
                    'reason': 'Missing zone or performance association'
                }
            
            # Special validation for balcony seats (common ghost pack source)
            if hasattr(seat_pack.zone_id, 'name') and 'balc' in seat_pack.zone_id.name.lower():
                # Extra validation for balcony seats
                if not seat_pack.level or not hasattr(seat_pack.level, 'name'):
                    return {
                        'is_valid': False,
                        'reason': 'Balcony seat pack missing level information'
                    }
                
                # Ensure the seat pack size matches the seat keys count
                if len(seat_pack.seat_keys) != seat_pack.pack_size:
                    return {
                        'is_valid': False,
                        'reason': f'Balcony seat pack size mismatch: {seat_pack.pack_size} != {len(seat_pack.seat_keys)}'
                    }
            
            # Check if seat pack was manually delisted using four-dimensional model
            if (hasattr(seat_pack, 'delist_reason') and 
                seat_pack.delist_reason == 'manual_delist' and
                hasattr(seat_pack, 'manually_delisted_by') and 
                seat_pack.manually_delisted_by):
                return {
                    'is_valid': False,
                    'reason': f'Seat pack was manually delisted by {seat_pack.manually_delisted_by}'
                }
            
            # All validations passed
            return {
                'is_valid': True,
                'reason': 'All validations passed'
            }
            
        except Exception as e:
            logger.error(f"Error validating seat pack {seat_pack.internal_pack_id}: {str(e)}")
            return {
                'is_valid': False,
                'reason': f'Validation error: {str(e)}'
            }

    def _extract_inventory_id_from_response(self, pos_response) -> Optional[str]:
        """
        Extract inventory ID from StubHub POS API response
        
        Args:
            pos_response: POSAPIResponse object from POS API
            
        Returns:
            Inventory ID string or None if not found
        """
        # Handle POSAPIResponse object
        if not pos_response.is_successful:
            logger.error(f"POS API call failed: {pos_response.error}")
            return None
            
        response_data = pos_response.data
        if not response_data:
            logger.warning("No data in POS API response")
            return None
        
        # Common field names to check for inventory ID
        possible_fields = ['id', 'inventoryId', 'inventory_id', 'listingId', 'listing_id']
        
        for field in possible_fields:
            if field in response_data:
                return str(response_data[field])
        
        # Check nested objects
        if 'data' in response_data:
            for field in possible_fields:
                if field in response_data['data']:
                    return str(response_data['data'][field])
        
        logger.warning(f"Could not extract inventory ID from response: {response_data}")
        return None
    
    def _create_pos_listing(self, seat_pack: SeatPack, stubhub_inventory_id: str) -> POSListing:
        """
        Create a POSListing record and link it to the seat pack
        
        Args:
            seat_pack: The SeatPack to create listing for
            stubhub_inventory_id: The inventory ID returned from StubHub API
            
        Returns:
            Created POSListing instance
        """
        try:
            # Get performance from seat pack
            performance = seat_pack.zone_id.performance_id
            
            # Create POSListing record
            pos_listing = POSListing.objects.create(
                performance=performance,
                pos_inventory_id=f"sync_{stubhub_inventory_id}",  # Prefix to indicate sync creation
                stubhub_inventory_id=stubhub_inventory_id,
                status='ACTIVE'
            )
            
            # Link seat pack to the POS listing
            seat_pack.pos_listing = pos_listing
            seat_pack.save(update_fields=['pos_listing'])
            
            logger.info(f"Created POS listing {pos_listing.pos_listing_id} for seat pack {seat_pack.internal_pack_id}")
            
            return pos_listing
            
        except Exception as e:
            logger.error(f"Error creating POS listing for seat pack {seat_pack.internal_pack_id}: {str(e)}", exc_info=True)
            raise
    
    def get_inventory_creation_status(self, performance_id: str) -> Dict[str, Any]:
        """
        Get the current inventory creation status for a performance
        
        Args:
            performance_id: Internal performance ID
            
        Returns:
            Dictionary with inventory creation status
        """
        try:
            performance = Performance.objects.get(internal_performance_id=performance_id)
            
            total_seat_packs = SeatPack.objects.filter(
                zone_id__performance_id=performance,
                source_website=self.source_website,
                pack_status='active'
            ).count()
            
            with_inventory = SeatPack.objects.filter(
                zone_id__performance_id=performance,
                source_website=self.source_website,
                pack_status='active',
                pos_status='active',
                pos_listing__isnull=False,
                pos_listing__stubhub_inventory_id__isnull=False
            ).count()
            
            return {
                'performance_id': performance_id,
                'total_active_seat_packs': total_seat_packs,
                'with_inventory': with_inventory,
                'without_inventory': total_seat_packs - with_inventory,
                'inventory_coverage': (with_inventory / total_seat_packs * 100) if total_seat_packs > 0 else 0
            }
            
        except Performance.DoesNotExist:
            return {
                'error': f"Performance {performance_id} not found"
            }
        except Exception as e:
            logger.error(f"Error getting inventory status for performance {performance_id}: {str(e)}")
            return {
                'error': f"Error getting inventory status: {str(e)}"
            }
    
    def _send_pos_sync_start_notification(self, performance_id: str, total_packs: int, operation_id: str = None) -> bool:
        """
        Send POS sync start notification to backend
        
        Args:
            performance_id: Internal performance ID
            total_packs: Total number of packs to sync
            operation_id: Optional operation ID for tracking (generated if not provided)
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            # Get performance and event details
            performance = Performance.objects.select_related('event_id', 'venue_id').get(
                internal_performance_id=performance_id
            )
            
            # Generate operation ID for tracking (if not provided)
            if not operation_id:
                operation_id = f"sync_{performance_id}_{timezone.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Send start notification
            message_data = {
                'pattern': 'pos.sync.success',
                'data': {
                    'operationId': operation_id,
                    'performanceId': performance_id,
                    'venue': performance.venue_id.name if performance.venue_id else 'Unknown Venue',
                    'syncResults': {
                        'totalPacks': total_packs,
                        'processed': 0,
                        'errors': 0,
                        'message': f"Starting POS sync for {total_packs} seat packs"
                    },
                    'syncType': 'started',
                    'eventTitle': performance.event_id.name if performance.event_id else 'Unknown Event',
                    'packsProcessed': 0
                }
            }
            
            logger.info(f"Sending POS sync start notification for performance {performance_id}: {total_packs} packs")
            
            # Send the message via RabbitMQ
            message_sent = producer.send_message(message_data)
            
            if message_sent:
                logger.info(f"Successfully sent POS sync start notification for performance {performance_id}")
                return True
            else:
                logger.error(f"Failed to send POS sync start notification for performance {performance_id}")
                return False
                
        except Performance.DoesNotExist:
            logger.error(f"Performance {performance_id} not found when sending sync start notification")
            return False
        except Exception as e:
            logger.error(f"Error sending POS sync start notification for performance {performance_id}: {str(e)}", exc_info=True)
            return False
    
    def _send_pos_sync_completion_message(self, performance_id: str, success: bool, sync_results: Dict[str, Any], error_message: Optional[str] = None, operation_id: str = None) -> bool:
        """
        Send POS sync completion message to backend matching NestJS interface requirements
        
        Args:
            performance_id: Internal performance ID
            success: Whether the sync was successful
            sync_results: Dictionary with sync results (processed, errors, etc.)
            error_message: Error message if sync failed
            operation_id: Optional operation ID for tracking (generated if not provided)
            
        Returns:
            bool: True if message sent successfully, False otherwise
        """
        try:
            # Get performance and event details
            performance = Performance.objects.select_related('event_id', 'venue_id').get(
                internal_performance_id=performance_id
            )
            
            # Generate operation ID for tracking (if not provided)
            if not operation_id:
                operation_id = f"sync_{performance_id}_{timezone.now().strftime('%Y%m%d_%H%M%S')}"
            processed_packs = sync_results.get('created', 0) + sync_results.get('deleted', 0)
            
            if success:
                # Send success message matching POSSyncSuccessData interface
                message_data = {
                    'pattern': 'pos.sync.success',
                    'data': {
                        'operationId': operation_id,
                        'performanceId': performance_id,
                        'venue': performance.venue_id.name if performance.venue_id else 'Unknown Venue',
                        'syncResults': {
                            'totalPacks': sync_results.get('total_packs', processed_packs + sync_results.get('failed', 0)),
                            'processed': processed_packs,
                            'errors': sync_results.get('failed', 0),
                            'message': f"Successfully processed {processed_packs} seat packs ({sync_results.get('created', 0)} created, {sync_results.get('deleted', 0)} deleted)"
                        },
                        'syncType': 'ended',
                        'eventTitle': performance.event_id.name if performance.event_id else 'Unknown Event',
                        'packsProcessed': processed_packs
                    }
                }
                
                logger.info(f"Sending POS sync success completion message for performance {performance_id}: processed={processed_packs}, failed={sync_results.get('failed', 0)}")
            else:
                # Send error message matching POSSyncErrorData interface
                total_attempted = sync_results.get('total_packs', processed_packs + sync_results.get('failed', 0))
                message_data = {
                    'pattern': 'pos.sync.error',
                    'data': {
                        'operationId': operation_id,
                        'performanceId': performance_id,
                        'venue': performance.venue_id.name if performance.venue_id else 'Unknown Venue',
                        'errorMessage': error_message or f'POS sync failed with {sync_results.get("failed", 0)} errors out of {total_attempted} total packs',
                        'syncType': 'ended',
                        'eventTitle': performance.event_id.name if performance.event_id else 'Unknown Event',
                        'packsAttempted': total_attempted,
                        'packsSuccessful': processed_packs
                    }
                }
                
                logger.warning(f"Sending POS sync error completion message for performance {performance_id}: attempted={total_attempted}, successful={processed_packs}, failed={sync_results.get('failed', 0)}")
            
            # Send the message via RabbitMQ
            message_sent = producer.send_message(message_data)
            
            if message_sent:
                status = 'success' if success else 'error'
                logger.info(f"✅ Successfully sent POS sync {status} completion message for performance {performance_id}")
                return True
            else:
                logger.error(f"❌ Failed to send POS sync completion message for performance {performance_id}")
                return False
                
        except Performance.DoesNotExist:
            logger.error(f"❌ Performance {performance_id} not found when sending sync completion message")
            return False
        except Exception as e:
            logger.error(f"❌ Error sending POS sync completion message for performance {performance_id}: {str(e)}", exc_info=True)
            return False
    

def sync_pending_packs_to_pos(performance_id: str = None, source_website: str = None) -> Dict[str, Any]:
    """
    High-level function to sync all pending seat packs to POS.
    
    Args:
        performance_id: Optional performance ID to limit sync scope
        source_website: Optional source website identifier
        
    Returns:
        Dictionary with sync results
    """
    creator = StubHubInventoryCreator(source_website)
    return creator.sync_pending_packs(performance_id)


def is_inventory_creation_enabled() -> bool:
    """
    Check if inventory creation is enabled in the current environment
    
    Returns:
        Boolean indicating if inventory creation is enabled
    """
    return (
        hasattr(settings, 'STUBHUB_POS_BASE_URL') and 
        settings.STUBHUB_POS_BASE_URL and
        hasattr(settings, 'STUBHUB_POS_AUTH_TOKEN')
    )