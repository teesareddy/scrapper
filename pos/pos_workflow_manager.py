"""
POS Workflow Manager

Main orchestrator for the POS seat pack workflow. This class coordinates
all components to provide a clean, unified interface for POS operations
following the documentation patterns and handling both initial and
subsequent scrape scenarios.
"""

from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
import logging
import uuid
from django.db import transaction
from django.utils import timezone
from datetime import datetime

# Import notification helpers
try:
    from consumer.notification_helpers import notify_pos_sync_success, notify_pos_sync_error
except ImportError:
    logger.warning("Notification helpers not available - notifications will be disabled")
    notify_pos_sync_success = lambda *args, **kwargs: False
    notify_pos_sync_error = lambda *args, **kwargs: False

logger = logging.getLogger(__name__)


@dataclass
class WorkflowResult:
    """Result of a complete POS workflow execution"""
    success: bool
    scenario: str  # 'initial_scrape', 'subsequent_scrape'
    performance_id: str
    total_packs_processed: int
    packs_created: int
    packs_updated: int
    packs_delisted: int
    packs_synced_to_pos: int
    pos_inventories_created: int
    execution_time_seconds: float
    error_messages: List[str]
    warnings: List[str]
    detailed_results: dict


class POSWorkflowManager:
    """
    Main orchestrator for POS seat pack workflows.
    
    This class provides the primary interface for POS operations and coordinates:
    - Diff algorithm execution
    - Pack comparison and transformation analysis
    - Database sync execution
    - POS inventory creation
    - Comprehensive logging and error handling
    
    Supports both initial scrape and subsequent scrape scenarios with
    appropriate safety mechanisms and business logic.
    """
    
    def __init__(self, source_website: str, prefix: str):
        self.source_website = source_website
        self.prefix = prefix
        self.logger = logger
        
        # Initialize components
        from .pos_diff_algorithm import POSDiffAlgorithm
        from .pos_comparator import POSComparator
        from .pos_sync_executor import POSSyncExecutor
        from .pos_inventory_pusher import POSInventoryPusher
        
        self.diff_algorithm = POSDiffAlgorithm()
        self.comparator = POSComparator()
        self.sync_executor = POSSyncExecutor(source_website, prefix)
        self.inventory_pusher = POSInventoryPusher()
    
    @transaction.atomic
    def process_initial_scrape(
        self,
        seat_pack_data: List[dict],
        performance_data: dict,
        event_data: dict,
        venue_data: dict
    ) -> WorkflowResult:
        """
        Process initial scrape scenario - create all seat packs in database.
        
        For first-time scrapes:
        1. Create all seat packs in database
        2. Create POS inventory if POS is enabled
        3. No comparison or delisting needed
        
        Args:
            seat_pack_data: List of seat pack data from scraping
            performance_data: Performance information
            event_data: Event information
            venue_data: Venue information
            
        Returns:
            WorkflowResult with detailed execution results
        """
        start_time = timezone.now()
        performance_id = performance_data.get('internal_performance_id', 'unknown')
        pos_enabled = performance_data.get('pos_enabled', False)
        
        self.logger.info(
            f"🎯 INITIAL SCRAPE WORKFLOW for performance {performance_id} "
            f"({len(seat_pack_data)} packs, POS enabled: {pos_enabled})"
        )
        
        warnings = []
        error_messages = []
        
        try:
            # Validate input data
            if not seat_pack_data:
                warning_msg = "No seat pack data provided for initial scrape"
                self.logger.warning(f"⚠️ {warning_msg}")
                warnings.append(warning_msg)
                
                return self._create_workflow_result(
                    success=True,
                    scenario='initial_scrape',
                    performance_id=performance_id,
                    start_time=start_time,
                    warnings=warnings,
                    error_messages=error_messages
                )
            
            # Step 1: Execute initial scrape (create all packs)
            self.logger.info(f"📝 STEP 1: Creating {len(seat_pack_data)} seat packs in database")
            
            sync_result = self.sync_executor.execute_initial_scrape(
                seat_pack_data, performance_data, event_data, venue_data
            )
            
            if sync_result.failed_actions > 0:
                error_messages.extend(sync_result.errors)
                self.logger.warning(f"⚠️ Some pack creation failed: {sync_result.failed_actions} failures")
            
            # Step 2: Process POS inventory operations
            pos_inventories_created = 0
            if pos_enabled:
                self.logger.info("🎫 STEP 2: Processing POS inventory operations")
                
                # 2a: Create inventory for any new packs
                if sync_result.created_packs > 0:
                    self.logger.info(f"  Creating inventory for {sync_result.created_packs} new packs")
                    
                    # Get created pack data for inventory creation
                    created_pack_data = [
                        result.pack_id for result in sync_result.execution_results 
                        if result.success and result.action_type == 'create'
                    ]
                    
                    if created_pack_data:
                        inventory_result = self.inventory_pusher.create_bulk_inventory(
                            seat_pack_data[:sync_result.created_packs],  # Match successful creations
                            performance_data, event_data, venue_data
                        )
                        
                        pos_inventories_created += inventory_result.successful_creations
                        
                        if inventory_result.failed_creations > 0:
                            warnings.append(f"{inventory_result.failed_creations} new pack inventory creations failed")
                            error_messages.extend(inventory_result.errors[:3])
                    else:
                        warnings.append("No successful pack creations found for POS inventory")
                
                # 2b: Also sync any existing pending packs (in case there were pre-existing packs)
                self.logger.info(f"  Checking for existing pending packs to sync")
                pending_packs_synced = self._sync_pending_packs_to_pos(performance_id, performance_data, event_data, venue_data, pos_enabled)
                pos_inventories_created += pending_packs_synced
                
                if pos_inventories_created > 0:
                    self.logger.info(f"✅ TOTAL POS INVENTORY: {pos_inventories_created} inventories processed")
                else:
                    self.logger.info("ℹ️ No POS inventory operations needed")
            else:
                self.logger.info("ℹ️ POS not enabled, skipping inventory operations")
            
            # Calculate final results
            end_time = timezone.now()
            execution_time = (end_time - start_time).total_seconds()
            
            success = sync_result.failed_actions == 0
            
            result = WorkflowResult(
                success=success,
                scenario='initial_scrape',
                performance_id=performance_id,
                total_packs_processed=len(seat_pack_data),
                packs_created=sync_result.created_packs,
                packs_updated=0,  # No updates in initial scrape
                packs_delisted=0,  # No delisting in initial scrape
                packs_synced_to_pos=0,  # No existing packs to sync
                pos_inventories_created=pos_inventories_created,
                execution_time_seconds=execution_time,
                error_messages=error_messages,
                warnings=warnings,
                detailed_results={
                    'sync_result': sync_result,
                    'pos_enabled': pos_enabled
                }
            )
            
            self.logger.info(
                f"✅ INITIAL SCRAPE COMPLETE: {sync_result.created_packs} packs created, "
                f"{pos_inventories_created} POS inventories in {execution_time:.2f}s"
            )
            
            return result
            
        except Exception as e:
            error_msg = f"Initial scrape workflow failed: {str(e)}"
            self.logger.error(f"💥 WORKFLOW ERROR: {error_msg}", exc_info=True)
            error_messages.append(error_msg)
            
            return self._create_workflow_result(
                success=False,
                scenario='initial_scrape',
                performance_id=performance_id,
                start_time=start_time,
                warnings=warnings,
                error_messages=error_messages
            )
    
    @transaction.atomic
    def process_subsequent_scrape(
        self,
        seat_pack_data: List[dict],
        performance_data: dict,
        event_data: dict,
        venue_data: dict
    ) -> WorkflowResult:
        """
        Process subsequent scrape scenario - compare with existing packs and sync changes.
        
        For subsequent scrapes:
        1. Get existing active seat packs from database
        2. Run diff algorithm to identify changes
        3. Execute sync plan (create new, update changed, delist removed)
        4. Create POS inventory for new packs
        5. Sync unchanged packs to POS if needed
        
        Args:
            seat_pack_data: List of seat pack data from scraping
            performance_data: Performance information
            event_data: Event information
            venue_data: Venue information
            
        Returns:
            WorkflowResult with detailed execution results
        """
        start_time = timezone.now()
        performance_id = performance_data.get('internal_performance_id', 'unknown')
        pos_enabled = performance_data.get('pos_enabled', False)
        
        self.logger.info(
            f"🔄 SUBSEQUENT SCRAPE WORKFLOW for performance {performance_id} "
            f"({len(seat_pack_data)} new packs, POS enabled: {pos_enabled})"
        )
        
        warnings = []
        error_messages = []
        
        try:
            # Step 1: Get existing active seat packs
            self.logger.info("📋 STEP 1: Retrieving existing seat packs from database")
            existing_packs = self._get_existing_active_packs(performance_id)
            
            self.logger.info(f"Found {len(existing_packs)} existing active packs")
            
            # Step 2: Run diff algorithm
            self.logger.info("🔍 STEP 2: Running diff algorithm")
            sync_plan = self.diff_algorithm.diff_seat_packs(
                existing_packs=existing_packs,
                newly_generated_packs=seat_pack_data,
                performance_pos_enabled=pos_enabled,
                performance_id=performance_id
            )
            
            # Step 3: Execute sync plan
            self.logger.info(f"⚡ STEP 3: Executing sync plan ({sync_plan.total_actions()} actions)")
            sync_result = self.sync_executor.execute_sync_plan(
                sync_plan, performance_data, is_initial_scrape=False
            )
            
            if sync_result.failed_actions > 0:
                error_messages.extend(sync_result.errors)
                self.logger.warning(f"⚠️ Some sync actions failed: {sync_result.failed_actions} failures")
            
            # Step 4: Create POS inventory for new packs AND sync existing pending packs
            pos_inventories_created = 0
            if pos_enabled:
                self.logger.info(f"🎫 STEP 4: Processing POS inventory operations")
                
                # 4a: Create inventory for any new packs
                if sync_result.created_packs > 0:
                    self.logger.info(f"  Creating inventory for {sync_result.created_packs} new packs")
                    new_pack_data = [action.pack_data for action in sync_plan.creation_actions]
                    
                    if new_pack_data:
                        inventory_result = self.inventory_pusher.create_bulk_inventory(
                            new_pack_data, performance_data, event_data, venue_data
                        )
                        
                        pos_inventories_created += inventory_result.successful_creations
                        
                        if inventory_result.failed_creations > 0:
                            warnings.append(f"{inventory_result.failed_creations} new pack inventory creations failed")
                            error_messages.extend(inventory_result.errors[:3])
                
                # 4b: Push existing pending packs to POS inventory
                self.logger.info(f"  Checking for existing packs that need POS sync")
                pending_packs_synced = self._sync_pending_packs_to_pos(performance_id, performance_data, event_data, venue_data, pos_enabled)
                pos_inventories_created += pending_packs_synced
                
                if pos_inventories_created > 0:
                    self.logger.info(f"✅ TOTAL POS INVENTORY: {pos_inventories_created} inventories processed")
                else:
                    self.logger.info("ℹ️ No POS inventory operations needed")
            else:
                self.logger.info("ℹ️ POS not enabled, skipping inventory operations")
            
            # Calculate final results
            end_time = timezone.now()
            execution_time = (end_time - start_time).total_seconds()
            
            success = sync_result.failed_actions == 0
            
            result = WorkflowResult(
                success=success,
                scenario='subsequent_scrape',
                performance_id=performance_id,
                total_packs_processed=len(seat_pack_data),
                packs_created=sync_result.created_packs,
                packs_updated=sync_result.updated_packs,
                packs_delisted=sync_result.delisted_packs,
                packs_synced_to_pos=sync_result.synced_packs,
                pos_inventories_created=pos_inventories_created,
                execution_time_seconds=execution_time,
                error_messages=error_messages,
                warnings=warnings,
                detailed_results={
                    'sync_plan': sync_plan,
                    'sync_result': sync_result,
                    'existing_packs_count': len(existing_packs),
                    'pos_enabled': pos_enabled
                }
            )
            
            self.logger.info(
                f"✅ SUBSEQUENT SCRAPE COMPLETE: {sync_result.created_packs} created, "
                f"{sync_result.updated_packs} updated, {sync_result.delisted_packs} delisted, "
                f"{sync_result.synced_packs} synced, {pos_inventories_created} POS inventories "
                f"in {execution_time:.2f}s"
            )
            
            return result
            
        except Exception as e:
            error_msg = f"Subsequent scrape workflow failed: {str(e)}"
            self.logger.error(f"💥 WORKFLOW ERROR: {error_msg}", exc_info=True)
            error_messages.append(error_msg)
            
            return self._create_workflow_result(
                success=False,
                scenario='subsequent_scrape',
                performance_id=performance_id,
                start_time=start_time,
                warnings=warnings,
                error_messages=error_messages
            )
    
    def process_auto_detect_scenario(
        self,
        seat_pack_data: List[dict],
        performance_data: dict,
        event_data: dict,
        venue_data: dict
    ) -> WorkflowResult:
        """
        Automatically detect whether this is an initial or subsequent scrape.
        
        Args:
            seat_pack_data: List of seat pack data from scraping
            performance_data: Performance information
            event_data: Event information
            venue_data: Venue information
            
        Returns:
            WorkflowResult with detailed execution results
        """
        performance_id = performance_data.get('internal_performance_id', 'unknown')
        
        # Check if any active seat packs exist for this performance
        existing_packs = self._get_existing_active_packs(performance_id)
        is_initial = len(existing_packs) == 0
        
        self.logger.info(
            f"🎯 AUTO-DETECT for {performance_id}: "
            f"{'INITIAL' if is_initial else 'SUBSEQUENT'} scrape "
            f"({len(existing_packs)} existing packs)"
        )
        
        if is_initial:
            return self.process_initial_scrape(
                seat_pack_data, performance_data, event_data, venue_data
            )
        else:
            return self.process_subsequent_scrape(
                seat_pack_data, performance_data, event_data, venue_data
            )
    
    def _get_existing_active_packs(self, performance_id: str) -> List[dict]:
        """
        Get existing active seat packs for a performance.
        
        Returns:
            List of seat pack dictionaries
        """
        try:
            # Import here to avoid circular imports
            from scrapers.models.seat_packs import SeatPack
            
            packs = SeatPack.objects.filter(
                performance=performance_id,  # This maps to internal_performance_id in DB
                pack_status='active'
            ).values(
                'internal_pack_id', 'zone_id', 'row_label',
                'start_seat_number', 'end_seat_number',
                'pack_price', 'pack_size', 'pack_status', 'pos_status',
                'pack_state', 'delist_reason'
            )
            
            return list(packs)
            
        except Exception as e:
            self.logger.error(f"❌ Error getting existing packs: {str(e)}", exc_info=True)
            return []
    
    def _sync_pending_packs_to_pos(
        self,
        performance_id: str,
        performance_data: dict,
        event_data: dict,
        venue_data: dict,
        pos_enabled: bool = True
    ) -> int:
        """
        Sync existing packs with pos_status='pending' to POS inventory.
        
        Returns:
            Number of packs successfully synced to POS
        """
        operation_id = str(uuid.uuid4())
        venue_name = venue_data.get('name', 'Unknown Venue')
        event_title = event_data.get('name', 'Unknown Event')
        
        try:
            from scrapers.models.seat_packs import SeatPack
            
            # Get packs that need POS sync
            pending_packs = SeatPack.objects.filter(
                performance=performance_id,
                pack_status='active',
                pos_status='pending'
            ).values(
                'internal_pack_id', 'zone_id', 'row_label',
                'start_seat_number', 'end_seat_number',
                'pack_price', 'pack_size', 'pack_status', 'pos_status'
            )
            
            pending_count = len(pending_packs)
            
            # Always send POS sync started notification (even if no packs)
            notify_pos_sync_success(
                operation_id=operation_id,
                performance_id=performance_id,
                venue=venue_name,
                sync_results={'message': 'POS sync started', 'pending_packs': pending_count},
                sync_type="started",
                event_title=event_title
            )
            
            if pending_count == 0:
                self.logger.info("  No pending packs found for POS sync")
                # Send completion notification for no-packs case
                notify_pos_sync_success(
                    operation_id=operation_id,
                    performance_id=performance_id,
                    venue=venue_name,
                    sync_results={'total_packs': 0, 'successful': 0, 'failed': 0, 'message': 'No packs needed sync'},
                    sync_type="ended",
                    event_title=event_title
                )
                return 0
            
            self.logger.info(f"  Found {pending_count} pending packs to sync to POS")
            
            # Use existing StubHub inventory creator
            try:
                from scrapers.core.stubhub_inventory_creator import StubHubInventoryCreator
                
                inventory_creator = StubHubInventoryCreator(
                    source_website=self.source_website, 
                    pos_enabled=pos_enabled
                )
                
                # Sync ALL pending packs for this source (not just the specific performance)
                # This ensures we don't miss packs from other performances that need syncing
                self.logger.info(f"  Syncing ALL pending packs for source '{self.source_website}' (not just performance {performance_id})")
                sync_result = inventory_creator.sync_pending_packs()
                
                successful_syncs = sync_result.get('created', 0)
                failed_syncs = sync_result.get('failed', 0)
                
                self.logger.info(f"  POS sync result: {successful_syncs} successful, {failed_syncs} failed")
                
                # Send POS sync completed notification
                notify_pos_sync_success(
                    operation_id=operation_id,
                    performance_id=performance_id,
                    venue=venue_name,
                    sync_results={'total_packs': pending_count, 'successful': successful_syncs, 'failed': failed_syncs},
                    sync_type="ended",
                    event_title=event_title
                )
                
                return successful_syncs
                
            except Exception as e:
                self.logger.error(f"  Error using StubHubInventoryCreator: {e}")
                
                # Send error notification
                notify_pos_sync_error(
                    operation_id=operation_id,
                    performance_id=performance_id,
                    venue=venue_name,
                    error_message=f"StubHub integration error: {str(e)}",
                    sync_type="ended",
                    event_title=event_title,
                    packs_attempted=pending_count,
                    packs_successful=0
                )
                
                # Fallback: use our inventory pusher directly
                return self._fallback_pos_sync(list(pending_packs), performance_data, event_data, venue_data, operation_id)
                
        except Exception as e:
            self.logger.error(f"❌ Error syncing pending packs to POS: {e}", exc_info=True)
            
            # Send error notification
            notify_pos_sync_error(
                operation_id=operation_id,
                performance_id=performance_id,
                venue=venue_name,
                error_message=f"POS sync error: {str(e)}",
                sync_type="ended",
                event_title=event_title,
                packs_attempted=pending_count if 'pending_count' in locals() else 0,
                packs_successful=0
            )
            
            return 0
    
    def _fallback_pos_sync(
        self,
        pending_packs: List[dict],
        performance_data: dict,
        event_data: dict,
        venue_data: dict,
        operation_id: str = None
    ) -> int:
        """
        Fallback method to sync packs using our inventory pusher.
        """
        successful_syncs = 0
        total_packs = len(pending_packs[:10])  # Limit to 10 for testing
        
        for pack in pending_packs[:10]:
            try:
                result = self.inventory_pusher.create_inventory_for_pack(
                    pack, performance_data, event_data, venue_data
                )
                
                if result.success:
                    successful_syncs += 1
                    # Update pack status in database
                    self._update_pack_pos_status(pack['internal_pack_id'], 'active')
                    
            except Exception as e:
                self.logger.error(f"  Failed to sync pack {pack.get('internal_pack_id')}: {e}")
        
        # Send completion notification if operation_id provided
        if operation_id:
            venue_name = venue_data.get('name', 'Unknown Venue')
            event_title = event_data.get('name', 'Unknown Event')
            performance_id = performance_data.get('internal_performance_id', 'unknown')
            
            notify_pos_sync_success(
                operation_id=operation_id,
                performance_id=performance_id,
                venue=venue_name,
                sync_results={'total_packs': total_packs, 'successful': successful_syncs, 'failed': total_packs - successful_syncs},
                sync_type="ended",
                event_title=event_title
            )
        
        return successful_syncs
    
    def _update_pack_pos_status(self, pack_id: str, pos_status: str):
        """Update a pack's POS status in the database."""
        try:
            from scrapers.models.seat_packs import SeatPack
            
            SeatPack.objects.filter(internal_pack_id=pack_id).update(
                pos_status=pos_status,
                updated_at=timezone.now()
            )
            
        except Exception as e:
            self.logger.error(f"Failed to update pack {pack_id} pos_status: {e}")

    def _create_workflow_result(
        self,
        success: bool,
        scenario: str,
        performance_id: str,
        start_time: datetime,
        warnings: List[str] = None,
        error_messages: List[str] = None,
        **kwargs
    ) -> WorkflowResult:
        """Create a WorkflowResult with sensible defaults."""
        end_time = timezone.now()
        execution_time = (end_time - start_time).total_seconds()
        
        return WorkflowResult(
            success=success,
            scenario=scenario,
            performance_id=performance_id,
            total_packs_processed=kwargs.get('total_packs_processed', 0),
            packs_created=kwargs.get('packs_created', 0),
            packs_updated=kwargs.get('packs_updated', 0),
            packs_delisted=kwargs.get('packs_delisted', 0),
            packs_synced_to_pos=kwargs.get('packs_synced_to_pos', 0),
            pos_inventories_created=kwargs.get('pos_inventories_created', 0),
            execution_time_seconds=execution_time,
            error_messages=error_messages or [],
            warnings=warnings or [],
            detailed_results=kwargs.get('detailed_results', {})
        )