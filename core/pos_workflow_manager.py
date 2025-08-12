"""
POS Workflow Manager - Main Integration for POS Sync Workflow

This module provides the main integration point for the complete POS sync workflow,
orchestrating seat pack generation, comparison, saving, and POS synchronization.
"""

import logging
from typing import Dict, Any, Optional, List
from django.db import transaction
from django.core.exceptions import ValidationError

from .seat_pack_generator import generate_and_compare_seat_packs
from .seat_pack_saver import save_new_seat_packs_with_lineage
from .pos_config_handler import extract_pos_configuration, POSConfiguration
from .pos_sync_service import sync_seat_packs_with_pos, POSSyncResult
from .pos_status_tracker import track_pos_workflow_completion, POSStatusUpdate
from .seat_pack_comparator import SeatPackComparison

logger = logging.getLogger(__name__)


class POSWorkflowResult:
    """Complete result of POS workflow execution"""
    
    def __init__(self):
        self.success: bool = False
        self.performance_id: str = ""
        self.source_website: str = ""
        self.workflow_id: str = ""
        
        # Phase results
        self.comparison: Optional[SeatPackComparison] = None
        self.save_results: Dict[str, Any] = {}
        self.sync_result: Optional[POSSyncResult] = None
        self.status_updates: Dict[str, POSStatusUpdate] = {}
        
        # Configuration used
        self.pos_config: Optional[POSConfiguration] = None
        
        # Summary statistics
        self.total_new_packs: int = 0
        self.total_saved_packs: int = 0
        self.total_synced_packs: int = 0
        self.total_errors: int = 0
        
        # User-friendly messages
        self.summary_message: str = ""
        self.detailed_messages: List[str] = []
        self.error_messages: List[str] = []
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'success': self.success,
            'performance_id': self.performance_id,
            'source_website': self.source_website,
            'workflow_id': self.workflow_id,
            'summary': {
                'total_new_packs': self.total_new_packs,
                'total_saved_packs': self.total_saved_packs,
                'total_synced_packs': self.total_synced_packs,
                'total_errors': self.total_errors
            },
            'messages': {
                'summary': self.summary_message,
                'detailed': self.detailed_messages,
                'errors': self.error_messages
            },
            'comparison_results': self.comparison.to_dict() if self.comparison else {},
            'save_results': self.save_results,
            'sync_results': self.sync_result.to_dict() if self.sync_result else {},
            'status_updates': {
                key: update.to_dict() for key, update in self.status_updates.items()
            },
            'pos_config': self.pos_config.to_dict() if self.pos_config else {}
        }


class POSWorkflowManager:
    """
    Main workflow manager for the complete POS sync process
    """
    
    def __init__(self, performance_id: str, source_website: str, scrape_job_id: str):
        """
        Initialize the POS workflow manager
        
        Args:
            performance_id: Internal performance ID
            source_website: Source website identifier
            scrape_job_id: Current scrape job ID
        """
        self.performance_id = performance_id
        self.source_website = source_website
        self.scrape_job_id = scrape_job_id
        
    def execute_complete_workflow(
        self,
        scraped_data: Dict[str, Any],
        enriched_data: Dict[str, Any],
        venue_prefix_map: Optional[Dict[str, str]] = None,
        force_sync_mode: Optional[str] = None
    ) -> POSWorkflowResult:
        """
        Execute the complete POS workflow from seat pack generation to POS sync
        
        Args:
            scraped_data: Raw scraped data (seats, sections, performance info)
            enriched_data: Enriched data from NestJS (event, venue, POS config)
            venue_prefix_map: Mapping of venue prefixes for pack ID generation
            force_sync_mode: Override sync mode ('immediate' or 'on_demand')
            
        Returns:
            POSWorkflowResult with complete workflow results
        """
        import uuid
        workflow_id = str(uuid.uuid4())
        
        logger.info(f"Starting complete POS workflow {workflow_id} for performance {self.performance_id}")
        
        result = POSWorkflowResult()
        result.performance_id = self.performance_id
        result.source_website = self.source_website
        result.workflow_id = workflow_id
        
        try:
            # Step 1: Extract POS Configuration
            logger.info("Step 1: Extracting POS configuration")
            result.pos_config = self._extract_pos_configuration(enriched_data)
            
            # Step 2: Generate and Compare Seat Packs
            logger.info("Step 2: Generating and comparing seat packs")
            result.comparison = self._generate_and_compare_packs(
                scraped_data, venue_prefix_map
            )
            
            # Step 3: Save New Seat Packs with Lineage
            logger.info("Step 3: Saving new seat packs with lineage tracking")
            result.save_results = self._save_seat_packs_with_lineage(result.comparison)
            
            # Step 4: Execute POS Sync (if enabled)
            logger.info("Step 4: Executing POS sync operations")
            result.sync_result = self._execute_pos_sync(
                result.comparison, result.pos_config, force_sync_mode
            )
            
            # Step 5: Track Status and Generate Messages
            logger.info("Step 5: Tracking status and generating messages")
            result.status_updates = self._track_workflow_status(
                enriched_data, result.comparison, result.save_results, 
                result.sync_result, result.pos_config
            )
            
            # Step 6: Build Summary Results
            self._build_workflow_summary(result)
            
            # Step 7: Send POS Sync Completion Notification to NestJS
            logger.info("Step 7: Sending POS sync completion notification")
            self._send_sync_completion_notification(result, enriched_data)
            
            result.success = result.total_errors == 0
            logger.info(f"POS workflow {workflow_id} completed: success={result.success}")
            
        except Exception as e:
            logger.error(f"POS workflow {workflow_id} failed: {e}", exc_info=True)
            result.success = False
            result.error_messages.append(f"Workflow failed: {e}")
            result.total_errors += 1
        
        return result
    
    def execute_generation_only(
        self,
        scraped_data: Dict[str, Any],
        venue_prefix_map: Optional[Dict[str, str]] = None
    ) -> POSWorkflowResult:
        """
        Execute only seat pack generation and saving (no POS sync)
        
        Args:
            scraped_data: Raw scraped data
            venue_prefix_map: Venue prefix mapping
            
        Returns:
            POSWorkflowResult with generation results only
        """
        import uuid
        workflow_id = str(uuid.uuid4())
        
        logger.info(f"Starting generation-only workflow {workflow_id}")
        
        result = POSWorkflowResult()
        result.performance_id = self.performance_id
        result.source_website = self.source_website
        result.workflow_id = workflow_id
        
        try:
            # Generate and compare seat packs
            result.comparison = self._generate_and_compare_packs(
                scraped_data, venue_prefix_map
            )
            
            # Save new seat packs
            result.save_results = self._save_seat_packs_with_lineage(result.comparison)
            
            # Build summary
            result.total_new_packs = len(result.comparison.new_packs)
            result.total_saved_packs = result.save_results.get('saved_count', 0)
            result.total_errors = len(result.save_results.get('errors', []))
            
            result.success = result.total_errors == 0
            
        except Exception as e:
            logger.error(f"Generation workflow {workflow_id} failed: {e}", exc_info=True)
            result.success = False
            result.error_messages.append(f"Generation failed: {e}")
            result.total_errors += 1
        
        return result
    
    def execute_sync_only(
        self,
        enriched_data: Dict[str, Any],
        sync_mode: Optional[str] = None
    ) -> POSWorkflowResult:
        """
        Execute only POS sync for existing packs (no generation)
        
        Args:
            enriched_data: Enriched data from NestJS
            sync_mode: Override sync mode
            
        Returns:
            POSWorkflowResult with sync results only
        """
        import uuid
        workflow_id = str(uuid.uuid4())
        
        logger.info(f"Starting sync-only workflow {workflow_id}")
        
        result = POSWorkflowResult()
        result.performance_id = self.performance_id
        result.source_website = self.source_website
        result.workflow_id = workflow_id
        
        try:
            # Extract POS configuration
            result.pos_config = self._extract_pos_configuration(enriched_data)
            
            # Create empty comparison for existing packs
            from .seat_pack_comparator import SeatPackComparator
            comparator = SeatPackComparator(self.performance_id, self.source_website)
            result.comparison = comparator.compare_seat_packs([])  # Empty new packs
            
            # Execute POS sync
            result.sync_result = self._execute_pos_sync(
                result.comparison, result.pos_config, sync_mode
            )
            
            # Build summary
            result.total_synced_packs = result.sync_result.processed_count
            result.total_errors = result.sync_result.failed_count
            
            result.success = result.total_errors == 0
            
        except Exception as e:
            logger.error(f"Sync workflow {workflow_id} failed: {e}", exc_info=True)
            result.success = False
            result.error_messages.append(f"Sync failed: {e}")
            result.total_errors += 1
        
        return result
    
    def _extract_pos_configuration(self, enriched_data: Dict[str, Any]) -> POSConfiguration:
        """Extract and validate POS configuration"""
        try:
            config = extract_pos_configuration(
                enriched_data, self.performance_id, self.source_website
            )
            logger.debug(f"POS configuration extracted: enabled={config.pos_enabled}")
            return config
        except Exception as e:
            logger.warning(f"Failed to extract POS configuration: {e}")
            # Return default configuration
            from .pos_config_handler import POSConfigurationHandler
            handler = POSConfigurationHandler(self.performance_id, self.source_website)
            return handler.get_default_configuration()
    
    def _generate_and_compare_packs(
        self, 
        scraped_data: Dict[str, Any],
        venue_prefix_map: Optional[Dict[str, str]]
    ) -> SeatPackComparison:
        """Generate seat packs and compare with existing ones"""
        comparison = generate_and_compare_seat_packs(
            scraped_data=scraped_data,
            performance_id=self.performance_id,
            source_website=self.source_website,
            venue_prefix_map=venue_prefix_map or {self.source_website: "sp"}
        )
        
        logger.info(f"Generated {len(comparison.new_packs)} new packs, "
                   f"{len(comparison.unchanged_packs)} unchanged, "
                   f"{len(comparison.removed_packs)} removed")
        
        return comparison
    
    def _save_seat_packs_with_lineage(self, comparison: SeatPackComparison) -> Dict[str, Any]:
        """Save new seat packs with lineage tracking"""
        save_results = save_new_seat_packs_with_lineage(
            comparison=comparison,
            performance_id=self.performance_id,
            source_website=self.source_website,
            scrape_job_id=self.scrape_job_id
        )
        
        logger.info(f"Saved {save_results.get('saved_count', 0)} new seat packs, "
                   f"updated {save_results.get('removed_count', 0)} existing packs")
        
        return save_results
    
    def _execute_pos_sync(
        self, 
        comparison: SeatPackComparison,
        config: POSConfiguration,
        force_sync_mode: Optional[str]
    ) -> POSSyncResult:
        """Execute POS sync operations"""
        sync_result = sync_seat_packs_with_pos(
            comparison=comparison,
            config=config,
            sync_mode=force_sync_mode
        )
        
        logger.info(f"POS sync completed: {sync_result.processed_count} processed, "
                   f"{sync_result.failed_count} failed")
        
        return sync_result
    
    def _track_workflow_status(
        self,
        enriched_data: Dict[str, Any],
        comparison: SeatPackComparison,
        save_results: Dict[str, Any],
        sync_result: POSSyncResult,
        config: POSConfiguration
    ) -> Dict[str, POSStatusUpdate]:
        """Track workflow status and generate messages"""
        status_updates = track_pos_workflow_completion(
            performance_id=self.performance_id,
            source_website=self.source_website,
            enriched_data=enriched_data,
            comparison=comparison,
            save_results=save_results,
            sync_result=sync_result,
            config=config
        )
        
        logger.debug("Status tracking completed")
        return status_updates
    
    def _build_workflow_summary(self, result: POSWorkflowResult):
        """Build summary statistics and messages for the workflow"""
        # Calculate summary statistics
        if result.comparison:
            result.total_new_packs = len(result.comparison.new_packs)
        
        result.total_saved_packs = result.save_results.get('saved_count', 0)
        
        if result.sync_result:
            result.total_synced_packs = result.sync_result.processed_count
            result.total_errors += result.sync_result.failed_count
        
        result.total_errors += len(result.save_results.get('errors', []))
        
        # Build summary message
        if result.success:
            result.summary_message = (
                f"POS workflow completed successfully: "
                f"{result.total_saved_packs} packs saved, "
                f"{result.total_synced_packs} packs synced"
            )
        else:
            result.summary_message = (
                f"POS workflow completed with {result.total_errors} error(s)"
            )
        
        # Collect detailed messages from status updates
        for phase, update in result.status_updates.items():
            if update.detailed_messages:
                result.detailed_messages.extend([
                    f"[{phase.title()}] {msg}" for msg in update.detailed_messages
                ])
            if update.error_messages:
                result.error_messages.extend([
                    f"[{phase.title()}] {msg}" for msg in update.error_messages
                ])

    def _send_sync_completion_notification(self, result: POSWorkflowResult, enriched_data: Dict[str, Any]):
        """Send POS sync completion notification to NestJS backend"""
        try:
            from consumer.notification_helpers import notify_pos_sync_completed
            
            # Extract event and venue information from enriched data
            event_info = enriched_data.get('event', {})
            venue_info = enriched_data.get('venue', {})
            performance_info = enriched_data.get('performance', {})
            
            event_title = event_info.get('name', 'Unknown Event')
            venue = venue_info.get('name', 'Unknown Venue')
            performance_date = performance_info.get('event_date')
            
            # Prepare sync results data
            sync_results = {
                'processedCount': result.sync_result.processed_count if result.sync_result else 0,
                'pushedCount': result.sync_result.pushed_count if result.sync_result else 0,
                'delistedCount': result.sync_result.delisted_count if result.sync_result else 0,
                'failedCount': result.sync_result.failed_count if result.sync_result else 0,
                'errors': result.sync_result.errors if result.sync_result else [],
                'totalPacks': result.total_new_packs + (result.comparison.unchanged_packs.__len__() if result.comparison else 0),
            }
            
            # Prepare workflow summary
            workflow_summary = {
                'totalNewPacks': result.total_new_packs,
                'totalSavedPacks': result.total_saved_packs,
                'totalSyncedPacks': result.total_synced_packs,
                'totalErrors': result.total_errors,
                'summaryMessage': result.summary_message,
            }
            
            # Send the completion notification
            success = notify_pos_sync_completed(
                scrape_job_id=self.scrape_job_id,
                performance_id=result.performance_id,
                operation_id=result.workflow_id,
                success=result.success,
                venue=venue,
                event_title=event_title,
                performance_date=performance_date,
                sync_results=sync_results,
                workflow_summary=workflow_summary
            )
            
            if success:
                logger.info(f"Successfully sent POS sync completion notification for workflow {result.workflow_id}")
            else:
                logger.error(f"Failed to send POS sync completion notification for workflow {result.workflow_id}")
                
        except Exception as e:
            logger.error(f"Error sending POS sync completion notification: {e}", exc_info=True)


def execute_pos_workflow(
    performance_id: str,
    source_website: str,
    scrape_job_id: str,
    scraped_data: Dict[str, Any],
    enriched_data: Dict[str, Any],
    venue_prefix_map: Optional[Dict[str, str]] = None,
    workflow_mode: str = 'complete',
    force_sync_mode: Optional[str] = None
) -> POSWorkflowResult:
    """
    Convenience function for executing POS workflows
    
    Args:
        performance_id: Internal performance ID
        source_website: Source website identifier
        scrape_job_id: Current scrape job ID
        scraped_data: Raw scraped data
        enriched_data: Enriched data from NestJS
        venue_prefix_map: Venue prefix mapping
        workflow_mode: 'complete', 'generation_only', or 'sync_only'
        force_sync_mode: Override sync mode
        
    Returns:
        POSWorkflowResult with workflow execution results
    """
    manager = POSWorkflowManager(performance_id, source_website, scrape_job_id)
    
    if workflow_mode == 'generation_only':
        return manager.execute_generation_only(scraped_data, venue_prefix_map)
    elif workflow_mode == 'sync_only':
        return manager.execute_sync_only(enriched_data, force_sync_mode)
    else:  # complete
        return manager.execute_complete_workflow(
            scraped_data, enriched_data, venue_prefix_map, force_sync_mode
        )