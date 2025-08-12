"""
POS Integration Workflow Package

Clean, modular implementation of POS/StubHub seat pack synchronization workflow.
This package provides a complete solution for managing seat pack lifecycle
in the POS system, separate from the existing core scrapers.

Main Components:
- POSWorkflowManager: Main orchestrator for all POS operations
- POSDiffAlgorithm: Intelligent seat pack diffing algorithm
- POSComparator: Advanced pack comparison and transformation detection
- POSSyncExecutor: Database operations with transaction management
- POSInventoryPusher: StubHub inventory creation and management

Usage:
    from scrapers.pos import POSWorkflowManager
    
    # Initialize workflow manager
    pos_manager = POSWorkflowManager(source_website="broadway_sf", prefix="BSF")
    
    # Process initial scrape (creates all packs)
    result = pos_manager.process_initial_scrape(
        seat_pack_data, performance_data, event_data, venue_data
    )
    
    # Process subsequent scrapes (compare and sync changes)
    result = pos_manager.process_subsequent_scrape(
        seat_pack_data, performance_data, event_data, venue_data
    )
    
    # Auto-detect scenario
    result = pos_manager.process_auto_detect_scenario(
        seat_pack_data, performance_data, event_data, venue_data
    )

Features:
-  Clean separation from existing core scrapers
-  Complete transaction management and rollback support
-  Empty scrape protection (prevents mass delisting)
-  Comprehensive logging with emojis for easy identification
-  POS/StubHub API integration
-  Admin hold functionality
-  Bulk operations with batching
-  Advanced pack transformation detection (splits, merges)
-  Four-dimensional seat pack model compliance
-  Initial vs subsequent scrape scenario handling
"""

from .pos_workflow_manager import POSWorkflowManager, WorkflowResult
from .pos_diff_algorithm import (
    POSDiffAlgorithm,
    POSSyncPlan,
    POSCreationAction,
    POSUpdateAction,
    POSDelistAction,
    POSSyncAction
)
from .pos_comparator import POSComparator, ComparisonResult, PackTransformation
from .pos_sync_executor import POSSyncExecutor, ExecutionResult, SyncExecutionSummary
from .pos_inventory_pusher import POSInventoryPusher, InventoryCreationResult, BulkInventoryResult

__all__ = [
    # Main orchestrator
    'POSWorkflowManager',
    'WorkflowResult',
    
    # Diff algorithm
    'POSDiffAlgorithm',
    'POSSyncPlan',
    'POSCreationAction',
    'POSUpdateAction',
    'POSDelistAction',
    'POSSyncAction',
    
    # Comparison
    'POSComparator',
    'ComparisonResult',
    'PackTransformation',
    
    # Sync execution
    'POSSyncExecutor',
    'ExecutionResult',
    'SyncExecutionSummary',
    
    # Inventory management
    'POSInventoryPusher',
    'InventoryCreationResult',
    'BulkInventoryResult'
]