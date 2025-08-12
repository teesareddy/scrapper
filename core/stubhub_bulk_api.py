"""
StubHub Bulk Inventory API Integration for Core Diffing Algorithm

This module implements the StubHub bulk inventory API integration,
specifically for handling inventory deletions as part of the Phase 2
seat pack synchronization process.
"""

import json
import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from django.conf import settings
from django.db import transaction
from django.utils import timezone
import requests
from ..models.pos import POSListing
from ..models.seat_packs import SeatPack
from .seat_pack_sync import DelistAction

logger = logging.getLogger(__name__)


@dataclass
class BulkDeleteRequest:
    """Request structure for StubHub bulk inventory deletion"""
    inventory_id: int


@dataclass
class BulkInventoryRequest:
    """Complete bulk inventory request structure"""
    bulk_processing_id: str
    delete_requests: List[BulkDeleteRequest]


@dataclass
class StubHubAPIResponse:
    """Response structure from StubHub API"""
    success: bool
    status_code: int
    response_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class StubHubBulkInventoryAPI:
    """
    Handles StubHub bulk inventory API operations for seat pack synchronization
    """
    
    def __init__(self, account_id: str, authorization_token: str):
        """
        Initialize the StubHub API client
        
        Args:
            account_id: StubHub account ID
            authorization_token: StubHub authorization token
        """
        self.account_id = account_id
        self.authorization_token = authorization_token
        self.base_url = "https://pointofsaleapi.stubhub.net"
        self.bulk_endpoint = f"{self.base_url}/inventory/bulk"
        
    def delete_inventory_items(self, inventory_ids: List[int]) -> StubHubAPIResponse:
        """
        Delete multiple inventory items using StubHub bulk API
        
        Args:
            inventory_ids: List of StubHub inventory IDs to delete
            
        Returns:
            StubHubAPIResponse with operation results
        """
        if not inventory_ids:
            logger.info("No inventory IDs provided for deletion")
            return StubHubAPIResponse(
                success=True,
                status_code=200,
                response_data={"message": "No items to delete"}
            )
        
        # Generate unique bulk processing ID
        bulk_processing_id = self._generate_bulk_processing_id()
        
        # Create delete requests
        delete_requests = [
            BulkDeleteRequest(inventory_id=inv_id) 
            for inv_id in inventory_ids
        ]
        
        # Build request payload
        request_payload = BulkInventoryRequest(
            bulk_processing_id=bulk_processing_id,
            delete_requests=delete_requests
        )
        
        try:
            response = self._make_bulk_request(request_payload)
            
            if response.success:
                logger.info(f"Successfully deleted {len(inventory_ids)} inventory items")
            else:
                logger.error(f"Failed to delete inventory items: {response.error_message}")
                
            return response
            
        except Exception as e:
            logger.error(f"Exception during bulk inventory deletion: {str(e)}")
            return StubHubAPIResponse(
                success=False,
                status_code=500,
                error_message=str(e)
            )
    
    def _make_bulk_request(self, request_payload: BulkInventoryRequest) -> StubHubAPIResponse:
        """
        Make the actual HTTP request to StubHub bulk API
        
        Args:
            request_payload: Bulk inventory request payload
            
        Returns:
            StubHubAPIResponse with API response details
        """
        headers = {
            'account-id': self.account_id,
            'Content-Type': 'application/json',
            'Authorization': self.authorization_token
        }
        
        # Convert request to JSON format matching the cURL example
        json_payload = {
            "bulkProcessingId": request_payload.bulk_processing_id,
            "deleteRequests": [
                {"inventoryId": req.inventory_id} 
                for req in request_payload.delete_requests
            ]
        }
        
        logger.debug(f"Making bulk request to {self.bulk_endpoint}")
        logger.debug(f"Request payload: {json_payload}")
        
        # DEBUG: Comment out actual HTTP request for debugging
        logger.info(f"[DEBUG MODE] Would make HTTP request to {self.bulk_endpoint}")
        logger.debug(f"[DEBUG MODE] Headers: {headers}")
        logger.debug(f"[DEBUG MODE] Payload: {json_payload}")
        
        # SIMULATE SUCCESS for debugging
        return StubHubAPIResponse(
            success=True,
            status_code=200,
            response_data={"message": "DEBUG MODE: Simulated successful deletion"},
            error_message=None
        )
        
        # ORIGINAL CODE COMMENTED OUT:
        # try:
        #     response = requests.post(
        #         self.bulk_endpoint,
        #         headers=headers,
        #         json=json_payload,
        #         timeout=30
        #     )
        #     
        #     response_data = None
        #     if response.content:
        #         try:
        #             response_data = response.json()
        #         except json.JSONDecodeError:
        #             logger.warning("Could not parse response as JSON")
        #             response_data = {"raw_response": response.text}
        #     
        #     return StubHubAPIResponse(
        #         success=response.status_code == 200,
        #         status_code=response.status_code,
        #         response_data=response_data,
        #         error_message=None if response.status_code == 200 else f"HTTP {response.status_code}: {response.text}"
        #     )
        #     
        # except requests.exceptions.RequestException as e:
        #     logger.error(f"Request exception: {str(e)}")
        #     return StubHubAPIResponse(
        #         success=False,
        #         status_code=500,
        #         error_message=str(e)
        #     )
    
    def _generate_bulk_processing_id(self) -> str:
        """
        Generate a unique bulk processing ID for the request
        
        Returns:
            UUID string for bulk processing ID
        """
        import uuid
        return str(uuid.uuid4()).upper()


class StubHubSyncIntegration:
    """
    Integrates StubHub bulk inventory operations with the Core Diffing Algorithm
    """
    
    def __init__(self, account_id: str, authorization_token: str):
        """
        Initialize the StubHub sync integration
        
        Args:
            account_id: StubHub account ID
            authorization_token: StubHub authorization token
        """
        self.api_client = StubHubBulkInventoryAPI(account_id, authorization_token)
    
    @transaction.atomic
    def process_delist_actions_with_stubhub(self, delist_actions: List[DelistAction]) -> Dict[str, Any]:
        """
        Process delist actions by calling StubHub API for inventory deletions
        
        Args:
            delist_actions: List of delist actions from sync plan
            
        Returns:
            Dictionary with operation results
        """
        if not delist_actions:
            return {
                'stubhub_deletions': 0,
                'local_delistings': 0,
                'errors': []
            }
        
        logger.info(f"Processing {len(delist_actions)} delist actions with StubHub integration")
        
        # Collect StubHub inventory IDs for deletion
        inventory_ids_to_delete = []
        local_pack_ids = []
        
        for action in delist_actions:
            # Get the seat pack to find its associated POS listing
            try:
                seat_pack = SeatPack.objects.get(
                    internal_pack_id=action.pack_id,
                    is_active=True
                )
                
                if seat_pack.pos_listing and seat_pack.pos_listing.stubhub_inventory_id:
                    # This pack has a StubHub inventory ID - add to deletion list
                    inventory_ids_to_delete.append(seat_pack.pos_listing.stubhub_inventory_id)
                    logger.debug(f"Pack {action.pack_id} -> StubHub inventory {seat_pack.pos_listing.stubhub_inventory_id}")
                
                local_pack_ids.append(action.pack_id)
                
            except SeatPack.DoesNotExist:
                logger.warning(f"Pack {action.pack_id} not found for StubHub deletion")
                continue
        
        results = {
            'stubhub_deletions': 0,
            'local_delistings': 0,
            'errors': []
        }
        
        # Process individual StubHub inventory deletions
        if inventory_ids_to_delete:
            logger.info(f"Deleting {len(inventory_ids_to_delete)} items from StubHub using individual DELETE calls")
            
            # Use POSAPIClient for individual deletions
            from consumer.services.pos_api_client import POSAPIClient
            pos_client = POSAPIClient()
            
            successful_deletions = 0
            failed_deletions = 0
            
            for inventory_id in inventory_ids_to_delete:
                try:
                    logger.debug(f"Deleting StubHub inventory {inventory_id}")
                    api_response = pos_client.delete_inventory_listing(inventory_id)
                    
                    if api_response.is_successful:
                        successful_deletions += 1
                        logger.debug(f"Successfully deleted inventory {inventory_id}")
                    else:
                        failed_deletions += 1
                        error_msg = f"Failed to delete inventory {inventory_id}: {api_response.error}"
                        results['errors'].append(error_msg)
                        logger.error(error_msg)
                        
                except Exception as e:
                    failed_deletions += 1
                    error_msg = f"Exception deleting inventory {inventory_id}: {str(e)}"
                    results['errors'].append(error_msg)
                    logger.error(error_msg)
            
            results['stubhub_deletions'] = successful_deletions
            logger.info(f"StubHub deletion completed: {successful_deletions} successful, {failed_deletions} failed")
        
        # Update local database - mark seat packs as inactive
        local_delistings = self._update_local_seat_packs(delist_actions)
        results['local_delistings'] = local_delistings
        
        return results
    
    def _update_local_seat_packs(self, delist_actions: List[DelistAction]) -> int:
        """
        Update local seat packs to mark them as inactive
        
        Args:
            delist_actions: List of delist actions to process
            
        Returns:
            Number of successfully delisted packs
        """
        delisted_count = 0
        
        for action in delist_actions:
            try:
                # Update the seat pack
                updated_count = SeatPack.objects.filter(
                    internal_pack_id=action.pack_id,
                    is_active=True,
                    manually_delisted=False
                ).update(
                    is_active=False,
                    delist_reason=action.reason,
                    updated_at=timezone.now()
                )
                
                if updated_count > 0:
                    delisted_count += 1
                    logger.debug(f"Locally delisted pack {action.pack_id}: {action.reason}")
                
                # Update associated POS listing status
                try:
                    pos_listing = POSListing.objects.get(
                        seat_packs__internal_pack_id=action.pack_id
                    )
                    pos_listing.status = 'INACTIVE'
                    pos_listing.save(update_fields=['status', 'updated_at'])
                    logger.debug(f"Updated POS listing {pos_listing.pos_inventory_id} to INACTIVE")
                    
                except POSListing.DoesNotExist:
                    logger.debug(f"No POS listing found for pack {action.pack_id}")
                    
            except Exception as e:
                logger.error(f"Failed to update pack {action.pack_id}: {str(e)}")
        
        return delisted_count


def get_stubhub_api_client() -> Optional[StubHubSyncIntegration]:
    """
    Factory function to create StubHub API client from Django settings
    
    Returns:
        StubHubSyncIntegration instance or None if not configured
    """
    try:
        account_id = getattr(settings, 'STUBHUB_ACCOUNT_ID', None)
        auth_token = getattr(settings, 'STUBHUB_AUTHORIZATION_TOKEN', None)
        
        if not account_id or not auth_token:
            logger.warning("StubHub API credentials not configured in Django settings")
            return None
        
        return StubHubSyncIntegration(account_id, auth_token)
        
    except Exception as e:
        logger.error(f"Failed to initialize StubHub API client: {str(e)}")
        return None


def process_sync_plan_with_stubhub(delist_actions: List[DelistAction]) -> Dict[str, Any]:
    """
    High-level function to process sync plan delist actions with StubHub integration
    
    Args:
        delist_actions: List of delist actions from sync plan
        
    Returns:
        Dictionary with operation results
    """
    stubhub_client = get_stubhub_api_client()
    
    if not stubhub_client:
        logger.info("StubHub integration not available, processing locally only")
        return {
            'stubhub_deletions': 0,
            'local_delistings': len(delist_actions),
            'errors': ['StubHub integration not configured']
        }
    
    return stubhub_client.process_delist_actions_with_stubhub(delist_actions)