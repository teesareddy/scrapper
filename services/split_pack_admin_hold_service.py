"""
StubHub Admin Hold Service for Split Packs

This service automatically applies admin holds to StubHub inventory
when seat packs get split, preventing double-selling of original inventory.
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

logger = logging.getLogger(__name__)


@dataclass
class AdminHoldResult:
    """Result of applying admin hold to a StubHub inventory"""
    success: bool
    stubhub_inventory_id: str
    pos_listing_id: int
    error_message: Optional[str] = None
    status_code: Optional[int] = None


class SplitPackAdminHoldService:
    """
    Service to handle admin holds for split seat packs
    """
    
    def __init__(self, stubhub_api_base_url: str = None):
        """
        Initialize the admin hold service
        
        Args:
            stubhub_api_base_url: Base URL for StubHub API (defaults to production)
        """
        self.stubhub_api_base_url = stubhub_api_base_url or "https://pointofsaleapi.stubhub.net"
        self.timeout = 30  # seconds
        
    def find_split_packs(self, source_website: str = None) -> List[SeatPack]:
        """
        Find seat packs that have been split (transformed)
        
        Args:
            source_website: Optional filter by source website
            
        Returns:
            List of SeatPack objects that represent split packs
        """
        query = SeatPack.objects.filter(
            delist_reason='transformed',
            is_active=False,
            pos_listing__isnull=False,  # Must have POSListing
            pos_listing__stubhub_inventory_id__isnull=False,  # Must have StubHub ID
            pos_listing__status='ACTIVE'  # Only process active POS listings
        ).select_related('pos_listing')
        
        if source_website:
            query = query.filter(source_website=source_website)
            
        split_packs = list(query)
        logger.info(f"Found {len(split_packs)} split packs for admin hold processing")
        return split_packs
        
    def apply_admin_hold(self, stubhub_inventory_id: str, notes: str = None) -> AdminHoldResult:
        """
        Apply admin hold to a StubHub inventory item
        
        Args:
            stubhub_inventory_id: StubHub inventory ID
            notes: Optional notes for the admin hold
            
        Returns:
            AdminHoldResult with success status and details
        """
        url = f"{self.stubhub_api_base_url}/inventory/{stubhub_inventory_id}"
        
        # Default admin hold payload
        payload = {
            "adminHold": {
                "expirationDate": "2026-01-01T00:00:00",
                "notes": notes or "Auto-hold: Pack split detected - Original inventory superseded"
            }
        }
        
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        try:
            logger.debug(f"Applying admin hold to StubHub inventory {stubhub_inventory_id}")
            
            response = requests.put(
                url,
                json=payload,
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code in [200, 201, 204]:
                logger.info(f"Successfully applied admin hold to inventory {stubhub_inventory_id}")
                return AdminHoldResult(
                    success=True,
                    stubhub_inventory_id=stubhub_inventory_id,
                    pos_listing_id=0,  # Will be set by caller
                    status_code=response.status_code
                )
            else:
                error_msg = f"StubHub API returned status {response.status_code}: {response.text}"
                logger.error(error_msg)
                return AdminHoldResult(
                    success=False,
                    stubhub_inventory_id=stubhub_inventory_id,
                    pos_listing_id=0,
                    error_message=error_msg,
                    status_code=response.status_code
                )
                
        except requests.exceptions.Timeout:
            error_msg = f"Timeout applying admin hold to inventory {stubhub_inventory_id}"
            logger.error(error_msg)
            return AdminHoldResult(
                success=False,
                stubhub_inventory_id=stubhub_inventory_id,
                pos_listing_id=0,
                error_message=error_msg
            )
        except requests.exceptions.RequestException as e:
            error_msg = f"Request error applying admin hold to inventory {stubhub_inventory_id}: {str(e)}"
            logger.error(error_msg)
            return AdminHoldResult(
                success=False,
                stubhub_inventory_id=stubhub_inventory_id,
                pos_listing_id=0,
                error_message=error_msg
            )
        except Exception as e:
            error_msg = f"Unexpected error applying admin hold to inventory {stubhub_inventory_id}: {str(e)}"
            logger.error(error_msg)
            return AdminHoldResult(
                success=False,
                stubhub_inventory_id=stubhub_inventory_id,
                pos_listing_id=0,
                error_message=error_msg
            )
            
    def mark_pos_inactive(self, pos_listing: POSListing, admin_hold_reason: str = None) -> bool:
        """
        Mark a POSListing as inactive and record admin hold details
        
        Args:
            pos_listing: POSListing object to mark as inactive
            admin_hold_reason: Reason for applying admin hold
            
        Returns:
            True if successful, False otherwise
        """
        try:
            pos_listing.status = 'INACTIVE'
            pos_listing.admin_hold_applied = True
            pos_listing.admin_hold_date = timezone.now()
            pos_listing.admin_hold_reason = admin_hold_reason or "Admin hold applied due to pack split"
            pos_listing.updated_at = timezone.now()
            pos_listing.save(update_fields=[
                'status', 
                'admin_hold_applied', 
                'admin_hold_date', 
                'admin_hold_reason', 
                'updated_at'
            ])
            
            logger.info(f"Marked POSListing {pos_listing.pos_listing_id} as inactive with admin hold")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark POSListing {pos_listing.pos_listing_id} as inactive: {str(e)}")
            return False
            
    @transaction.atomic
    def process_split_packs(self, source_website: str = None) -> Dict[str, Any]:
        """
        Main method to process all split packs and apply admin holds
        
        Args:
            source_website: Optional filter by source website
            
        Returns:
            Dictionary with processing results and statistics
        """
        results = {
            'total_split_packs': 0,
            'admin_holds_applied': 0,
            'pos_listings_deactivated': 0,
            'errors': 0,
            'error_details': [],
            'processed_inventory_ids': []
        }
        
        try:
            # Find all split packs
            split_packs = self.find_split_packs(source_website)
            results['total_split_packs'] = len(split_packs)
            
            if not split_packs:
                logger.info("No split packs found for admin hold processing")
                return results
            
            # Process each split pack
            for split_pack in split_packs:
                pos_listing = split_pack.pos_listing
                stubhub_inventory_id = pos_listing.stubhub_inventory_id
                
                logger.debug(f"Processing split pack {split_pack.internal_pack_id} with StubHub ID {stubhub_inventory_id}")
                
                # Apply admin hold via StubHub API
                hold_result = self.apply_admin_hold(
                    stubhub_inventory_id,
                    f"Auto-hold: Pack {split_pack.internal_pack_id} split detected"
                )
                
                hold_result.pos_listing_id = pos_listing.pos_listing_id
                
                if hold_result.success:
                    results['admin_holds_applied'] += 1
                    results['processed_inventory_ids'].append(stubhub_inventory_id)
                    
                    # Mark POSListing as inactive with admin hold reason
                    admin_hold_reason = f"Auto-hold: Pack {split_pack.internal_pack_id} split detected"
                    if self.mark_pos_inactive(pos_listing, admin_hold_reason):
                        results['pos_listings_deactivated'] += 1
                    else:
                        results['errors'] += 1
                        results['error_details'].append({
                            'pack_id': split_pack.internal_pack_id,
                            'inventory_id': stubhub_inventory_id,
                            'error': 'Failed to mark POSListing as inactive'
                        })
                else:
                    results['errors'] += 1
                    results['error_details'].append({
                        'pack_id': split_pack.internal_pack_id,
                        'inventory_id': stubhub_inventory_id,
                        'error': hold_result.error_message
                    })
                    
            logger.info(f"Split pack processing completed: {results}")
            return results
            
        except Exception as e:
            logger.error(f"Error processing split packs: {str(e)}")
            results['errors'] += 1
            results['error_details'].append({
                'pack_id': 'unknown',
                'inventory_id': 'unknown',
                'error': str(e)
            })
            return results
            
    def get_split_pack_statistics(self, source_website: str = None) -> Dict[str, Any]:
        """
        Get statistics about split packs and their processing status
        
        Args:
            source_website: Optional filter by source website
            
        Returns:
            Dictionary with split pack statistics
        """
        # Base query for split packs
        split_packs_query = SeatPack.objects.filter(
            delist_reason='transformed',
            is_active=False
        )
        
        if source_website:
            split_packs_query = split_packs_query.filter(source_website=source_website)
            
        # Count various categories
        total_split_packs = split_packs_query.count()
        
        split_packs_with_pos = split_packs_query.filter(
            pos_listing__isnull=False
        ).count()
        
        split_packs_with_stubhub_id = split_packs_query.filter(
            pos_listing__isnull=False,
            pos_listing__stubhub_inventory_id__isnull=False
        ).count()
        
        processed_split_packs = split_packs_query.filter(
            pos_listing__isnull=False,
            pos_listing__status='INACTIVE'
        ).count()
        
        admin_holds_applied = split_packs_query.filter(
            pos_listing__isnull=False,
            pos_listing__admin_hold_applied=True
        ).count()
        
        pending_split_packs = split_packs_query.filter(
            pos_listing__isnull=False,
            pos_listing__stubhub_inventory_id__isnull=False,
            pos_listing__status='ACTIVE'
        ).count()
        
        return {
            'total_split_packs': total_split_packs,
            'split_packs_with_pos_listing': split_packs_with_pos,
            'split_packs_with_stubhub_id': split_packs_with_stubhub_id,
            'processed_split_packs': processed_split_packs,
            'admin_holds_applied': admin_holds_applied,
            'pending_split_packs': pending_split_packs,
            'source_website': source_website
        }