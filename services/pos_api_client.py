# consumer/services/pos_api_client.py
import json
import logging
import http.client
from django.conf import settings
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)




class POSAPIResponse:
    """Wrapper for POS API responses"""

    def __init__(self, success: bool, data: Optional[Dict] = None, error: Optional[str] = None):
        self.is_successful = success
        self.data = data or {}
        self.error = error


class POSAPIClient:
    """Client for interacting with StubHub POS API"""

    def __init__(self):
        # Use the exact working token that we know works
        self.auth_token = settings.STUBHUB_POS_AUTH_TOKEN

    def create_inventory_listing(self, payload: Dict[str, Any]) -> POSAPIResponse:
        """
        Create a new inventory listing in StubHub POS system
        
        Args:
            payload: Dictionary containing the inventory data in StubHub format
            
        Returns:
            POSAPIResponse with success status and response data or error
        """
        if not self.auth_token:
            error_msg = (
                "StubHub POS API auth token not configured. "
                "Please set the STUBHUB_API_TOKEN environment variable."
            )

            return POSAPIResponse(False, error="Authentication token not configured")

        try:
            # HARDCODE everything exactly like working Postman
            conn = http.client.HTTPSConnection("pointofsaleapi.stubhub.net")
            payload_json = json.dumps(payload)
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOjkwOCwiaWF0IjoxNzQ3Mjc0NjcyLCJzY29wZSI6IiIsImp0aSI6InpMbi9LWHFCSk5CZmdCT2c4L0ovdVBHbVRXSzl2RGlPRjltdW5nckMwNkNBZ3RlUnppeXJmdVlweGJxbGIrbzc1V2lZZ2cwc0doWXZtcDZnM1NKSUlpUHh5V2cydURUeDRpeGwwYWJTcGh3PSIsInZnZy1zdiI6IjEyMjZjY2FiYzE1MzQ3M2I5NjgxYWUzZThkYzk0ZDAwIiwiZXhwIjoyMDYzMTUzMDcyLCJwcm4iOiIzTTZoSlZObmhFcXpRaFpjeVBZanFDbnhhRlI1M2VyeUVaMUE5V0JteG9mbG8ybGpOa1hQV2U2dmZkczZ2RElnVzFNSHJJYXFwckMrejJlbFpMUVRmQUZPNUgwQnQ4WG1LUytIMVE1SERYYz0ifQ.Y9F28p8cDjHABKrXlugEUkDU9seWlqjxtJxDTOGNok8'
            }
            
            logger.info(f"🔗 Making POST request to: https://pointofsaleapi.stubhub.net/inventory/")
            logger.info(f"📋 Request headers: {headers}")
            logger.info(f"📦 Request payload: {json.dumps(payload, indent=2)}")
            
            # EXACT same as Postman
            conn.request("POST", "/inventory/", payload_json, headers)
            res = conn.getresponse()
            data = res.read()
            
            logger.info(f"📨 Response status: {res.status}")
            logger.info(f"📨 Response headers: {dict(res.getheaders())}")
            logger.info(f"📨 Response data: {data.decode('utf-8')}")
            
            # Check response status exactly like Postman
            if res.status in [200, 201]:
                response_json = json.loads(data.decode('utf-8'))
                
                # Check broadcast status in response
                is_broadcast = response_json.get('isBroadcast', False)
                listing_statuses = response_json.get('listingStatusByMarketplace', [])
                stubhub_status = next((status for status in listing_statuses if status.get('marketplaceName') == 'StubHub'), {})
                
                logger.info(f"✅ Successfully created inventory listing ID: {response_json.get('id')}")
                logger.info(f"📡 Broadcast status: {'✅ Broadcasting' if is_broadcast else '❌ Not broadcasting'}")
                logger.info(f"🏪 StubHub status: {stubhub_status.get('listingStatus', 'Unknown')}")
                
                if not is_broadcast:
                    logger.warning("⚠️ Inventory created but NOT broadcasting to marketplaces!")
                    logger.warning("This means the listing won't appear in StubHub dashboard")
                    
                    # Log potential reasons for non-broadcast
                    logger.info("Checking potential reasons for non-broadcast:")
                    logger.info(f"  - External ID: {payload.get('externalId')}")
                    logger.info(f"  - Ticket Count: {payload.get('ticketCount')}")
                    logger.info(f"  - Unit Cost: {payload.get('unitCost')}")
                    logger.info(f"  - Auto Broadcast requested: {payload.get('autoBroadcast')}")
                
                return POSAPIResponse(True, data=response_json)
            else:
                error_msg = f"HTTP {res.status}: {data.decode('utf-8')}"
                logger.error(f"Failed to create inventory listing: {error_msg}")
                return POSAPIResponse(False, error=error_msg)

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON response from POS API: {str(e)}"
            logger.error(error_msg)
            return POSAPIResponse(False, error=error_msg)

        except Exception as e:
            error_msg = f"Unexpected error while creating inventory listing: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return POSAPIResponse(False, error=error_msg)

    def split_inventory(self, inventory_id: str, payload: Dict[str, Any]) -> POSAPIResponse:
        """
        Split an existing inventory listing (used by reconciliation service)
        
        Args:
            inventory_id: The inventory ID to split
            payload: Split request payload
            
        Returns:
            POSAPIResponse with success status and response data or error
        """
        if not self.auth_token:
            error_msg = (
                "StubHub POS API auth token not configured. "
                "Please set the STUBHUB_API_TOKEN environment variable."
            )
            logger.error(error_msg)
            return POSAPIResponse(False, error="Authentication token not configured")

        try:
            logger.info(f"Splitting inventory {inventory_id}")
            logger.debug(f"Split payload: {json.dumps(payload, indent=2)}")

            # Hardcode like Postman
            conn = http.client.HTTPSConnection("pointofsaleapi.stubhub.net")
            json_data = json.dumps(payload)
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOjkwOCwiaWF0IjoxNzQ3Mjc0NjcyLCJzY29wZSI6IiIsImp0aSI6InpMbi9LWHFCSk5CZmdCT2c4L0ovdVBHbVRXSzl2RGlPRjltdW5nckMwNkNBZ3RlUnppeXJmdVlweGJxbGIrbzc1V2lZZ2cwc0doWXZtcDZnM1NKSUlpUHh5V2cydURUeDRpeGwwYWJTcGh3PSIsInZnZy1zdiI6IjEyMjZjY2FiYzE1MzQ3M2I5NjgxYWUzZThkYzk0ZDAwIiwiZXhwIjoyMDYzMTUzMDcyLCJwcm4iOiIzTTZoSlZObmhFcXpRaFpjeVBZanFDbnhhRlI1M2VyeUVaMUE5V0JteG9mbG8ybGpOa1hQV2U2dmZkczZ2RElnVzFNSHJJYXFwckMrejJlbFpMUVRmQUZPNUgwQnQ4WG1LUytIMVE1SERYYz0ifQ.Y9F28p8cDjHABKrXlugEUkDU9seWlqjxtJxDTOGNok8'
            }
            
            conn.request("POST", f"/inventory/{inventory_id}/split", json_data, headers)
            res = conn.getresponse()
            data = res.read()

            if res.status in [200, 201]:
                response_json = json.loads(data.decode('utf-8'))
                logger.info(f"Successfully split inventory {inventory_id}. Response: {response_json}")
                return POSAPIResponse(True, data=response_json)
            else:
                error_msg = f"HTTP {res.status}: {data.decode('utf-8')}"
                logger.error(f"Failed to split inventory {inventory_id}: {error_msg}")
                return POSAPIResponse(False, error=error_msg)

        except Exception as e:
            error_msg = f"Error splitting inventory {inventory_id}: {str(e)}"
            # logger.error(error_msg, exc_info=True)
            return POSAPIResponse(False, error=error_msg)

    def delete_inventory_listing(self, stubhub_inventory_id: str) -> POSAPIResponse:
        """
        Delete an inventory listing from StubHub POS system
        
        Args:
            stubhub_inventory_id: The StubHub inventory ID to delete
            
        Returns:
            POSAPIResponse with success status (204 = success)
        """
        if not self.auth_token:
            error_msg = (
                "StubHub POS API auth token not configured. "
                "Please set the STUBHUB_API_TOKEN environment variable."
            )
            logger.error(error_msg)
            return POSAPIResponse(False, error="Authentication token not configured")

        if not stubhub_inventory_id:
            logger.error("StubHub inventory ID is required for deletion")
            return POSAPIResponse(False, error="Inventory ID is required")

        try:
            logger.info(f"Deleting StubHub inventory {stubhub_inventory_id}")

            # Hardcode like Postman
            conn = http.client.HTTPSConnection("pointofsaleapi.stubhub.net")
            headers = {
                'Content-Type': 'application/json',
                'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOjkwOCwiaWF0IjoxNzQ3Mjc0NjcyLCJzY29wZSI6IiIsImp0aSI6InpMbi9LWHFCSk5CZmdCT2c4L0ovdVBHbVRXSzl2RGlPRjltdW5nckMwNkNBZ3RlUnppeXJmdVlweGJxbGIrbzc1V2lZZ2cwc0doWXZtcDZnM1NKSUlpUHh5V2cydURUeDRpeGwwYWJTcGh3PSIsInZnZy1zdiI6IjEyMjZjY2FiYzE1MzQ3M2I5NjgxYWUzZThkYzk0ZDAwIiwiZXhwIjoyMDYzMTUzMDcyLCJwcm4iOiIzTTZoSlZObmhFcXpRaFpjeVBZanFDbnhhRlI1M2VyeUVaMUE5V0JteG9mbG8ybGpOa1hQV2U2dmZkczZ2RElnVzFNSHJJYXFwckMrejJlbFpMUVRmQUZPNUgwQnQ4WG1LUytIMVE1SERYYz0ifQ.Y9F28p8cDjHABKrXlugEUkDU9seWlqjxtJxDTOGNok8'
            }
            
            conn.request("DELETE", f"/inventory/{stubhub_inventory_id}", "", headers)
            res = conn.getresponse()
            data = res.read()

            if res.status == 204:
                # 204 No Content = successful deletion
                logger.info(f"Successfully deleted inventory {stubhub_inventory_id}")
                return POSAPIResponse(True, data={"message": "Successfully deleted", "inventory_id": stubhub_inventory_id})
            elif res.status == 404:
                # 404 = inventory not found (may already be deleted)
                logger.warning(f"Inventory {stubhub_inventory_id} not found (may already be deleted)")
                return POSAPIResponse(True, data={"message": "Inventory not found (may already be deleted)", "inventory_id": stubhub_inventory_id})
            else:
                error_msg = f"HTTP {res.status}: {data.decode('utf-8')}"
                logger.error(f"Failed to delete inventory {stubhub_inventory_id}: {error_msg}")
                return POSAPIResponse(False, error=error_msg)

        except Exception as e:
            error_msg = f"Error deleting inventory {stubhub_inventory_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return POSAPIResponse(False, error=error_msg)
