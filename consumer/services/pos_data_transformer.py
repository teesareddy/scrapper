# consumer/services/pos_data_transformer.py
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from decimal import Decimal
from django.utils import timezone
from django.db import models
import pytz

logger = logging.getLogger(__name__)


class POSDataTransformer:
    """Transform scraped seat pack data to StubHub POS API format"""
    
    @staticmethod
    def transform_seat_pack_to_pos_payload(seat_pack, performance_data=None) -> Dict[str, Any]:
        """
        Transform a SeatPack object to StubHub POS API format
        
        Args:
            seat_pack: SeatPack model instance
            performance_data: Optional dict with additional performance info
            
        Returns:
            Dictionary in StubHub POS API format
        """
        try:
            # Get related data
            zone = seat_pack.zone_id
            performance = zone.performance_id
            event = performance.event_id
            venue = performance.venue_id
            
            # Calculate pricing from zone or seat pack
            unit_cost = POSDataTransformer._calculate_unit_cost(seat_pack, zone)
            expected_value = POSDataTransformer._calculate_expected_value(seat_pack, zone)
            face_value_cost = POSDataTransformer._calculate_face_value(seat_pack, zone)
            
            # Get section name (prefer level name, fallback to zone name)
            section_name = POSDataTransformer._get_section_name(seat_pack, zone)
            
            # Convert UTC performance time to venue local timezone
            if venue.venue_timezone:
                try:
                    venue_tz = pytz.timezone(venue.venue_timezone)
                    local_performance_time = performance.performance_datetime_utc.astimezone(venue_tz)
                    formatted_time = local_performance_time.strftime('%Y-%m-%dT%H:%M:%S')
                except pytz.UnknownTimeZoneError:
                    logger.warning(f"Unknown timezone {venue.venue_timezone} for venue {venue.name}, using UTC")
                    formatted_time = performance.performance_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S')
            else:
                # Fallback to UTC if venue timezone not available
                formatted_time = performance.performance_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S')
            
            # Create dynamic payload using Postman-compatible format
            payload = {
                "currencyCode": "USD",  # Force USD to match Postman exactly
                "unitCost": float(unit_cost) if unit_cost else 0.0,
                # "expectedValue": float(expected_value) if expected_value else float(unit_cost) if unit_cost else 0.0,
                # "taxPaid": 0,
                # "faceValueCost": float(face_value_cost) if face_value_cost else float(unit_cost) if unit_cost else 0.0,
                "deliveryType": "InApp",
                # "deliveryCost": 0,
                "inHandAt": formatted_time,
                "seating": {
                    "section": section_name,
                    "row": seat_pack.row_label
                },
                "eventMapping": {
                    "eventName": event.name,
                    "eventDate": formatted_time,
                    "venueName": venue.name,
                    "isEventDateConfirmed": True,
                    "city": venue.city,
                    "stateProvince": venue.state,
                    "countryCode": venue.country if venue.country else "US"  # Use "US" to match Postman exactly
                },
                "externalId": seat_pack.internal_pack_id,
                "ticketCount": seat_pack.pack_size,
                "autoBroadcast": True,
                "internalNotes": f"Auto-created via database sync. Generated on {timezone.now().strftime('%Y-%m-%d %H:%M')} (Validation passed)",
                "zoneFill": True,
            }
            
            # Only add listingNotes if zone is wheelchair accessible
            if seat_pack.zone_id.wheelchair_accessible:
                payload["listingNotes"] = [{
                    "note": "Wheelchair accessible seating"
                }]
            
            logger.info("🔧 Using dynamic payload with Postman-compatible connection method")
            
            # Validate business rules that might affect broadcasting
            validation_warnings = []
            
            if payload['unitCost'] <= 0:
                validation_warnings.append("Zero or negative unit cost might prevent broadcasting")
                
            if payload['ticketCount'] <= 0:
                validation_warnings.append("Zero ticket count might prevent broadcasting")
                
            if not payload['seating']['section'] or not payload['seating']['row']:
                validation_warnings.append("Missing seating information might prevent broadcasting")
                
            if validation_warnings:
                logger.warning("⚠️ Potential broadcast validation issues:")
                for warning in validation_warnings:
                    logger.warning(f"  - {warning}")
            
            logger.debug(f"Transformed seat pack {seat_pack.internal_pack_id} to POS payload: {payload}")
            return payload
            
        except Exception as e:
            logger.error(f"Error transforming seat pack {seat_pack.internal_pack_id} to POS payload: {e}", exc_info=True)
            raise
    
    @staticmethod
    def _calculate_unit_cost(seat_pack, zone) -> Optional[Decimal]:
        """Calculate unit cost per ticket"""
        try:
            # Try to get from seat pack price first
            if seat_pack.pack_price and seat_pack.pack_size > 0:
                return seat_pack.pack_price / seat_pack.pack_size
            
            # Fallback to zone pricing
            if hasattr(zone, 'min_price') and zone.min_price:
                return zone.min_price
                
            # Get pricing from related seats if available
            seats = seat_pack.zone_id.seats.filter(
                row_label=seat_pack.row_label,
                seat_number__in=seat_pack.seat_keys
            ).exclude(current_price__isnull=True)
            
            if seats.exists():
                avg_price = seats.aggregate(avg_price=models.Avg('current_price'))['avg_price']
                return avg_price
                
            return None
            
        except Exception as e:
            logger.warning(f"Error calculating unit cost for seat pack {seat_pack.internal_pack_id}: {e}")
            return None
    
    @staticmethod
    def _calculate_expected_value(seat_pack, zone) -> Optional[Decimal]:
        """Calculate expected value (same as unit cost for now)"""
        return POSDataTransformer._calculate_unit_cost(seat_pack, zone)
    
    @staticmethod
    def _calculate_face_value(seat_pack, zone) -> Optional[Decimal]:
        """Calculate face value (use unit cost as fallback)"""
        # For now, use the same calculation as unit cost
        # In the future, this could be enhanced to use actual face value data
        return POSDataTransformer._calculate_unit_cost(seat_pack, zone)
    
    @staticmethod
    def _get_section_name(seat_pack, zone) -> str:
        """Get the best section name for the seat pack"""
        try:
            # First, try to get level name directly from seat pack's level relationship
            if hasattr(seat_pack, 'level') and seat_pack.level:
                return seat_pack.level.alias
            
            # Fallback: Try to get section name from seat pack's zone relationships
            # Get level name through related seats if direct relationship doesn't exist
            seats = seat_pack.zone_id.seats.filter(
                row_label=seat_pack.row_label
            ).select_related('section_id__level_id')
            
            if seats.exists():
                seat = seats.first()
                if hasattr(seat, 'section_id') and seat.section_id:
                    if hasattr(seat.section_id, 'level_id') and seat.section_id.level_id:
                        return seat.section_id.level_id.alias
                    return seat.section_id.alias
            
            # Final fallback to zone name
            return zone.name
            
        except Exception as e:
            logger.warning(f"Error getting section name for seat pack {seat_pack.internal_pack_id}: {e}")
            return zone.name or "General"
    
    @staticmethod
    def validate_pos_payload(payload: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate that the POS payload has all required fields
        
        Args:
            payload: The POS payload to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        required_fields = [
            'currencyCode', 'externalId',  'ticketCount',
            'seating', 'eventMapping'
        ]
        
        for field in required_fields:
            if field not in payload:
                return False, f"Missing required field: {field}"
        
        # Validate nested required fields
        if 'section' not in payload['seating'] or 'row' not in payload['seating']:
            return False, "Missing required seating information (section or row)"
        
        required_event_fields = ['eventName', 'eventDate', 'venueName', 'city']
        for field in required_event_fields:
            if field not in payload['eventMapping']:
                return False, f"Missing required eventMapping field: {field}"
        
        # Validate data types
        if not isinstance(payload['ticketCount'], int) or payload['ticketCount'] <= 0:
            return False, "ticketCount must be a positive integer"
        
        return True, None