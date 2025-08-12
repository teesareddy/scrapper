"""
POS Configuration Handler for POS Sync Workflow

This module handles extracting and validating POS configuration from enriched data
provided by NestJS, decoupling POS configuration from venue dependencies.
"""

import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from django.core.exceptions import ValidationError

logger = logging.getLogger(__name__)


@dataclass
class POSConfiguration:
    """Validated POS configuration extracted from enriched data"""
    
    # Core POS settings
    pos_enabled: bool
    sync_mode: str  # 'immediate', 'on_demand'
    
    # StubHub API configuration
    stubhub_api_key: str
    stubhub_secret: str
    stubhub_base_url: str
    
    # POS operation settings
    create_enabled: bool
    delete_enabled: bool
    max_retry_attempts: int
    retry_delay_seconds: int
    
    # Performance-specific settings
    performance_id: str
    source_website: str
    venue_name: str
    event_name: str
    
    # Optional admin hold settings
    admin_hold_enabled: bool = False
    admin_hold_reason: Optional[str] = None
    
    # Validation settings
    min_pack_size: int = 2
    max_pack_size: int = 8
    price_validation_enabled: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary for easy serialization"""
        return {
            'pos_enabled': self.pos_enabled,
            'sync_mode': self.sync_mode,
            'stubhub_api_key': self.stubhub_api_key,
            'stubhub_secret': self.stubhub_secret,
            'stubhub_base_url': self.stubhub_base_url,
            'create_enabled': self.create_enabled,
            'delete_enabled': self.delete_enabled,
            'max_retry_attempts': self.max_retry_attempts,
            'retry_delay_seconds': self.retry_delay_seconds,
            'performance_id': self.performance_id,
            'source_website': self.source_website,
            'venue_name': self.venue_name,
            'event_name': self.event_name,
            'admin_hold_enabled': self.admin_hold_enabled,
            'admin_hold_reason': self.admin_hold_reason,
            'min_pack_size': self.min_pack_size,
            'max_pack_size': self.max_pack_size,
            'price_validation_enabled': self.price_validation_enabled
        }


class POSConfigurationHandler:
    """
    Extracts and validates POS configuration from enriched data
    """
    
    def __init__(self, performance_id: str, source_website: str):
        """
        Initialize the POS configuration handler
        
        Args:
            performance_id: Internal performance ID
            source_website: Source website identifier
        """
        self.performance_id = performance_id
        self.source_website = source_website
        
    def extract_pos_configuration(self, enriched_data: Dict[str, Any]) -> POSConfiguration:
        """
        Extract and validate POS configuration from enriched data
        
        Args:
            enriched_data: Enriched data from NestJS containing POS configuration
            
        Returns:
            POSConfiguration object with validated settings
            
        Raises:
            ValidationError: If configuration is invalid or missing required fields
        """
        logger.info(f"Extracting POS configuration for performance {self.performance_id}")
        
        try:
            # Extract POS configuration section
            pos_config = enriched_data.get('pos_config', {})
            if not pos_config:
                logger.warning("No pos_config found in enriched data, using defaults")
                pos_config = {}
            
            # Extract event and venue information
            event_info = enriched_data.get('event', {})
            venue_info = enriched_data.get('venue', {})
            performance_info = enriched_data.get('performance', {})
            
            # If enriched_data is empty or missing required fields, fetch from database
            if not event_info.get('name') or not venue_info.get('name'):
                logger.info("Missing event/venue information in enriched_data, fetching from database")
                db_info = self._fetch_performance_info_from_db()
                
                # Update enriched_data with database information
                if not event_info.get('name') and db_info.get('event_name'):
                    event_info['name'] = db_info['event_name']
                if not venue_info.get('name') and db_info.get('venue_name'):
                    venue_info['name'] = db_info['venue_name']
                if not performance_info.get('event_date') and db_info.get('performance_datetime'):
                    performance_info['event_date'] = db_info['performance_datetime']
            
            # Validate required fields
            self._validate_required_fields(pos_config, event_info, venue_info)
            
            # Build configuration object
            config = POSConfiguration(
                # Core POS settings
                pos_enabled=pos_config.get('enabled', False),
                sync_mode=pos_config.get('sync_mode', 'on_demand'),
                
                # StubHub API configuration
                stubhub_api_key=pos_config.get('stubhub_api_key', ''),
                stubhub_secret=pos_config.get('stubhub_secret', ''),
                stubhub_base_url=pos_config.get('stubhub_base_url', 'https://api.stubhub.com'),
                
                # POS operation settings
                create_enabled=pos_config.get('create_enabled', True),
                delete_enabled=pos_config.get('delete_enabled', True),
                max_retry_attempts=pos_config.get('max_retry_attempts', 3),
                retry_delay_seconds=pos_config.get('retry_delay_seconds', 30),
                
                # Performance-specific settings
                performance_id=self.performance_id,
                source_website=self.source_website,
                venue_name=venue_info.get('name', 'Unknown Venue'),
                event_name=event_info.get('name', 'Unknown Event'),
                
                # Optional admin hold settings
                admin_hold_enabled=pos_config.get('admin_hold_enabled', False),
                admin_hold_reason=pos_config.get('admin_hold_reason'),
                
                # Validation settings
                min_pack_size=pos_config.get('min_pack_size', 2),
                max_pack_size=pos_config.get('max_pack_size', 8),
                price_validation_enabled=pos_config.get('price_validation_enabled', True)
            )
            
            # Validate configuration values
            self._validate_configuration(config)
            
            logger.info(f"Successfully extracted POS configuration: enabled={config.pos_enabled}, mode={config.sync_mode}")
            return config
            
        except Exception as e:
            logger.error(f"Error extracting POS configuration: {e}", exc_info=True)
            raise ValidationError(f"Invalid POS configuration: {e}")
    
    def _validate_required_fields(
        self, 
        pos_config: Dict[str, Any], 
        event_info: Dict[str, Any], 
        venue_info: Dict[str, Any]
    ):
        """
        Validate that required fields are present in enriched data
        
        Args:
            pos_config: POS configuration section
            event_info: Event information section
            venue_info: Venue information section
            
        Raises:
            ValidationError: If required fields are missing
        """
        missing_fields = []
        
        # Check POS configuration requirements
        if pos_config.get('enabled', False):
            required_pos_fields = ['stubhub_api_key', 'stubhub_secret']
            for field in required_pos_fields:
                if not pos_config.get(field):
                    missing_fields.append(f"pos_config.{field}")
        
        # Check event information
        if not event_info.get('name'):
            missing_fields.append('event.name')
        
        # Check venue information
        if not venue_info.get('name'):
            missing_fields.append('venue.name')
        
        if missing_fields:
            raise ValidationError(f"Missing required fields: {', '.join(missing_fields)}")
    
    def _validate_configuration(self, config: POSConfiguration):
        """
        Validate configuration values for consistency and correctness
        
        Args:
            config: POSConfiguration object to validate
            
        Raises:
            ValidationError: If configuration values are invalid
        """
        validation_errors = []
        
        # Validate sync mode
        valid_sync_modes = ['immediate', 'on_demand']
        if config.sync_mode not in valid_sync_modes:
            validation_errors.append(f"Invalid sync_mode '{config.sync_mode}'. Must be one of: {valid_sync_modes}")
        
        # Validate retry settings
        if config.max_retry_attempts < 0 or config.max_retry_attempts > 10:
            validation_errors.append("max_retry_attempts must be between 0 and 10")
        
        if config.retry_delay_seconds < 1 or config.retry_delay_seconds > 300:
            validation_errors.append("retry_delay_seconds must be between 1 and 300")
        
        # Validate pack size settings
        if config.min_pack_size < 1 or config.min_pack_size > config.max_pack_size:
            validation_errors.append("min_pack_size must be >= 1 and <= max_pack_size")
        
        if config.max_pack_size < config.min_pack_size or config.max_pack_size > 20:
            validation_errors.append("max_pack_size must be >= min_pack_size and <= 20")
        
        # Validate StubHub API settings if POS is enabled
        if config.pos_enabled:
            if not config.stubhub_api_key or len(config.stubhub_api_key) < 10:
                validation_errors.append("stubhub_api_key must be at least 10 characters when POS is enabled")
            
            if not config.stubhub_secret or len(config.stubhub_secret) < 10:
                validation_errors.append("stubhub_secret must be at least 10 characters when POS is enabled")
            
            if not config.stubhub_base_url or not config.stubhub_base_url.startswith('https://'):
                validation_errors.append("stubhub_base_url must be a valid HTTPS URL when POS is enabled")
        
        # Validate admin hold settings
        if config.admin_hold_enabled and not config.admin_hold_reason:
            validation_errors.append("admin_hold_reason is required when admin_hold_enabled is True")
        
        if validation_errors:
            raise ValidationError(f"Configuration validation failed: {'; '.join(validation_errors)}")
    
    def get_default_configuration(self) -> POSConfiguration:
        """
        Get default POS configuration when enriched data is not available
        
        Returns:
            POSConfiguration with safe default values
        """
        logger.info("Using default POS configuration")
        
        return POSConfiguration(
            # Core POS settings (disabled by default)
            pos_enabled=False,
            sync_mode='on_demand',
            
            # Empty StubHub API configuration
            stubhub_api_key='',
            stubhub_secret='',
            stubhub_base_url='https://api.stubhub.com',
            
            # Conservative operation settings
            create_enabled=False,
            delete_enabled=False,
            max_retry_attempts=3,
            retry_delay_seconds=30,
            
            # Performance-specific settings
            performance_id=self.performance_id,
            source_website=self.source_website,
            venue_name='Unknown Venue',
            event_name='Unknown Event',
            
            # Safe defaults
            admin_hold_enabled=False,
            min_pack_size=2,
            max_pack_size=8,
            price_validation_enabled=True
        )
    
    def is_pos_enabled(self, enriched_data: Dict[str, Any]) -> bool:
        """
        Quick check if POS is enabled without full configuration extraction
        
        Args:
            enriched_data: Enriched data from NestJS
            
        Returns:
            True if POS is enabled, False otherwise
        """
        pos_config = enriched_data.get('pos_config', {})
        return pos_config.get('enabled', False)
    
    def get_sync_mode(self, enriched_data: Dict[str, Any]) -> str:
        """
        Get the sync mode from enriched data
        
        Args:
            enriched_data: Enriched data from NestJS
            
        Returns:
            Sync mode ('immediate' or 'on_demand')
        """
        pos_config = enriched_data.get('pos_config', {})
        return pos_config.get('sync_mode', 'on_demand')
    
    def _fetch_performance_info_from_db(self) -> Dict[str, Any]:
        """
        Fetch performance, event, and venue information from database
        
        Returns:
            Dictionary with event_name, venue_name, and performance_datetime
        """
        try:
            from ..models.base import Performance
            
            performance = Performance.objects.select_related(
                'event_id', 'venue_id'
            ).get(internal_performance_id=self.performance_id)
            
            return {
                'event_name': performance.event_id.name if performance.event_id else None,
                'venue_name': performance.venue_id.name if performance.venue_id else None,
                'performance_datetime': performance.performance_datetime_utc.isoformat() if performance.performance_datetime_utc else None
            }
            
        except Exception as e:
            logger.error(f"Error fetching performance info from database: {e}")
            return {}


def extract_pos_configuration(
    enriched_data: Dict[str, Any],
    performance_id: str,
    source_website: str
) -> POSConfiguration:
    """
    Convenience function for extracting POS configuration
    
    Args:
        enriched_data: Enriched data from NestJS
        performance_id: Internal performance ID
        source_website: Source website identifier
        
    Returns:
        POSConfiguration object with validated settings
    """
    handler = POSConfigurationHandler(performance_id, source_website)
    return handler.extract_pos_configuration(enriched_data)


def is_pos_enabled_for_performance(
    enriched_data: Dict[str, Any],
    performance_id: str,
    source_website: str
) -> bool:
    """
    Convenience function to check if POS is enabled for a performance
    
    Args:
        enriched_data: Enriched data from NestJS
        performance_id: Internal performance ID
        source_website: Source website identifier
        
    Returns:
        True if POS is enabled, False otherwise
    """
    handler = POSConfigurationHandler(performance_id, source_website)
    return handler.is_pos_enabled(enriched_data)