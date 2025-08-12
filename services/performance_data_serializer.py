"""
Performance Data Serializer Service

Provides clean, type-safe serialization of performance data for NestJS integration.
Eliminates fallbacks and ensures consistent data structure.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from django.utils import timezone
from django.db import transaction

logger = logging.getLogger(__name__)


class PerformanceDataSerializationError(Exception):
    """Raised when performance data cannot be properly serialized"""
    pass


class PerformanceDataSerializer:
    """
    Service for serializing performance data into a clean, consistent format for NestJS.
    
    This eliminates the need for fallbacks and optional field handling on the NestJS side,
    ensuring type safety and data consistency.
    """
    
    @staticmethod
    def serialize_for_nestjs(performance_id: str, scrape_job_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Serialize performance data for NestJS consumption.
        
        Args:
            performance_id: Django internal performance ID
            scrape_job_id: Optional scrape job ID for tracking
            
        Returns:
            Dict with guaranteed structure and required fields
            
        Raises:
            PerformanceDataSerializationError: If data cannot be serialized
        """
        try:
            logger.info(f"Serializing performance data for NestJS: {performance_id}")
            
            # Get performance with all related data in single query
            performance, event, venue = PerformanceDataSerializer._get_performance_with_relations(performance_id)
            
            # Validate data completeness
            PerformanceDataSerializer._validate_data_completeness(performance, event, venue)
            
            # Build the response structure
            response_data = {
                "success": True,
                "data": {
                    "scrape_completion": PerformanceDataSerializer._format_scrape_completion_data(
                        scrape_job_id, performance
                    ),
                    "performance": PerformanceDataSerializer._format_performance_data(performance),
                    "event": PerformanceDataSerializer._format_event_data(event),
                    "venue": PerformanceDataSerializer._format_venue_data(venue)
                }
            }
            
            logger.info(f"Successfully serialized performance data for {performance_id}")
            return response_data
            
        except Exception as e:
            error_msg = f"Failed to serialize performance data for {performance_id}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            return {
                "success": False,
                "error": {
                    "message": error_msg,
                    "performance_id": performance_id,
                    "timestamp": timezone.now().isoformat()
                }
            }
    
    @staticmethod
    def _get_performance_with_relations(performance_id: str):
        """
        Get performance with all related data in a single optimized query.
        
        Returns:
            Tuple of (performance, event, venue)
        """
        from scrapers.models import Performance
        
        try:
            performance = Performance.objects.select_related(
                'event_id',
                'venue_id'
            ).get(
                internal_performance_id=performance_id,
                is_active=True
            )
            
            event = performance.event_id
            venue = performance.venue_id
            
            logger.debug(f"Retrieved performance {performance_id} with relations")
            return performance, event, venue
            
        except Performance.DoesNotExist:
            raise PerformanceDataSerializationError(
                f"Performance {performance_id} not found or inactive"
            )
        except Exception as e:
            raise PerformanceDataSerializationError(
                f"Database error retrieving performance {performance_id}: {str(e)}"
            )
    
    @staticmethod
    def _validate_data_completeness(performance, event, venue):
        """
        Validate that all required data is present and complete.
        
        Raises:
            PerformanceDataSerializationError: If required data is missing
        """
        errors = []
        
        # Validate performance
        if not performance:
            errors.append("Performance object is None")
        else:
            if not performance.internal_performance_id:
                errors.append("Performance missing internal_performance_id")
            if not performance.performance_datetime_utc:
                errors.append("Performance missing performance_datetime_utc")
            if not performance.source_website:
                errors.append("Performance missing source_website")
        
        # Validate event
        if not event:
            errors.append("Event object is None")
        else:
            if not event.internal_event_id:
                errors.append("Event missing internal_event_id")
            if not event.name:
                errors.append("Event missing name")
            if not event.source_website:
                errors.append("Event missing source_website")
        
        # Validate venue
        if not venue:
            errors.append("Venue object is None")
        else:
            if not venue.internal_venue_id:
                errors.append("Venue missing internal_venue_id")
            if not venue.name:
                errors.append("Venue missing name")
            if not venue.city or not venue.state:
                errors.append("Venue missing city or state")
            if not venue.source_website:
                errors.append("Venue missing source_website")
        
        if errors:
            raise PerformanceDataSerializationError(
                f"Data validation failed: {'; '.join(errors)}"
            )
        
        logger.debug("Data validation passed for performance serialization")
    
    @staticmethod
    def _format_scrape_completion_data(scrape_job_id: Optional[str], performance) -> Dict[str, Any]:
        """Format scrape completion metadata."""
        return {
            "scrape_job_id": scrape_job_id,
            "status": "success",
            "scraped_at": timezone.now().isoformat(),
            "scraper_name": "django_performance_scraper",
            "performance_id": performance.internal_performance_id
        }
    
    @staticmethod
    def _format_performance_data(performance) -> Dict[str, Any]:
        """Format performance data with guaranteed fields."""
        return {
            "id": performance.internal_performance_id,
            "source_performance_id": performance.source_performance_id,
            "source_website": performance.source_website,
            "datetime_utc": performance.performance_datetime_utc.isoformat(),
            "seat_map_url": performance.seat_map_url,
            "map_dimensions": {
                "width": performance.map_width,
                "height": performance.map_height
            } if performance.map_width and performance.map_height else None,
            "pos_enabled": performance.pos_enabled,
            "pos_enabled_at": performance.pos_enabled_at.isoformat() if performance.pos_enabled_at else None,
            "pos_disabled_at": performance.pos_disabled_at.isoformat() if performance.pos_disabled_at else None,
            "created_at": performance.created_at.isoformat(),
            "updated_at": performance.updated_at.isoformat() if performance.updated_at else None
        }
    
    @staticmethod
    def _format_event_data(event) -> Dict[str, Any]:
        """Format event data with guaranteed fields."""
        return {
            "id": event.internal_event_id,
            "source_event_id": event.source_event_id,
            "source_website": event.source_website,
            "name": event.name,
            "url": event.url,
            "currency": event.currency,
            "event_type": event.event_type,
            "created_at": event.created_at.isoformat(),
            "updated_at": event.updated_at.isoformat() if event.updated_at else None,
            "is_active": event.is_active
        }
    
    @staticmethod
    def _format_venue_data(venue) -> Dict[str, Any]:
        """Format venue data with guaranteed fields and seating configuration."""
        return {
            "id": venue.internal_venue_id,
            "source_venue_id": venue.source_venue_id,
            "source_website": venue.source_website,
            "name": venue.name,
            "address": venue.address,
            "city": venue.city,
            "state": venue.state,
            "country": venue.country,
            "postal_code": venue.postal_code,
            "timezone": venue.venue_timezone,
            "url": venue.url,
            "seating_config": {
                "seat_structure": venue.seat_structure,
                "previous_seat_structure": venue.previous_seat_structure,
                "markup_type": venue.price_markup_type,
                "markup_value": float(venue.price_markup_value) if venue.price_markup_value else None,
                "markup_updated_at": venue.price_markup_updated_at.isoformat() if venue.price_markup_updated_at else None
            },
            "pos_enabled": venue.pos_enabled,
            "pos_enabled_at": venue.pos_enabled_at.isoformat() if venue.pos_enabled_at else None,
            "created_at": venue.created_at.isoformat(),
            "updated_at": venue.updated_at.isoformat() if venue.updated_at else None,
            "is_active": venue.is_active
        }
    
    @staticmethod
    def serialize_for_rabbitmq_message(performance_id: str, scrape_job_id: str) -> Dict[str, Any]:
        """
        Create a RabbitMQ-ready message for NestJS consumption.
        
        Args:
            performance_id: Django internal performance ID
            scrape_job_id: Scrape job ID for tracking
            
        Returns:
            Dict formatted for RabbitMQ pattern messaging
        """
        try:
            # Get serialized data
            serialized_data = PerformanceDataSerializer.serialize_for_nestjs(
                performance_id, scrape_job_id
            )
            
            if not serialized_data.get("success"):
                # Return error message format
                return {
                    "pattern": "scrape.performance.error",
                    "data": {
                        "scrapeJobId": scrape_job_id,
                        "error": serialized_data.get("error", {}).get("message", "Unknown serialization error"),
                        "performance_id": performance_id,
                        "timestamp": timezone.now().isoformat()
                    }
                }
            
            # Extract the data for RabbitMQ message
            data = serialized_data["data"]
            
            # Format for NestJS microservices pattern
            return {
                "pattern": "scrape.performance.success",
                "data": {
                    "scrapeJobId": scrape_job_id,
                    "userId": None,  # Will be set by caller if available
                    "result": {
                        # Core identification
                        "url": data["event"]["url"] or f"performance://{performance_id}",
                        "performance_key": performance_id,  # Legacy compatibility
                        "internal_performance_id": data["performance"]["id"],
                        "internal_event_id": data["event"]["id"],
                        "internal_venue_id": data["venue"]["id"],
                        
                        # Metadata
                        "scraper_name": data["scrape_completion"]["scraper_name"],
                        "status": "success",
                        "scraped_at": data["scrape_completion"]["scraped_at"],
                        "venue_timezone": data["venue"]["timezone"],
                        
                        # Structured data sections
                        "event_info": {
                            "name": data["event"]["name"],
                            "title": data["event"]["name"],  # Alias for compatibility
                            "source_event_id": data["event"]["source_event_id"],
                            "source_website": data["event"]["source_website"],
                            "url": data["event"]["url"],
                            "currency": data["event"]["currency"],
                            "event_type": data["event"]["event_type"]
                        },
                        "venue_info": {
                            "name": data["venue"]["name"],
                            "source_venue_id": data["venue"]["source_venue_id"],
                            "source_website": data["venue"]["source_website"],
                            "city": data["venue"]["city"],
                            "state": data["venue"]["state"],
                            "country": data["venue"]["country"],
                            "address": data["venue"]["address"],
                            "venue_timezone": data["venue"]["timezone"],
                            "url": data["venue"]["url"]
                        },
                        "performance_info": {
                            "source_performance_id": data["performance"]["source_performance_id"],
                            "source_website": data["performance"]["source_website"],
                            "performance_datetime_utc": data["performance"]["datetime_utc"],
                            "seat_map_url": data["performance"]["seat_map_url"],
                            "map_dimensions": data["performance"]["map_dimensions"],
                            "pos_enabled": data["performance"]["pos_enabled"]
                        }
                    }
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to create RabbitMQ message for {performance_id}: {str(e)}")
            return {
                "pattern": "scrape.performance.error",
                "data": {
                    "scrapeJobId": scrape_job_id,
                    "error": f"Failed to serialize performance data: {str(e)}",
                    "performance_id": performance_id,
                    "timestamp": timezone.now().isoformat()
                }
            }