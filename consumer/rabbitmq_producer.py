import json
import logging
import pika
import os
from django.utils import timezone
from django.core.cache import cache
from django.core.serializers.json import DjangoJSONEncoder

logger = logging.getLogger(__name__)


class CustomJSONEncoder(DjangoJSONEncoder):
    """Custom JSON encoder that handles Django models and datetime objects"""

    def default(self, obj):
        # Handle datetime objects
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()

        # Handle Django model instances by converting to dict
        if hasattr(obj, '_meta') and hasattr(obj, 'pk'):
            return self._serialize_model(obj)

        # Use Django's default encoder for other types
        return super().default(obj)

    def _serialize_model(self, obj):
        """Convert Django model to serializable dict"""
        data = {}
        for field in obj._meta.fields:
            field_name = field.name
            field_value = getattr(obj, field_name, None)

            if field_value is None:
                data[field_name] = None
            elif hasattr(field_value, 'isoformat'):  # datetime objects
                data[field_name] = field_value.isoformat()
            elif hasattr(field_value, '_meta'):  # Foreign key objects
                data[field_name] = {
                    'pk': field_value.pk,
                    'str': str(field_value)
                }
            else:
                data[field_name] = field_value

        return data


def safe_json_dumps(obj, **kwargs):
    """Safely serialize object to JSON with custom encoder"""
    try:
        return json.dumps(obj, cls=CustomJSONEncoder, **kwargs)
    except Exception as e:
        logger.error(f"JSON serialization failed: {e}")
        # Return a safe fallback
        return json.dumps({
            'error': f'Serialization failed: {str(e)}',
            'original_type': str(type(obj))
        })


class RabbitMQProducer:
    """
    Simplified RabbitMQ producer for sending event IDs to NestJS
    """

    def __init__(self):
        """Initialize the RabbitMQ connection and Redis client"""
        self.connection = None
        self.channel = None
        self.queue_name = 'django_to_nest'
        self.response_exchange = ''
        self.rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
        self.rabbitmq_port = int(os.environ.get('RABBITMQ_PORT', 5672))
        self.rabbitmq_user = os.environ.get('RABBITMQ_USER', 'admin')
        self.rabbitmq_password = os.environ.get('RABBITMQ_PASSWORD', 'admin123')

    def connect(self):
        """Establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(
                self.rabbitmq_user,
                self.rabbitmq_password
            )

            logger.info(f"Connecting to RabbitMQ at {self.rabbitmq_host}:{self.rabbitmq_port}")
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.rabbitmq_host,
                    port=self.rabbitmq_port,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )

            self.channel = self.connection.channel()

            # Declare queue
            self.channel.queue_declare(
                queue=self.queue_name,
                durable=True
            )

            logger.info(f"Connected to RabbitMQ and declared queue '{self.queue_name}'")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}", exc_info=True)
            return False

    def close(self):
        """Close the connection"""
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.debug("RabbitMQ connection closed")

    def send_scrape_completed(self, scrape_job_id, scraped_data):
        """
        Store scraped data in Redis and send completion notification to NestJS

        Args:
            scrape_job_id (str): The scrape job ID from NestJS
            scraped_data (dict): The scraped data to store in Redis

        Returns:
            bool: True if successful, False otherwise
        """
        if not scrape_job_id:
            logger.warning("Cannot send scrape completion: no scrape_job_id provided")
            return False

        try:
            # Extract performance_id from scraped_data
            performance_id = scraped_data.get('performance_id') or scraped_data.get('internal_performance_id')

            if not performance_id:
                logger.warning("No performance_id found in scraped data")
                return False

            # Get performance_time and event_id from database
            event_id = None
            performance_time = None
            venue_id = None

            try:
                from scrapers.models import Performance
                
                # Handle different performance ID formats - database stores without prefix
                db_performance_id = performance_id
                if performance_id.startswith('wp_perf_'):
                    db_performance_id = performance_id.replace('wp_perf_', '')
                
                performance = Performance.objects.get(source_performance_id=db_performance_id)
                performance_time = performance.performance_datetime_utc.isoformat()
                event_id = performance.event_id.internal_event_id if performance.event_id else None
                # Get venue_id from the performance
                venue_id = performance.venue_id.internal_venue_id if performance.venue_id else None
                logger.info(f"Retrieved performance data from database - performance_time: {performance_time}, event_id: {event_id}, venue_id: {venue_id}")

            except Exception as db_error:
                logger.error(f"Could not retrieve performance data from database: {db_error}")
                return False

            logger.info(f"Stored scraped data in Redis for performance {performance_id}")

            # Send completion notification to NestJS using the new message format
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return False

            # Get venue configuration to include in scrape completion message
            logger.info(f"🔄 Getting venue config for scrape completion, venue_id: {venue_id}")
            venue_config = self._get_venue_config_for_scrape_completion(venue_id)
            logger.info(f"📤 Venue config result: {venue_config}")

            # Build message data for NestJS microservices
            message_data = {
                'scrapeJobId': scrape_job_id,
                'userId': None,  # No user context in scraper
                'result': {
                    'url': scraped_data.get('url', ''),
                    'performance_key': str(performance_id),  # Keep for backwards compatibility
                    'internal_performance_id': str(performance_id),
                    'internal_event_id': f"{event_id}" if event_id else scraped_data.get('internal_event_id', 'missing_event_id'),
                    'internal_venue_id': f"{venue_id}" if venue_id else scraped_data.get('internal_venue_id'),
                    'venue_timezone': scraped_data.get('venue_timezone', 'America/Chicago'),
                    'scraper_name': scraped_data.get('scraper_name', 'washington_pavilion_scraper_v5'),
                    'status': 'success',
                    'scraped_at': timezone.now().isoformat(),
                    # Include structured data for NestJS processing
                    'event_info': scraped_data.get('event_info'),
                    'venue_info': scraped_data.get('venue_info'),
                    'performance_info': scraped_data.get('performance_info'),
                    # Include venue configuration for backend update
                    'venue_config': venue_config
                }
            }
            
            logger.info(f"📋 Complete message data structure being sent:")
            logger.info(f"   - scrapeJobId: {scrape_job_id}")
            logger.info(f"   - result.venue_config: {message_data['result'].get('venue_config')}")
            if venue_config:
                logger.info(f"✅ Venue config is included in message")
            else:
                logger.warning(f"⚠️  Venue config is None - will not be processed by backend")

            # For NestJS microservices, we need to send the message with the pattern as headers
            message = {
                'pattern': 'scrape.performance.success',
                'data': message_data
            }

            message_body = safe_json_dumps(message)

            # Send the message to NestJS microservice
            self.channel.basic_publish(
                exchange=self.response_exchange,
                routing_key=self.queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )

            logger.info(f"Sent scrape success notification for scrape job {scrape_job_id}, performance {performance_id}, event {event_id}")
            return True

        except Exception as e:
            logger.error(f"Error sending scrape completion: {str(e)}", exc_info=True)
            return False

    def send_test_message(self):
        """Send a test message to verify RabbitMQ connection"""
        try:
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return False

            test_message = {
                'pattern': 'test.message',
                'data': {
                    'test': True,
                    'timestamp': timezone.now().isoformat(),
                    'message': 'This is a test message from Django'
                }
            }

            message_body = safe_json_dumps(test_message)
            
            self.channel.basic_publish(
                exchange=self.response_exchange,
                routing_key=self.queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            
            logger.info("Sent test message to NestJS")
            return True
            
        except Exception as e:
            logger.error(f"Error sending test message: {str(e)}", exc_info=True)
            return False

    def send_performance_data_response(self, response_data):
        """
        Send performance data response to NestJS for WebSocket delivery

        Args:
            response_data (dict): The response data containing:
                - performanceId: str
                - requestId: str  
                - success: bool
                - data: dict (optional, if success=True)
                - error: str (optional, if success=False)

        Returns:
            bool: True if successful, False otherwise
        """
        if not response_data.get('performanceId') or not response_data.get('requestId'):
            logger.warning("Cannot send performance data response: missing performanceId or requestId")
            return False

        try:
            # Ensure connection is established
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return False

            message = {
                'pattern': 'performance_data_response',
                'data': {
                    'performanceId': str(response_data.get('performanceId')),
                    'requestId': str(response_data.get('requestId')),
                    'success': response_data.get('success', False),
                    'data': response_data.get('data'),
                    'error': response_data.get('error')
                }
            }

            message_body = safe_json_dumps(message)

            self.channel.basic_publish(
                exchange=self.response_exchange,
                routing_key=self.queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )

            logger.info(f"Sent performance data response for performance {response_data.get('performanceId')}, request {response_data.get('requestId')}")
            return True

        except Exception as e:
            logger.error(f"Error sending performance data response: {str(e)}", exc_info=True)
            return False

    def send_message(self, message_data):
        """
        Send a generic structured message to RabbitMQ
        
        Args:
            message_data (dict): Message with 'pattern' and 'data' fields
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Ensure connection is established
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return False

            # Validate message structure
            if not isinstance(message_data, dict) or 'pattern' not in message_data:
                logger.error("Invalid message format: missing 'pattern' field")
                return False

            message_body = safe_json_dumps(message_data)

            self.channel.basic_publish(
                exchange=self.response_exchange,
                routing_key=self.queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )

            pattern = message_data.get('pattern', 'unknown')
            scrape_job_id = message_data.get('data', {}).get('scrapeJobId', 'unknown')
            logger.info(f"Sent message with pattern '{pattern}' for job {scrape_job_id}")
            return True

        except Exception as e:
            logger.error(f"Error sending message: {str(e)}", exc_info=True)
            return False


    def _get_venue_config_for_scrape_completion(self, venue_id):
        """
        Get venue configuration to include in scrape completion message
        
        Args:
            venue_id: str - internal_venue_id
            
        Returns:
            dict: Venue configuration data or None if not found
        """
        try:
            logger.info(f"🔍 Getting venue config for venue_id: {venue_id}")
            if not venue_id:
                logger.warning("No venue_id provided to _get_venue_config_for_scrape_completion")
                return None
                
            # Import here to avoid circular imports
            from scrapers.models.base import Venue
            
            try:
                logger.info(f"🔍 Searching for venue with internal_venue_id: {venue_id}")
                venue = Venue.objects.get(internal_venue_id=venue_id)
                
                venue_config = {
                    'venue_id': venue_id,
                    'seat_structure': venue.seat_structure,
                    'markup_type': venue.price_markup_type,
                    'markup_value': float(venue.price_markup_value) if venue.price_markup_value else None,
                    'pos_enabled': venue.pos_enabled,
                    'updated_at': timezone.now().isoformat()
                }
                
                logger.info(f"✅ Found venue config: {venue_config}")
                return venue_config
                
            except Venue.DoesNotExist:
                logger.warning(f"❌ Venue {venue_id} not found in database")
                # Let's also check what venues exist
                from scrapers.models.base import Venue
                all_venues = Venue.objects.values('internal_venue_id', 'name')[:10]
                logger.info(f"📋 Available venues (first 10): {list(all_venues)}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error getting venue config for {venue_id}: {str(e)}", exc_info=True)
            return None


# Create a singleton instance
producer = RabbitMQProducer()