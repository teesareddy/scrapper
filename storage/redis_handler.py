# scrapers/storage/redis_handler.py
import json
import redis
import logging
from django.conf import settings
from django.core.serializers.json import DjangoJSONEncoder
from typing import Dict, Any, Optional


class SafeRedisJSONEncoder(DjangoJSONEncoder):
    """Safe JSON encoder for Redis storage"""

    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        if hasattr(obj, '_meta') and hasattr(obj, 'pk'):
            return {
                'model': f"{obj._meta.app_label}.{obj._meta.model_name}",
                'pk': obj.pk,
                'str': str(obj)
            }
        return super().default(obj)


def safe_redis_json_dumps(obj, **kwargs):
    """Safely serialize object to JSON for Redis"""
    try:
        return json.dumps(obj, cls=SafeRedisJSONEncoder, **kwargs)
    except Exception as e:
        logging.error(f"Redis JSON serialization failed: {e}")
        return json.dumps({
            'error': f'Redis serialization failed: {str(e)}',
            'object_type': str(type(obj))
        })


class RedisStorageHandler:
    """Handles Redis storage operations for scraped data"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.redis_client = self._init_redis()

    def _init_redis(self) -> Optional[redis.Redis]:
        """Initialize Redis connection"""
        try:
            client = redis.Redis(
                host=getattr(settings, 'REDIS_HOST', 'redis'),
                port=getattr(settings, 'REDIS_PORT', 6379),
                db=getattr(settings, 'REDIS_DB', 0),
                password=getattr(settings, 'REDIS_PASSWORD', None),
                decode_responses=True
            )
            client.ping()
            self.logger.info("Redis connection established successfully")
            return client
        except Exception as e:
            self.logger.error(f"Redis connection failed: {e}")
            return None

    def store_performance_data(self, performance_id: str, data: Dict[str, Any]) -> bool:
        """
        Store performance data in Redis using performance ID as key

        Args:
            performance_id: Performance ID (performance_key from database)
            data: Performance data to store
        """
        if not self.redis_client or not performance_id:
            self.logger.warning(
                f"Cannot store data: Redis client={bool(self.redis_client)}, performance_id={performance_id}")
            return False

        try:
            cache_key = f"performance_data:{performance_id}"

            redis_data = {
                "performance_id": performance_id,
                "success": data.get("success", True),
                "scraped_at": data.get("scraped_at"),
                "url": data.get("url"),
                "data": data
            }

            redis_json = safe_redis_json_dumps(redis_data)
            self.redis_client.setex(cache_key, 86400, redis_json)
            self.logger.info(f"Successfully stored data in Redis for performance {performance_id} with key {cache_key}")
            return True

        except Exception as e:
            self.logger.error(f"Redis storage failed for performance {performance_id}: {e}", exc_info=True)
            return False

    def get_performance_data(self, performance_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve performance data from Redis using performance ID

        Args:
            performance_id: Performance ID (performance_key from database)
        """
        if not self.redis_client or not performance_id:
            self.logger.warning(
                f"Cannot retrieve data: Redis client={bool(self.redis_client)}, performance_id={performance_id}")
            return None

        try:
            cache_key = f"performance_data:{performance_id}"
            data = self.redis_client.get(cache_key)

            if data:
                self.logger.info(f"Successfully retrieved data from Redis for performance {performance_id}")
                return json.loads(data)
            else:
                self.logger.info(f"No data found in Redis for performance {performance_id} with key {cache_key}")
                return None

        except Exception as e:
            self.logger.error(f"Redis retrieval failed for performance {performance_id}: {e}", exc_info=True)
            return None

    def store_error(self, scrape_job_id: str, error: str, url: str = None) -> bool:
        """
        Store error information in Redis using scrape job ID
        (Errors use scrape_job_id since there's no performance_id when scraping fails)

        Args:
            scrape_job_id: NestJS scrape job tracking ID
            error: Error message
            url: URL that failed to scrape
        """
        if not self.redis_client or not scrape_job_id:
            self.logger.warning(
                f"Cannot store error: Redis client={bool(self.redis_client)}, scrape_job_id={scrape_job_id}")
            return False

        try:
            cache_key = f"scrape_error:{scrape_job_id}"

            error_data = {
                "scrape_job_id": scrape_job_id,
                "success": False,
                "error": error,
                "url": url,
                "scraped_at": None
            }

            error_json = safe_redis_json_dumps(error_data)
            self.redis_client.setex(cache_key, 86400, error_json)
            self.logger.info(f"Successfully stored error in Redis for scrape job {scrape_job_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error storage failed for scrape job {scrape_job_id}: {e}", exc_info=True)
            return False

    def delete_performance_data(self, performance_id: str) -> bool:
        """
        Delete performance data from Redis using performance ID

        Args:
            performance_id: Performance ID (performance_key from database)
        """
        if not self.redis_client or not performance_id:
            return False

        try:
            cache_key = f"performance_data:{performance_id}"
            result = self.redis_client.delete(cache_key)
            self.logger.info(f"Deleted data from Redis for performance {performance_id}: {bool(result)}")
            return bool(result)

        except Exception as e:
            self.logger.error(f"Redis deletion failed for performance {performance_id}: {e}")
            return False

    def check_connection(self) -> bool:
        """Check if Redis connection is working"""
        if not self.redis_client:
            return False

        try:
            self.redis_client.ping()
            return True
        except Exception as e:
            self.logger.error(f"Redis connection check failed: {e}")
            return False