# scrapers/management/commands/test_redis.py
from django.core.management.base import BaseCommand
from scrapers.storage.redis_handler import RedisStorageHandler
import json


class Command(BaseCommand):
    help = 'Test Redis connection and storage'

    def handle(self, *args, **options):
        self.stdout.write("Testing Redis connection...")

        redis_handler = RedisStorageHandler()

        # Test connection
        if redis_handler.check_connection():
            self.stdout.write(self.style.SUCCESS("✅ Redis connection successful"))
        else:
            self.stdout.write(self.style.ERROR("❌ Redis connection failed"))
            return

        # Test data storage
        test_event_id = "test_event_123"
        test_data = {
            "success": True,
            "url": "https://test.example.com",
            "scraped_at": "2025-06-06T19:00:00Z",
            "data": {
                "zones": [{"zone_id": 1, "name": "Test Zone"}],
                "seats": [{"seat_id": 1, "row": "A", "number": "1"}]
            }
        }

        self.stdout.write("Testing data storage...")
        if redis_handler.store_performance_data(test_event_id, test_data):
            self.stdout.write(self.style.SUCCESS("✅ Data storage successful"))
        else:
            self.stdout.write(self.style.ERROR("❌ Data storage failed"))
            return

        # Test data retrieval
        self.stdout.write("Testing data retrieval...")
        retrieved_data = redis_handler.get_performance_data(test_event_id)
        if retrieved_data:
            self.stdout.write(self.style.SUCCESS("✅ Data retrieval successful"))
            self.stdout.write(f"Retrieved data keys: {list(retrieved_data.keys())}")
        else:
            self.stdout.write(self.style.ERROR("❌ Data retrieval failed"))
            return

        # Test error storage
        self.stdout.write("Testing error storage...")
        test_error_id = "error_event_456"
        if redis_handler.store_error(test_error_id, "Test error message", "https://error.example.com"):
            self.stdout.write(self.style.SUCCESS("✅ Error storage successful"))
        else:
            self.stdout.write(self.style.ERROR("❌ Error storage failed"))

        # Clean up test data
        redis_handler.delete_performance_data(test_event_id)
        redis_handler.delete_performance_data(test_error_id)

        self.stdout.write(self.style.SUCCESS("🎉 Redis tests completed successfully!"))