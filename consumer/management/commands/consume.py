from django.core.management.base import BaseCommand
from consumer.consumer import start_consumer
import signal
import sys


class Command(BaseCommand):
    help = 'Starts the RabbitMQ consumer'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection = None
        self.channel = None

        # Set up signal handlers
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

    def handle_exit(self, sig, frame):
        self.stdout.write(self.style.WARNING('\nReceived exit signal. Stopping consumer...'))
        # Cleanup will be handled by start_consumer's own exception handling
        sys.exit(0)

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting RabbitMQ consumer...'))
        try:
            start_consumer()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('\nConsumer stopped manually.'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'\nError: {e}'))


