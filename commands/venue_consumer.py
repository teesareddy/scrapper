# consumer/management/commands/venue_consumer.py
from django.core.management.base import BaseCommand
from consumer.venue_reply_consumer import start_venue_reply_consumer
import signal
import sys


class Command(BaseCommand):
    help = 'Starts the venue reply queue consumer'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Set up signal handlers
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGTERM, self.handle_exit)

    def handle_exit(self, sig, frame):
        self.stdout.write(self.style.WARNING('\nReceived exit signal. Stopping venue consumer...'))
        sys.exit(0)

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting venue reply queue consumer...'))
        try:
            start_venue_reply_consumer()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING('\nVenue consumer stopped manually.'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'\nError: {e}'))