from django.core.management.base import BaseCommand
from consumer.rabbitmq_producer import producer


class Command(BaseCommand):
    help = 'Test RabbitMQ connection by sending a test message'

    def handle(self, *args, **options):
        self.stdout.write('Sending test message to RabbitMQ...')
        
        try:
            result = producer.send_test_message()
            if result:
                self.stdout.write(self.style.SUCCESS('Test message sent successfully!'))
            else:
                self.stdout.write(self.style.ERROR('Failed to send test message'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))