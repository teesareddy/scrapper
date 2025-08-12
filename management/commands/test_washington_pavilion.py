from django.core.management.base import BaseCommand
from scrapers.implementations.washington_pavilion.scraper import WashingtonPavilionScraper
import asyncio


class Command(BaseCommand):
    help = 'Test Washington Pavilion scraper'

    def add_arguments(self, parser):
        parser.add_argument(
            '--url',
            type=str,
            default='https://wpmi-3encore.shop.secutix.com/selection/event/date?productId=10229124351731',
            help='URL to scrape'
        )

    def handle(self, *args, **options):
        url = options['url']
        self.stdout.write(f'Testing Washington Pavilion scraper with URL: {url}')
        
        async def test_scraper():
            scraper = WashingtonPavilionScraper(url=url)
            try:
                result = await scraper.scrape()
                self.stdout.write(self.style.SUCCESS(f'Scrape successful! Result: {result}'))
                return result
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Scrape failed: {e}'))
                import traceback
                traceback.print_exc()
                return None
        
        result = asyncio.run(test_scraper())
        if result:
            self.stdout.write(self.style.SUCCESS('Test completed successfully'))
        else:
            self.stdout.write(self.style.ERROR('Test failed'))