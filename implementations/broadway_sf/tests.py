import asyncio
from unittest import IsolatedAsyncioTestCase
from .scraper import BroadwaySFScraper

class TestBroadwaySFScraper(IsolatedAsyncioTestCase):
    async def test_scrape_and_juliet(self):
        url = "https://www.broadwaysf.com/events/bsf-sofiane-pamart/curran-theater/tickets/D9A0B9B6-0252-4655-A635-BA2ED0B1352F"
        scraper = BroadwaySFScraper(url=url)
        data = await scraper.extract_data()
        processed_data = await scraper.process_data(data)
        self.assertEqual(processed_data['event_info']['name'], 'Sofiane Pamart: PIANO TOUR 2025 - USA & CANADA')