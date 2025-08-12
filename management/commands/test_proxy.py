"""
Django management command to test proxy configurations.

This command tests proxy configurations to ensure they are working correctly
and can be used by scrapers.
"""

import asyncio
import aiohttp
import time
from django.core.management.base import BaseCommand, CommandError
from scrapers.proxy.service import get_proxy_service
from scrapers.proxy.base import ProxyType
from scrapers.models import ProxyConfiguration


class Command(BaseCommand):
    help = 'Test proxy configurations'

    def add_arguments(self, parser):
        parser.add_argument(
            '--scraper',
            type=str,
            help='Test proxy for specific scraper'
        )
        parser.add_argument(
            '--provider',
            type=str,
            help='Test specific provider (e.g., webshare, bright_data)'
        )
        parser.add_argument(
            '--proxy-type',
            type=str,
            choices=['residential', 'datacenter'],
            help='Test specific proxy type'
        )
        parser.add_argument(
            '--config-id',
            type=int,
            help='Test specific proxy configuration by ID'
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Test all available proxy configurations'
        )
        parser.add_argument(
            '--timeout',
            type=int,
            default=30,
            help='Request timeout in seconds (default: 30)'
        )

    def handle(self, *args, **options):
        if options['all']:
            asyncio.run(self._test_all_configurations(options['timeout']))
        elif options['config_id']:
            asyncio.run(self._test_configuration_by_id(options['config_id'], options['timeout']))
        elif options['scraper']:
            asyncio.run(self._test_scraper_proxy(options['scraper'], options['timeout']))
        elif options['provider'] and options['proxy_type']:
            asyncio.run(self._test_provider_proxy(
                options['provider'], 
                options['proxy_type'], 
                options['timeout']
            ))
        else:
            raise CommandError(
                'Specify --all, --config-id, --scraper, or both --provider and --proxy-type'
            )

    async def _test_all_configurations(self, timeout: int):
        """Test all available proxy configurations."""
        self.stdout.write(self.style.SUCCESS('Testing all proxy configurations...'))
        self.stdout.write('')

        configs = ProxyConfiguration.objects.select_related('provider').filter(is_active=True)
        if not configs:
            self.stdout.write('No active proxy configurations found.')
            return

        results = []
        for config in configs:
            result = await self._test_proxy_config(config, timeout)
            results.append(result)

        self._print_test_summary(results)

    async def _test_configuration_by_id(self, config_id: int, timeout: int):
        """Test specific proxy configuration by ID."""
        try:
            config = ProxyConfiguration.objects.select_related('provider').get(
                id=config_id, 
                is_active=True
            )
        except ProxyConfiguration.DoesNotExist:
            raise CommandError(f'Proxy configuration with ID {config_id} not found or inactive')

        self.stdout.write(f'Testing proxy configuration ID {config_id}...')
        result = await self._test_proxy_config(config, timeout)
        self._print_single_result(result)

    async def _test_scraper_proxy(self, scraper_name: str, timeout: int):
        """Test proxy configuration for specific scraper."""
        self.stdout.write(f'Testing proxy for scraper: {scraper_name}')
        
        credentials = proxy_service.get_proxy_for_scraper(scraper_name)
        if not credentials:
            self.stdout.write(self.style.ERROR('No proxy configuration found for scraper'))
            return

        result = await self._test_proxy_credentials(credentials, scraper_name, timeout)
        self._print_single_result(result)

    async def _test_provider_proxy(self, provider_name: str, proxy_type: str, timeout: int):
        """Test proxy configuration for specific provider and type."""
        try:
            config = ProxyConfiguration.objects.select_related('provider').get(
                provider__name=provider_name,
                proxy_type=proxy_type,
                is_active=True
            )
        except ProxyConfiguration.DoesNotExist:
            raise CommandError(
                f'No active {proxy_type} proxy configuration found for provider {provider_name}'
            )

        self.stdout.write(f'Testing {provider_name} {proxy_type} proxy...')
        result = await self._test_proxy_config(config, timeout)
        self._print_single_result(result)

    async def _test_proxy_config(self, config: ProxyConfiguration, timeout: int):
        """Test a specific proxy configuration."""
        from scrapers.proxy.base import ProxyCredentials
        
        credentials = ProxyCredentials(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            proxy_type=ProxyType(config.proxy_type)
        )

        test_name = f"{config.provider.display_name} {config.proxy_type}"
        return await self._test_proxy_credentials(credentials, test_name, timeout)

    async def _test_proxy_credentials(self, credentials, test_name: str, timeout: int):
        """Test proxy credentials by making HTTP requests."""
        start_time = time.time()
        
        # Test URLs to check proxy functionality
        test_urls = [
            'http://httpbin.org/ip',  # Returns IP address
            'https://httpbin.org/ip',  # Returns IP address via HTTPS
        ]

        result = {
            'name': test_name,
            'success': False,
            'response_time': 0,
            'ip_address': None,
            'error': None,
            'details': {}
        }

        try:
            # Configure proxy for aiohttp
            proxy_url = f"http://{credentials.username}:{credentials.password}@{credentials.host}:{credentials.port}"
            
            connector = aiohttp.TCPConnector()
            async with aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as session:
                
                for test_url in test_urls:
                    try:
                        async with session.get(test_url, proxy=proxy_url) as response:
                            if response.status == 200:
                                data = await response.json()
                                result['ip_address'] = data.get('origin', 'Unknown')
                                result['success'] = True
                                result['details'][test_url] = {
                                    'status': response.status,
                                    'ip': data.get('origin'),
                                    'success': True
                                }
                                break
                            else:
                                result['details'][test_url] = {
                                    'status': response.status,
                                    'success': False,
                                    'error': f'HTTP {response.status}'
                                }
                    except Exception as e:
                        result['details'][test_url] = {
                            'success': False,
                            'error': str(e)
                        }

        except Exception as e:
            result['error'] = str(e)

        result['response_time'] = time.time() - start_time
        return result

    def _print_single_result(self, result):
        """Print result for a single proxy test."""
        if result['success']:
            self.stdout.write(
                self.style.SUCCESS(
                    f"✓ {result['name']} - Success "
                    f"(IP: {result['ip_address']}, Time: {result['response_time']:.2f}s)"
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(
                    f"✗ {result['name']} - Failed: {result.get('error', 'Unknown error')}"
                )
            )

        # Print detailed results if available
        if result['details']:
            for url, details in result['details'].items():
                status = "✓" if details.get('success') else "✗"
                self.stdout.write(f"  {status} {url}: {details}")

    def _print_test_summary(self, results):
        """Print summary of all proxy tests."""
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS('Test Summary:'))
        self.stdout.write('=' * 50)

        successful = sum(1 for r in results if r['success'])
        total = len(results)

        for result in results:
            self._print_single_result(result)

        self.stdout.write('')
        self.stdout.write(f'Total: {total}, Successful: {successful}, Failed: {total - successful}')
        
        if successful == total:
            self.stdout.write(self.style.SUCCESS('All proxy configurations are working! ✓'))
        elif successful > 0:
            self.stdout.write(self.style.WARNING(f'{total - successful} proxy configuration(s) failed'))
        else:
            self.stdout.write(self.style.ERROR('All proxy configurations failed'))