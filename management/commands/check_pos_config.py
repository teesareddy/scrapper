from django.core.management.base import BaseCommand
from django.conf import settings
from consumer.services.pos_api_client import POSAPIClient
import json
import requests


class Command(BaseCommand):
    help = 'Check StubHub POS API configuration and connectivity'

    def add_arguments(self, parser):
        parser.add_argument(
            '--test-connection',
            action='store_true',
            help='Test actual connection to POS API endpoint'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed configuration information'
        )

    def handle(self, *args, **options):
        self.stdout.write(self.style.HTTP_INFO('🔧 StubHub POS API Configuration Check'))
        self.stdout.write('=' * 60)
        
        # Check environment variables
        self.check_environment_variables(options['verbose'])
        
        # Check Django settings
        self.check_django_settings()
        
        # Test connection if requested
        if options['test_connection']:
            self.test_pos_api_connection()
        
        self.stdout.write('=' * 60)
        self.stdout.write(self.style.SUCCESS('✅ Configuration check complete'))

    def check_environment_variables(self, verbose=False):
        """Check required environment variables"""
        self.stdout.write('\n📋 Environment Variables:')
        
        required_vars = {
            'STUBHUB_API_BASE_URL': settings.STUBHUB_POS_BASE_URL,
            'STUBHUB_API_TOKEN': settings.STUBHUB_POS_AUTH_TOKEN
        }
        
        for var_name, var_value in required_vars.items():
            if var_value:
                if verbose:
                    # Show partial token for security
                    display_value = var_value
                    if 'TOKEN' in var_name and len(var_value) > 10:
                        display_value = f"{var_value[:5]}...{var_value[-3:]}"
                    self.stdout.write(f'  ✅ {var_name}: {display_value}')
                else:
                    self.stdout.write(f'  ✅ {var_name}: Set')
            else:
                self.stdout.write(f'  ❌ {var_name}: Not set')
                self.stdout.write(f'     Set with: export {var_name}=<your_value>')

    def check_django_settings(self):
        """Check Django settings configuration"""
        self.stdout.write('\n⚙️  Django Settings:')
        
        # Check if POS integration is enabled
        pos_enabled = getattr(settings, 'POS_API_ENABLED', False)
        if pos_enabled:
            self.stdout.write('  ✅ POS API integration: Enabled')
        else:
            self.stdout.write('  ❌ POS API integration: Disabled')
        
        # Check base URL format
        base_url = settings.STUBHUB_POS_BASE_URL
        if base_url:
            if base_url.startswith(('http://', 'https://')):
                self.stdout.write(f'  ✅ Base URL format: Valid ({base_url})')
            else:
                self.stdout.write(f'  ⚠️  Base URL format: Invalid ({base_url})')
                self.stdout.write('     Should start with http:// or https://')
        
        # Check auth token format
        auth_token = settings.STUBHUB_POS_AUTH_TOKEN
        if auth_token:
            if len(auth_token) > 10:  # Basic length check
                self.stdout.write('  ✅ Auth token: Valid length')
            else:
                self.stdout.write('  ⚠️  Auth token: Suspiciously short')
        
    def test_pos_api_connection(self):
        """Test actual connection to POS API endpoint"""
        self.stdout.write('\n🌐 Connection Test:')
        
        if not settings.STUBHUB_POS_BASE_URL or not settings.STUBHUB_POS_AUTH_TOKEN:
            self.stdout.write('  ❌ Cannot test connection: Missing configuration')
            return
        
        try:
            # Initialize client
            client = POSAPIClient()
            
            # Test with a simple payload to see if we get an authentication response
            test_payload = {
                "externalId": "config_test",
                "quantity": 1,
                "pricePerTicket": 100.00,
                "deliveryMethod": "PDF"
            }
            
            # Make test request
            response = client.create_inventory_listing(test_payload)
            
            if response.is_successful:
                self.stdout.write('  ✅ API Connection: Success')
                self.stdout.write(f'     Response: {response.data}')
            else:
                if "Authentication token not configured" in str(response.error):
                    self.stdout.write('  ❌ API Connection: Authentication failed')
                    self.stdout.write('     Error: Token not configured or invalid')
                elif "401" in str(response.error) or "Unauthorized" in str(response.error):
                    self.stdout.write('  ❌ API Connection: Authentication failed')
                    self.stdout.write(f'     Error: {response.error}')
                elif "Connection refused" in str(response.error):
                    self.stdout.write('  ❌ API Connection: Service unavailable')
                    self.stdout.write(f'     Error: {response.error}')
                    self.stdout.write(f'     Check if service is running at: {settings.STUBHUB_POS_BASE_URL}')
                else:
                    self.stdout.write('  ⚠️  API Connection: Other error')
                    self.stdout.write(f'     Error: {response.error}')
                    
        except Exception as e:
            self.stdout.write('  ❌ API Connection: Exception occurred')
            self.stdout.write(f'     Error: {str(e)}')
    
    def handle_error(self, message):
        """Handle and display errors"""
        self.stdout.write(self.style.ERROR(f'❌ {message}'))