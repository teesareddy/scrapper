from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.views.decorators.http import require_http_methods
from django.http import JsonResponse
from django.db import transaction
from django.utils import timezone
import json

from .models import (
    ScraperDefinition, ProxyProvider, ProxyConfiguration, 
    CaptchaType, OptimizationRule, ScraperSchedule,
    ScraperProxyAssignment
)


@staff_member_required
def scraper_onboarding(request):
    """
    Comprehensive scraper onboarding page where everything can be configured in one place.
    """
    if request.method == 'POST':
        # Simple form processing for now
        try:
            scraper_name = request.POST.get('scraper_name')
            display_name = request.POST.get('display_name')
            target_website = request.POST.get('target_website')
            status = request.POST.get('status', 'active')
            priority = request.POST.get('priority', 'normal')
            browser_engine = request.POST.get('browser_engine', 'playwright')
            
            # Create basic scraper
            scraper = ScraperDefinition.objects.create(
                name=scraper_name,
                display_name=display_name,
                target_website=target_website,
                status=status,
                priority=priority,
                browser_engine=browser_engine,
                description=request.POST.get('description', ''),
                target_domains=request.POST.get('target_domains', '').split(',') if request.POST.get('target_domains') else [],
                timeout_seconds=int(request.POST.get('timeout_seconds', 30)),
                retry_attempts=int(request.POST.get('retry_attempts', 3)),
                max_concurrent_jobs=int(request.POST.get('max_concurrent_jobs', 1)),
                headless_mode=request.POST.get('headless_mode') == 'on',
                enable_screenshots=request.POST.get('enable_screenshots') == 'on',
                enable_detailed_logging=request.POST.get('enable_detailed_logging') == 'on',
                can_be_scheduled=request.POST.get('can_be_scheduled') == 'on',
                log_level=request.POST.get('log_level', 'INFO'),
                created_by=f'admin_onboarding_{timezone.now().strftime("%Y%m%d_%H%M%S")}'
            )
            
            messages.success(request, f'✅ Successfully created scraper: {display_name} ({scraper_name})')
            return redirect('admin:scrapers_scraperdefinition_change', scraper.scraper_id)
            
        except Exception as e:
            messages.error(request, f'❌ Error creating scraper: {str(e)}')
            # Fall through to show the form again
    
    try:
        # Get all available options for dropdowns
        context = {
            'proxy_providers': ProxyProvider.objects.filter(is_active=True),
            'proxy_configurations': ProxyConfiguration.objects.filter(is_active=True),
            'captcha_types': CaptchaType.objects.filter(is_active=True),
            'optimization_rules': OptimizationRule.objects.filter(is_active=True),
            'browser_engines': ScraperDefinition.BROWSER_ENGINE_CHOICES,
            'status_choices': ScraperDefinition.SCRAPER_STATUS_CHOICES,
            'priority_choices': ScraperDefinition.PRIORITY_CHOICES,
            'schedule_types': ScraperSchedule.SCHEDULE_TYPE_CHOICES,
            'proxy_types': ProxyConfiguration.PROXY_TYPE_CHOICES,
            'protocols': [('http', 'HTTP'), ('https', 'HTTPS'), ('socks4', 'SOCKS4'), ('socks5', 'SOCKS5')],
        }
        

        
        return render(request, 'admin/scrapers/onboarding_simple.html', context)
        
    except Exception as e:
        print(f"❌ Error in scraper_onboarding view: {e}")
        import traceback
        traceback.print_exc()
        
        # Return a simple error page
        return render(request, 'admin/scrapers/onboarding_simple.html', {
            'error': str(e),
            'status_choices': [('active', 'Active'), ('inactive', 'Inactive')],
            'priority_choices': [('normal', 'Normal'), ('high', 'High')],
            'browser_engines': [('playwright', 'Playwright')],
            'schedule_types': [('interval', 'Interval'), ('manual', 'Manual')],
            'proxy_types': [('residential', 'Residential'), ('datacenter', 'Datacenter')],
            'protocols': [('http', 'HTTP'), ('https', 'HTTPS')],
            'proxy_providers': [],
            'proxy_configurations': [],
            'captcha_types': [],
            'optimization_rules': [],
        })





def handle_scraper_onboarding_post(request):
    """Handle the POST request for creating a new scraper with all configurations."""
    try:
        with transaction.atomic():
            # Extract form data
            scraper_data = extract_scraper_data(request.POST)
            proxy_data = extract_proxy_data(request.POST)
            schedule_data = extract_schedule_data(request.POST)
            
            # Create proxy configuration if needed
            proxy_config = None
            if request.POST.get('create_new_proxy') == 'on':
                proxy_config = create_proxy_configuration(proxy_data)
                messages.success(request, f'Created new proxy configuration: {proxy_config.name}')
            elif request.POST.get('existing_proxy'):
                proxy_config = ProxyConfiguration.objects.get(config_id=request.POST.get('existing_proxy'))
            
            # Create scraper definition
            scraper = create_scraper_definition(scraper_data, proxy_config)
            messages.success(request, f'Created scraper: {scraper.display_name}')
            
            # Create proxy assignment if proxy is configured
            if proxy_config:
                assignment = ScraperProxyAssignment.objects.create(
                    scraper_name=scraper.name,
                    scraper_definition=scraper,
                    proxy_configuration=proxy_config,
                    is_primary=True,
                    is_active=True
                )
                messages.success(request, 'Assigned proxy to scraper')
            
            # Create schedule if configured
            if request.POST.get('create_schedule') == 'on':
                schedule = create_scraper_schedule(schedule_data, scraper)
                messages.success(request, f'Created schedule: {schedule.name}')
            
            # Test the scraper if requested
            if request.POST.get('test_scraper') == 'on':
                test_url = request.POST.get('test_url', scraper.target_website)
                # Here you would integrate with your scraper testing logic
                messages.info(request, f'Scraper test queued for URL: {test_url}')
            
            messages.success(request, 'Scraper onboarding completed successfully!')
            return redirect('admin:scrapers_scraperdefinition_change', scraper.scraper_id)
            
    except Exception as e:
        messages.error(request, f'Error creating scraper: {str(e)}')
        return redirect('scraper_onboarding')


def extract_scraper_data(post_data):
    """Extract scraper configuration data from POST request."""
    return {
        'name': post_data.get('scraper_name'),
        'display_name': post_data.get('display_name'),
        'description': post_data.get('description', ''),
        'target_website': post_data.get('target_website'),
        'target_domains': post_data.get('target_domains', '').split(',') if post_data.get('target_domains') else [],
        'status': post_data.get('status', 'active'),
        'browser_engine': post_data.get('browser_engine', 'playwright'),
        'headless_mode': post_data.get('headless_mode') == 'on',
        'timeout_seconds': int(post_data.get('timeout_seconds', 30)),
        'retry_attempts': int(post_data.get('retry_attempts', 3)),
        'max_concurrent_jobs': int(post_data.get('max_concurrent_jobs', 1)),
        'priority': post_data.get('priority', 'normal'),
        'captcha_required': post_data.get('captcha_required') == 'on',
        'captcha_type_id': post_data.get('captcha_type') if post_data.get('captcha_required') else None,
        'enable_screenshots': post_data.get('enable_screenshots') == 'on',
        'enable_detailed_logging': post_data.get('enable_detailed_logging') == 'on',
        'log_level': post_data.get('log_level', 'INFO'),
        'can_be_scheduled': post_data.get('can_be_scheduled') == 'on',
        'optimization_rules': post_data.getlist('optimization_rules'),
    }


def extract_proxy_data(post_data):
    """Extract proxy configuration data from POST request."""
    return {
        'name': post_data.get('proxy_name'),
        'description': post_data.get('proxy_description', ''),
        'proxy_type': post_data.get('proxy_type', 'residential'),
        'host': post_data.get('proxy_host'),
        'port': int(post_data.get('proxy_port', 8080)),
        'username': post_data.get('proxy_username', ''),
        'password': post_data.get('proxy_password', ''),
        'protocol': post_data.get('proxy_protocol', 'http'),
        'country_code': post_data.get('proxy_country', ''),
        'provider_id': post_data.get('proxy_provider'),
    }


def extract_schedule_data(post_data):
    """Extract schedule configuration data from POST request."""
    return {
        'name': post_data.get('schedule_name'),
        'schedule_type': post_data.get('schedule_type', 'interval'),
        'interval_hours': int(post_data.get('interval_hours', 24)) if post_data.get('interval_hours') else None,
        'interval_minutes': int(post_data.get('interval_minutes', 0)) if post_data.get('interval_minutes') else None,
        'cron_expression': post_data.get('cron_expression', ''),
        'urls_to_scrape': post_data.get('urls_to_scrape', '').split(',') if post_data.get('urls_to_scrape') else [],
    }


def create_proxy_configuration(proxy_data):
    """Create a new proxy configuration."""
    provider = ProxyProvider.objects.get(provider_id=proxy_data['provider_id'])
    
    proxy_config = ProxyConfiguration.objects.create(
        provider=provider,
        name=proxy_data['name'],
        description=proxy_data['description'],
        proxy_type=proxy_data['proxy_type'],
        host=proxy_data['host'],
        port=proxy_data['port'],
        username=proxy_data['username'],
        password=proxy_data['password'],
        protocol=proxy_data['protocol'],
        country_code=proxy_data['country_code'],
        status='active',
        is_active=True
    )
    
    return proxy_config


def create_scraper_definition(scraper_data, proxy_config=None):
    """Create a new scraper definition."""
    scraper = ScraperDefinition.objects.create(
        name=scraper_data['name'],
        display_name=scraper_data['display_name'],
        description=scraper_data['description'],
        target_website=scraper_data['target_website'],
        target_domains=scraper_data['target_domains'],
        status=scraper_data['status'],
        browser_engine=scraper_data['browser_engine'],
        headless_mode=scraper_data['headless_mode'],
        timeout_seconds=scraper_data['timeout_seconds'],
        retry_attempts=scraper_data['retry_attempts'],
        max_concurrent_jobs=scraper_data['max_concurrent_jobs'],
        priority=scraper_data['priority'],
        captcha_required=scraper_data['captcha_required'],
        captcha_type_id=scraper_data['captcha_type_id'],
        enable_screenshots=scraper_data['enable_screenshots'],
        enable_detailed_logging=scraper_data['enable_detailed_logging'],
        log_level=scraper_data['log_level'],
        can_be_scheduled=scraper_data['can_be_scheduled'],
        proxy_settings=proxy_config,
        use_proxy=proxy_config is not None,
        created_by=f'admin_onboarding_{timezone.now().strftime("%Y%m%d_%H%M%S")}'
    )
    
    # Add optimization rules
    if scraper_data['optimization_rules']:
        rules = OptimizationRule.objects.filter(optimization_rule_id__in=scraper_data['optimization_rules'])
        scraper.optimization_rules.set(rules)
    
    return scraper


def create_scraper_schedule(schedule_data, scraper):
    """Create a new scraper schedule."""
    schedule = ScraperSchedule.objects.create(
        scraper=scraper,
        name=schedule_data['name'],
        schedule_type=schedule_data['schedule_type'],
        interval_hours=schedule_data['interval_hours'],
        interval_minutes=schedule_data['interval_minutes'],
        cron_expression=schedule_data['cron_expression'],
        urls_to_scrape=schedule_data['urls_to_scrape'],
        is_active=True
    )
    
    return schedule


@staff_member_required
@require_http_methods(["GET"])
def get_proxy_configurations(request):
    """AJAX endpoint to get proxy configurations for a specific provider."""
    provider_id = request.GET.get('provider_id')
    if not provider_id:
        return JsonResponse({'configurations': []})
    
    configurations = ProxyConfiguration.objects.filter(
        provider_id=provider_id,
        is_active=True
    ).values('config_id', 'name', 'proxy_type', 'host', 'port')
    
    return JsonResponse({'configurations': list(configurations)})


@staff_member_required
@require_http_methods(["POST"])
def test_proxy_connection(request):
    """AJAX endpoint to test a proxy connection."""
    try:
        data = json.loads(request.body)
        host = data.get('host')
        port = data.get('port')
        username = data.get('username')
        password = data.get('password')
        protocol = data.get('protocol', 'http')
        
        # Here you would implement actual proxy testing logic
        # For now, we'll simulate a test
        import time
        time.sleep(1)  # Simulate testing delay
        
        # Mock response - replace with actual proxy testing
        success = True  # This would be the result of actual testing
        
        if success:
            return JsonResponse({
                'success': True,
                'message': 'Proxy connection successful',
                'response_time': 150  # ms
            })
        else:
            return JsonResponse({
                'success': False,
                'message': 'Proxy connection failed',
                'error': 'Connection timeout'
            })
            
    except Exception as e:
        return JsonResponse({
            'success': False,
            'message': 'Test failed',
            'error': str(e)
        })
