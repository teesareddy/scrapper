from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.db import connection
from .models import PerformanceScrapeData
from .tasks import scrape_performance
from celery.result import AsyncResult
import json
import redis
from django.conf import settings


def dashboard_view(request):
    """
    Main dashboard view to monitor scraping tasks
    """
    recent_events = PerformanceScrapeData.objects.select_related().order_by('-created_at')[:20]
    return render(request, 'consumer/dashboard.html', {
        'recent_events': recent_events
    })


@require_http_methods(["POST"])
def submit_scrape_task(request):
    """
    API endpoint to submit a new scraping task
    """
    try:
        data = json.loads(request.body)
        url = data.get('url')

        if not url:
            return JsonResponse({'error': 'URL is required'}, status=400)

        # Submit task to Celery
        task = scrape_performance.delay(url, "manual_task")

        return JsonResponse({
            'task_id': task.id,
            'status': 'submitted',
            'url': url
        })

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def check_task_status(request, task_id):
    """
    Check the status of a Celery task
    """
    try:
        task = AsyncResult(task_id)

        response = {
            'task_id': task_id,
            'status': task.status,
            'ready': task.ready()
        }

        if task.ready():
            if task.successful():
                response['result'] = task.result
            elif task.failed():
                response['error'] = str(task.result)

        return JsonResponse(response)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)




# @require_http_methods(["POST"])
# def submit_batch_process(request):
#     """
#     API endpoint to submit a batch processing task
#     """
#     try:
#         data = json.loads(request.body)
#         batch_data = data.get('batch_data', [])
#
#         if not batch_data:
#             return JsonResponse({'error': 'Batch data is required'}, status=400)
#
#         # Submit batch processing task to Celery
#         from .tasks import process_batch_scraping
#         task = process_batch_scraping.delay(batch_data)
#
#         return JsonResponse({
#             'task_id': task.id,
#             'status': 'submitted',
#             'batch_size': len(batch_data)
#         })
#
#     except Exception as e:
#         return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def latest_scrapes(request):
    """
    Get latest scrape results
    """
    limit = int(request.GET.get('limit', 10))
    scrapes = PerformanceScrapeData.objects.filter(
        pattern__in=['performance_scrape_success', 'performance_scrape_error']
    ).order_by('-created_at')[:limit]

    results = []
    for scrape in scrapes:
        data = scrape.get_data_json()
        results.append({
            'id': scrape.id,
            'pattern': scrape.pattern,
            'created_at': scrape.created_at.isoformat(),
            'url': data.get('url'),
            'status': data.get('status'),
            'error': data.get('error'),
            'scrape_job_id': data.get('scrape_job_id')
        })

    return JsonResponse({'scrapes': results})


@require_http_methods(["GET"])
def health_check(request):
    """
    Health check endpoint for monitoring
    """
    health_status = {
        'status': 'healthy',
        'timestamp': None,
        'services': {
            'database': False,
            'redis': False,
            'celery': False
        }
    }
    
    from datetime import datetime
    health_status['timestamp'] = datetime.now().isoformat()
    
    # Check database connection
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            health_status['services']['database'] = True
    except Exception as e:
        health_status['services']['database'] = f"Error: {str(e)}"
    
    # Check Redis connection
    try:
        r = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            db=settings.REDIS_DB,
            password=settings.REDIS_PASSWORD
        )
        r.ping()
        health_status['services']['redis'] = True
    except Exception as e:
        health_status['services']['redis'] = f"Error: {str(e)}"
    
    # Check Celery workers
    try:
        from celery import current_app
        inspect = current_app.control.inspect()
        stats = inspect.stats()
        if stats:
            health_status['services']['celery'] = True
            health_status['celery_workers'] = len(stats)
        else:
            health_status['services']['celery'] = "No workers available"
    except Exception as e:
        health_status['services']['celery'] = f"Error: {str(e)}"
    
    # Determine overall status
    all_healthy = all(
        status is True for status in health_status['services'].values()
    )
    
    if not all_healthy:
        health_status['status'] = 'unhealthy'
        return JsonResponse(health_status, status=503)
    
    return JsonResponse(health_status)