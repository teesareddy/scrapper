"""
Seat Pack Performance Monitoring Service

This service provides monitoring, health checks, and optimization tools
for the four-dimensional seat pack system.
"""

import time
import logging
from typing import Dict, List, Any, Optional, Tuple
from decimal import Decimal
from django.db import connection
from django.utils import timezone
from datetime import timedelta
from django.core.cache import cache

from ..models.seat_packs import SeatPack
from ..models.pos import FailedRollback
from ..models.base import Performance

logger = logging.getLogger(__name__)


class SeatPackPerformanceMonitor:
    """Monitor and optimize seat pack query performance."""
    
    def __init__(self):
        self.cache_timeout = 300  # 5 minutes
        
    def get_system_health_metrics(self) -> Dict[str, Any]:
        """Get comprehensive system health metrics."""
        cache_key = 'seat_pack_health_metrics'
        cached_metrics = cache.get(cache_key)
        
        if cached_metrics:
            return cached_metrics
        
        try:
            metrics = {
                'timestamp': timezone.now().isoformat(),
                'pack_counts': self._get_pack_counts(),
                'pos_sync_health': self._get_pos_sync_health(),
                'concurrency_health': self._get_concurrency_health(),
                'performance_metrics': self._get_performance_metrics(),
                'rollback_health': self._get_rollback_health(),
                'audit_metrics': self._get_audit_metrics(),
            }
            
            # Cache for 5 minutes
            cache.set(cache_key, metrics, self.cache_timeout)
            return metrics
            
        except Exception as e:
            logger.error(f"Error collecting health metrics: {e}")
            return {
                'timestamp': timezone.now().isoformat(),
                'error': str(e),
                'status': 'unhealthy'
            }
    
    def _get_pack_counts(self) -> Dict[str, int]:
        """Get basic pack count metrics."""
        return {
            'total_packs': SeatPack.objects.count(),
            'active_packs': SeatPack.objects.filter(pack_status='active').count(),
            'inactive_packs': SeatPack.objects.filter(pack_status='inactive').count(),
            'pending_packs': SeatPack.objects.filter(pos_status='pending').count(),
            'failed_packs': SeatPack.objects.filter(pos_status='failed').count(),
        }
    
    def _get_pos_sync_health(self) -> Dict[str, int]:
        """Get POS sync health metrics."""
        return {
            'unsynced_packs': SeatPack.objects.filter(synced_to_pos=False).count(),
            'pending_creation': SeatPack.objects.pending_pos_creation().count(),
            'pending_delisting': SeatPack.objects.pending_pos_delisting().count(),
            'failed_syncs': SeatPack.objects.filter(pos_status='failed').count(),
            'high_retry_packs': SeatPack.objects.filter(pos_sync_attempts__gte=3).count(),
            'recent_failures': SeatPack.objects.filter(
                pos_status='failed',
                last_pos_sync_attempt__gte=timezone.now() - timedelta(hours=1)
            ).count(),
        }
    
    def _get_concurrency_health(self) -> Dict[str, int]:
        """Get concurrency control health metrics."""
        return {
            'active_locks': SeatPack.objects.filter(locked_by__isnull=False).count(),
            'stale_locks': SeatPack.objects.stale_locks().count(),
            'pending_operations': SeatPack.objects.pending_rollbacks().count(),
            'failed_operations': SeatPack.objects.failed_operations().count(),
        }
    
    def _get_performance_metrics(self) -> Dict[str, Any]:
        """Get query performance metrics."""
        try:
            # Benchmark common queries
            benchmarks = self._benchmark_common_queries()
            
            # Get database statistics
            db_stats = self._get_database_statistics()
            
            return {
                'query_benchmarks': benchmarks,
                'database_stats': db_stats,
                'slow_query_threshold_ms': 1000,  # 1 second
            }
        except Exception as e:
            logger.error(f"Error getting performance metrics: {e}")
            return {'error': str(e)}
    
    def _get_rollback_health(self) -> Dict[str, int]:
        """Get rollback operation health metrics."""
        return {
            'pending_rollbacks': SeatPack.objects.filter(
                pos_operation_status='failed'
            ).count(),
            'failed_rollbacks': FailedRollback.objects.filter(
                resolved_at__isnull=True
            ).count(),
            'recent_rollbacks': FailedRollback.objects.filter(
                created_at__gte=timezone.now() - timedelta(hours=24)
            ).count(),
        }
    
    def _get_audit_metrics(self) -> Dict[str, int]:
        """Get audit trail metrics."""
        last_24h = timezone.now() - timedelta(hours=24)
        last_7d = timezone.now() - timedelta(days=7)
        
        return {
            'manual_delists_today': SeatPack.objects.filter(
                delist_reason='manual_delist',
                manually_delisted_at__date=timezone.now().date()
            ).count(),
            'manual_delists_24h': SeatPack.objects.filter(
                delist_reason='manual_delist',
                manually_delisted_at__gte=last_24h
            ).count(),
            'manual_delists_7d': SeatPack.objects.filter(
                delist_reason='manual_delist',
                manually_delisted_at__gte=last_7d
            ).count(),
            'reactivated_packs_24h': SeatPack.objects.filter(
                manually_enabled_at__gte=last_24h
            ).count(),
        }
    
    def _benchmark_common_queries(self) -> Dict[str, float]:
        """Benchmark common query patterns."""
        benchmarks = {}
        
        try:
            # Test POS sync query
            start = time.time()
            list(SeatPack.objects.for_pos_sync()[:100])
            benchmarks['pos_sync_query_ms'] = (time.time() - start) * 1000
            
            # Test dashboard query
            start = time.time()
            list(SeatPack.objects.for_dashboard()[:100])
            benchmarks['dashboard_query_ms'] = (time.time() - start) * 1000
            
            # Test audit query
            start = time.time()
            list(SeatPack.objects.recent_manual_delists()[:100])
            benchmarks['audit_query_ms'] = (time.time() - start) * 1000
            
            # Test retry query
            start = time.time()
            list(SeatPack.objects.needs_retry()[:100])
            benchmarks['retry_query_ms'] = (time.time() - start) * 1000
            
        except Exception as e:
            logger.error(f"Error benchmarking queries: {e}")
            benchmarks['error'] = str(e)
        
        return benchmarks
    
    def _get_database_statistics(self) -> Dict[str, Any]:
        """Get database statistics for seat pack table."""
        try:
            with connection.cursor() as cursor:
                # Get table size
                cursor.execute("""
                    SELECT 
                        pg_size_pretty(pg_total_relation_size('seat_pack')) as table_size,
                        pg_size_pretty(pg_relation_size('seat_pack')) as data_size,
                        pg_size_pretty(pg_total_relation_size('seat_pack') - pg_relation_size('seat_pack')) as index_size
                """)
                size_info = cursor.fetchone()
                
                # Get row count estimate
                cursor.execute("""
                    SELECT reltuples::bigint as estimate
                    FROM pg_class 
                    WHERE relname = 'seat_pack'
                """)
                row_estimate = cursor.fetchone()
                
                return {
                    'table_size': size_info[0] if size_info else 'unknown',
                    'data_size': size_info[1] if size_info else 'unknown',
                    'index_size': size_info[2] if size_info else 'unknown',
                    'estimated_rows': row_estimate[0] if row_estimate else 0,
                }
        except Exception as e:
            logger.error(f"Error getting database statistics: {e}")
            return {'error': str(e)}
    
    def analyze_slow_queries(self) -> List[Dict[str, Any]]:
        """Analyze slow queries and suggest optimizations."""
        try:
            with connection.cursor() as cursor:
                # Check if pg_stat_statements is available
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_extension 
                        WHERE extname = 'pg_stat_statements'
                    )
                """)
                
                if not cursor.fetchone()[0]:
                    return [{'warning': 'pg_stat_statements extension not available'}]
                
                # Get slow queries related to seat_pack
                cursor.execute("""
                    SELECT 
                        query,
                        calls,
                        total_time,
                        mean_time,
                        rows
                    FROM pg_stat_statements
                    WHERE query LIKE '%seat_pack%'
                    AND mean_time > 100
                    ORDER BY mean_time DESC
                    LIMIT 10
                """)
                
                slow_queries = []
                for row in cursor.fetchall():
                    slow_queries.append({
                        'query': row[0][:200] + '...' if len(row[0]) > 200 else row[0],
                        'calls': row[1],
                        'total_time_ms': row[2],
                        'mean_time_ms': row[3],
                        'avg_rows': row[4]
                    })
                
                return slow_queries
                
        except Exception as e:
            logger.error(f"Error analyzing slow queries: {e}")
            return [{'error': str(e)}]
    
    def suggest_optimizations(self) -> List[Dict[str, str]]:
        """Suggest optimization strategies based on current metrics."""
        suggestions = []
        metrics = self.get_system_health_metrics()
        
        try:
            # Check for high unsynced count
            if metrics.get('pos_sync_health', {}).get('unsynced_packs', 0) > 1000:
                suggestions.append({
                    'type': 'pos_sync',
                    'priority': 'high',
                    'suggestion': 'High number of unsynced packs detected. Consider increasing POS sync batch size or frequency.',
                    'action': 'Check POS sync service configuration and StubHub API connectivity'
                })
            
            # Check for stale locks
            if metrics.get('concurrency_health', {}).get('stale_locks', 0) > 10:
                suggestions.append({
                    'type': 'concurrency',
                    'priority': 'medium',
                    'suggestion': 'Stale locks detected. Run lock cleanup maintenance.',
                    'action': 'Execute seat_pack_manager.cleanup_stale_locks()'
                })
            
            # Check query performance
            benchmarks = metrics.get('performance_metrics', {}).get('query_benchmarks', {})
            for query_name, time_ms in benchmarks.items():
                if query_name.endswith('_ms') and time_ms > 1000:
                    suggestions.append({
                        'type': 'performance',
                        'priority': 'medium',
                        'suggestion': f'Slow query detected: {query_name} taking {time_ms:.2f}ms',
                        'action': 'Review query optimization and index usage'
                    })
            
            # Check failed rollbacks
            if metrics.get('rollback_health', {}).get('failed_rollbacks', 0) > 0:
                suggestions.append({
                    'type': 'rollback',
                    'priority': 'high',
                    'suggestion': 'Failed rollbacks requiring manual intervention detected.',
                    'action': 'Review FailedRollback records and resolve manually'
                })
            
        except Exception as e:
            logger.error(f"Error generating suggestions: {e}")
            suggestions.append({
                'type': 'error',
                'priority': 'high',
                'suggestion': f'Error generating optimization suggestions: {e}',
                'action': 'Check system logs for detailed error information'
            })
        
        return suggestions
    
    def generate_health_report(self) -> Dict[str, Any]:
        """Generate a comprehensive health report."""
        metrics = self.get_system_health_metrics()
        suggestions = self.suggest_optimizations()
        slow_queries = self.analyze_slow_queries()
        
        # Determine overall health status
        health_status = self._calculate_health_status(metrics)
        
        return {
            'timestamp': timezone.now().isoformat(),
            'health_status': health_status,
            'metrics': metrics,
            'optimizations': suggestions,
            'slow_queries': slow_queries,
            'recommendations': self._generate_recommendations(health_status, suggestions)
        }
    
    def _calculate_health_status(self, metrics: Dict[str, Any]) -> str:
        """Calculate overall health status based on metrics."""
        try:
            pos_health = metrics.get('pos_sync_health', {})
            concurrency_health = metrics.get('concurrency_health', {})
            rollback_health = metrics.get('rollback_health', {})
            
            # Critical issues
            if rollback_health.get('failed_rollbacks', 0) > 0:
                return 'critical'
            
            if concurrency_health.get('stale_locks', 0) > 50:
                return 'critical'
            
            # Warning issues
            if pos_health.get('unsynced_packs', 0) > 1000:
                return 'warning'
            
            if pos_health.get('failed_syncs', 0) > 100:
                return 'warning'
            
            if concurrency_health.get('stale_locks', 0) > 10:
                return 'warning'
            
            # All good
            return 'healthy'
            
        except Exception as e:
            logger.error(f"Error calculating health status: {e}")
            return 'unknown'
    
    def _generate_recommendations(self, health_status: str, suggestions: List[Dict]) -> List[str]:
        """Generate high-level recommendations based on health status."""
        recommendations = []
        
        if health_status == 'critical':
            recommendations.append("CRITICAL: Immediate attention required for system stability")
            recommendations.append("Review failed rollbacks and resolve manually")
            recommendations.append("Check for deadlock or locking issues")
        
        elif health_status == 'warning':
            recommendations.append("WARNING: System performance may be degraded")
            recommendations.append("Monitor POS sync queue and clear backlogs")
            recommendations.append("Run maintenance tasks to clean up stale operations")
        
        elif health_status == 'healthy':
            recommendations.append("System is operating normally")
            recommendations.append("Continue regular monitoring and maintenance")
        
        # Add specific recommendations from suggestions
        high_priority_suggestions = [s for s in suggestions if s.get('priority') == 'high']
        for suggestion in high_priority_suggestions:
            recommendations.append(f"HIGH PRIORITY: {suggestion.get('suggestion', '')}")
        
        return recommendations


def get_seat_pack_health_metrics() -> Dict[str, Any]:
    """Convenience function to get health metrics."""
    monitor = SeatPackPerformanceMonitor()
    return monitor.get_system_health_metrics()


def generate_seat_pack_health_report() -> Dict[str, Any]:
    """Convenience function to generate health report."""
    monitor = SeatPackPerformanceMonitor()
    return monitor.generate_health_report()


def cleanup_seat_pack_system() -> Dict[str, int]:
    """Convenience function to run system cleanup."""
    from .seat_pack_manager import SeatPackManager
    
    manager = SeatPackManager()
    
    # Clean up stale locks
    stale_locks = manager.cleanup_stale_locks()
    
    # Clean up stale operations
    from ..models.seat_packs import SeatPack
    stale_operations = SeatPack.objects.filter(
        pos_operation_status='started',
        last_pos_sync_attempt__lt=timezone.now() - timedelta(hours=1)
    ).update(
        pos_operation_status='failed',
        pos_sync_error='Operation timed out',
        pos_operation_id=None
    )
    
    logger.info(f"System cleanup completed: {stale_locks} stale locks, {stale_operations} stale operations")
    
    return {
        'stale_locks_cleaned': stale_locks,
        'stale_operations_cleaned': stale_operations
    }


def optimize_seat_pack_table() -> Dict[str, Any]:
    """Perform database optimization on seat pack table."""
    try:
        with connection.cursor() as cursor:
            # Update table statistics
            cursor.execute("ANALYZE seat_pack;")
            
            # Get table statistics after analyze
            cursor.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    attname,
                    n_distinct,
                    correlation
                FROM pg_stats 
                WHERE tablename = 'seat_pack'
                ORDER BY n_distinct DESC
                LIMIT 10
            """)
            
            stats = cursor.fetchall()
            
            # Optional: Clean up old records (with caution)
            old_threshold = timezone.now() - timedelta(days=90)
            old_count = SeatPack.objects.filter(
                pack_status='inactive',
                pack_state='transformed',
                updated_at__lt=old_threshold
            ).count()
            
            return {
                'success': True,
                'table_analyzed': True,
                'statistics_updated': len(stats),
                'old_records_found': old_count,
                'optimization_suggestions': [
                    'Table statistics have been updated',
                    'Consider archiving transformed packs older than 90 days',
                    'Monitor query performance after optimization'
                ]
            }
            
    except Exception as e:
        logger.error(f"Error optimizing seat pack table: {e}")
        return {
            'success': False,
            'error': str(e)
        }