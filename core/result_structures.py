from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import time


class ScrapingStatus(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL_SUCCESS = "partial_success"
    TIMEOUT = "timeout"
    BLOCKED = "blocked"
    RATE_LIMITED = "rate_limited"


class ErrorCategory(Enum):
    NETWORK = "network"
    PARSING = "parsing"
    VALIDATION = "validation"
    AUTHENTICATION = "authentication"
    RATE_LIMIT = "rate_limit"
    BLOCKED = "blocked"
    TIMEOUT = "timeout"
    UNKNOWN = "unknown"


@dataclass
class ScrapingError:
    category: ErrorCategory
    message: str
    details: Optional[str] = None
    retry_after: Optional[int] = None
    fatal: bool = False
    
    def __post_init__(self):
        if not self.message or not self.message.strip():
            self.message = f"Unknown {self.category.value} error"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category.value,
            "message": self.message,
            "details": self.details,
            "retry_after": self.retry_after,
            "fatal": self.fatal
        }


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def add_error(self, error: str):
        self.errors.append(error)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        self.warnings.append(warning)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "errors": self.errors,
            "warnings": self.warnings
        }


@dataclass
class PerformanceMetrics:
    start_time: float
    end_time: Optional[float] = None
    total_duration: Optional[float] = None
    network_calls: int = 0
    bytes_downloaded: int = 0
    pages_scraped: int = 0
    items_extracted: int = 0
    cache_hits: int = 0
    optimization_stats: Optional[Dict[str, Any]] = None
    
    def mark_complete(self):
        self.end_time = time.time()
        self.total_duration = self.end_time - self.start_time
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_time": self.start_time,
            "end_time": self.end_time,
            "total_duration": self.total_duration,
            "network_calls": self.network_calls,
            "bytes_downloaded": self.bytes_downloaded,
            "pages_scraped": self.pages_scraped,
            "items_extracted": self.items_extracted,
            "cache_hits": self.cache_hits,
            "optimization_stats": self.optimization_stats
        }


@dataclass
class ScrapingResult:
    scraper_name: str
    url: str
    status: ScrapingStatus
    data: Dict[str, Any] = field(default_factory=dict)
    errors: List[ScrapingError] = field(default_factory=list)
    validation: Optional[ValidationResult] = None
    performance: Optional[PerformanceMetrics] = None
    scraped_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')
    scrape_job_id: Optional[str] = None
    database_key: Optional[str] = None
    
    @property
    def success(self) -> bool:
        return self.status == ScrapingStatus.SUCCESS
    
    @property
    def has_data(self) -> bool:
        return bool(self.data)
    
    @property
    def is_retryable(self) -> bool:
        if not self.errors:
            return False
        return not any(error.fatal for error in self.errors)
    
    def add_error(self, category: ErrorCategory, message: str, 
                  details: Optional[str] = None, retry_after: Optional[int] = None, 
                  fatal: bool = False):
        # Ensure message is not empty
        if not message or not message.strip():
            message = f"Unknown {category.value} error"
        
        error = ScrapingError(
            category=category,
            message=message.strip(),
            details=details,
            retry_after=retry_after,
            fatal=fatal
        )
        self.errors.append(error)
        
        if self.status == ScrapingStatus.SUCCESS:
            self.status = ScrapingStatus.PARTIAL_SUCCESS if self.data else ScrapingStatus.FAILED
    
    def set_validation_result(self, validation: ValidationResult):
        self.validation = validation
        if not validation.is_valid and self.status == ScrapingStatus.SUCCESS:
            self.status = ScrapingStatus.PARTIAL_SUCCESS
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "scraper_name": self.scraper_name,
            "url": self.url,
            "status": self.status.value,
            "success": self.success,
            "scraped_at": self.scraped_at,
            "data": self.data,
            "has_data": self.has_data,
            "is_retryable": self.is_retryable
        }
        
        if self.scrape_job_id:
            result["scrape_job_id"] = self.scrape_job_id
        
        if self.database_key:
            result["database_key"] = self.database_key
        
        if self.errors:
            result["errors"] = [error.to_dict() for error in self.errors]
        
        if self.validation:
            result["validation"] = self.validation.to_dict()
        
        if self.performance:
            result["performance"] = self.performance.to_dict()
        
        return result


class ResultBuilder:
    def __init__(self, scraper_name: str, url: str, scrape_job_id: Optional[str] = None):
        self.result = ScrapingResult(
            scraper_name=scraper_name,
            url=url,
            status=ScrapingStatus.SUCCESS,
            scrape_job_id=scrape_job_id
        )
        self.performance_metrics = PerformanceMetrics(start_time=time.time())
    
    def with_data(self, data: Dict[str, Any]) -> 'ResultBuilder':
        self.result.data = data
        return self
    
    def with_error(self, category: ErrorCategory, message: str, 
                   details: Optional[str] = None, retry_after: Optional[int] = None, 
                   fatal: bool = False) -> 'ResultBuilder':
        self.result.add_error(category, message, details, retry_after, fatal)
        return self
    
    def with_network_error(self, message: str, details: Optional[str] = None) -> 'ResultBuilder':
        return self.with_error(ErrorCategory.NETWORK, message, details, retry_after=30)
    
    def with_parsing_error(self, message: str, details: Optional[str] = None) -> 'ResultBuilder':
        return self.with_error(ErrorCategory.PARSING, message, details, fatal=True)
    
    def with_timeout_error(self, message: str = "Operation timed out") -> 'ResultBuilder':
        self.result.status = ScrapingStatus.TIMEOUT
        return self.with_error(ErrorCategory.TIMEOUT, message, retry_after=60)
    
    def with_rate_limit_error(self, message: str = "Rate limit exceeded", 
                             retry_after: int = 300) -> 'ResultBuilder':
        self.result.status = ScrapingStatus.RATE_LIMITED
        return self.with_error(ErrorCategory.RATE_LIMIT, message, retry_after=retry_after)
    
    def with_blocked_error(self, message: str = "Request blocked") -> 'ResultBuilder':
        self.result.status = ScrapingStatus.BLOCKED
        return self.with_error(ErrorCategory.BLOCKED, message, retry_after=600)
    
    def with_validation(self, validation: ValidationResult) -> 'ResultBuilder':
        self.result.set_validation_result(validation)
        return self
    
    def with_database_key(self, key: str) -> 'ResultBuilder':
        self.result.database_key = key
        return self
    
    def record_network_call(self, bytes_downloaded: int = 0) -> 'ResultBuilder':
        self.performance_metrics.network_calls += 1
        self.performance_metrics.bytes_downloaded += bytes_downloaded
        return self
    
    def record_page_scraped(self) -> 'ResultBuilder':
        self.performance_metrics.pages_scraped += 1
        return self
    
    def record_items_extracted(self, count: int) -> 'ResultBuilder':
        self.performance_metrics.items_extracted += count
        return self
    
    def record_cache_hit(self) -> 'ResultBuilder':
        self.performance_metrics.cache_hits += 1
        return self
    
    def with_optimization_stats(self, stats: Dict[str, Any]) -> 'ResultBuilder':
        self.performance_metrics.optimization_stats = stats
        return self
    
    def build(self) -> ScrapingResult:
        self.performance_metrics.mark_complete()
        self.result.performance = self.performance_metrics
        return self.result


def create_success_result(scraper_name: str, url: str, data: Dict[str, Any], 
                         scrape_job_id: Optional[str] = None, 
                         database_key: Optional[str] = None) -> ScrapingResult:
    builder = ResultBuilder(scraper_name, url, scrape_job_id).with_data(data)
    if database_key:
        builder = builder.with_database_key(database_key)
    return builder.build()


def create_error_result(scraper_name: str, url: str, error_category: ErrorCategory, 
                       error_message: str, scrape_job_id: Optional[str] = None,
                       details: Optional[str] = None, fatal: bool = False) -> ScrapingResult:
    return (ResultBuilder(scraper_name, url, scrape_job_id)
            .with_error(error_category, error_message, details, fatal=fatal)
            .build())


def create_timeout_result(scraper_name: str, url: str, 
                         scrape_job_id: Optional[str] = None) -> ScrapingResult:
    return (ResultBuilder(scraper_name, url, scrape_job_id)
            .with_timeout_error()
            .build())


def create_rate_limited_result(scraper_name: str, url: str,
                              retry_after: int = 300,
                              scrape_job_id: Optional[str] = None) -> ScrapingResult:
    return (ResultBuilder(scraper_name, url, scrape_job_id)
            .with_rate_limit_error(retry_after=retry_after)
            .build())