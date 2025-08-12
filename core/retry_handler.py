import asyncio
import random
from typing import Callable, Any, Optional, List
from dataclasses import dataclass
from enum import Enum
import time

from ..exceptions.scraping_exceptions import (
    ScrapingException, NetworkException, TimeoutException, 
    RateLimitException, BlockedException
)


class BackoffStrategy(Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_JITTER = "exponential_jitter"


@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 300.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL_JITTER
    retry_on_exceptions: List[type] = None
    stop_on_exceptions: List[type] = None
    
    def __post_init__(self):
        if self.retry_on_exceptions is None:
            self.retry_on_exceptions = [
                NetworkException,
                TimeoutException,
                RateLimitException,
                BlockedException
            ]
        
        if self.stop_on_exceptions is None:
            self.stop_on_exceptions = []


class RetryHandler:
    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
    
    def should_retry(self, exception: Exception, attempt: int) -> bool:
        if attempt >= self.config.max_retries:
            return False
        
        if any(isinstance(exception, exc_type) for exc_type in self.config.stop_on_exceptions):
            return False
        
        if isinstance(exception, ScrapingException) and exception.fatal:
            return False
        
        return any(isinstance(exception, exc_type) for exc_type in self.config.retry_on_exceptions)
    
    def calculate_delay(self, attempt: int, exception: Exception = None) -> float:
        if isinstance(exception, ScrapingException) and exception.retry_after:
            base_delay = exception.retry_after
        else:
            base_delay = self.config.base_delay
        
        if self.config.backoff_strategy == BackoffStrategy.FIXED:
            delay = base_delay
        elif self.config.backoff_strategy == BackoffStrategy.LINEAR:
            delay = base_delay * attempt
        elif self.config.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = base_delay * (2 ** (attempt - 1))
        elif self.config.backoff_strategy == BackoffStrategy.EXPONENTIAL_JITTER:
            base_exponential = base_delay * (2 ** (attempt - 1))
            jitter = random.uniform(0, base_exponential * 0.1)
            delay = base_exponential + jitter
        else:
            delay = base_delay
        
        return min(delay, self.config.max_delay)
    
    async def execute_with_retry(self, func: Callable, *args, **kwargs) -> Any:
        last_exception = None
        
        for attempt in range(1, self.config.max_retries + 2):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            
            except Exception as e:
                last_exception = e
                
                if not self.should_retry(e, attempt):
                    raise e
                
                delay = self.calculate_delay(attempt, e)
                await asyncio.sleep(delay)
        
        if last_exception:
            raise last_exception


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = "closed"
    
    def can_execute(self) -> bool:
        if self.state == "closed":
            return True
        elif self.state == "open":
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = "half-open"
                return True
            return False
        elif self.state == "half-open":
            return True
        return False
    
    def record_success(self):
        if self.state == "half-open":
            self.state = "closed"
            self.failure_count = 0
    
    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        if not self.can_execute():
            raise Exception("Circuit breaker is open")
        
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            self.record_success()
            return result
        
        except Exception as e:
            self.record_failure()
            raise e


class EnhancedRetryHandler:
    def __init__(self, retry_config: RetryConfig = None, 
                 circuit_breaker: CircuitBreaker = None):
        self.retry_handler = RetryHandler(retry_config)
        self.circuit_breaker = circuit_breaker
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        if self.circuit_breaker:
            return await self.circuit_breaker.execute(
                self.retry_handler.execute_with_retry, func, *args, **kwargs
            )
        else:
            return await self.retry_handler.execute_with_retry(func, *args, **kwargs)