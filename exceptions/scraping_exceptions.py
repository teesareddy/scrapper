from typing import Optional


class ScrapingException(Exception):
    def __init__(self, message: str, details: Optional[str] = None, 
                 retry_after: Optional[int] = None, fatal: bool = False):
        super().__init__(message)
        self.message = message
        self.details = details
        self.retry_after = retry_after
        self.fatal = fatal


class NetworkException(ScrapingException):
    def __init__(self, message: str, details: Optional[str] = None, 
                 retry_after: int = 30):
        super().__init__(message, details, retry_after, fatal=False)


class ParseException(ScrapingException):
    def __init__(self, message: str, details: Optional[str] = None):
        super().__init__(message, details, fatal=True)


class ValidationException(ScrapingException):
    def __init__(self, message: str, details: Optional[str] = None):
        super().__init__(message, details, fatal=True)


class TimeoutException(ScrapingException):
    def __init__(self, message: str = "Operation timed out", 
                 details: Optional[str] = None, retry_after: int = 60):
        super().__init__(message, details, retry_after, fatal=False)


class RateLimitException(ScrapingException):
    def __init__(self, message: str = "Rate limit exceeded", 
                 retry_after: int = 300, details: Optional[str] = None):
        super().__init__(message, details, retry_after, fatal=False)


class BlockedException(ScrapingException):
    def __init__(self, message: str = "Request blocked", 
                 details: Optional[str] = None, retry_after: int = 600):
        super().__init__(message, details, retry_after, fatal=False)


class DataExtractionException(ParseException):
    def __init__(self, element_selector: str, page_url: str, 
                 details: Optional[str] = None):
        message = f"Failed to extract data using selector '{element_selector}' on page '{page_url}'"
        super().__init__(message, details)
        self.element_selector = element_selector
        self.page_url = page_url


class DatabaseStorageException(ScrapingException):
    def __init__(self, message: str, details: Optional[str] = None):
        super().__init__(message, details, fatal=True)


class ConfigurationException(ScrapingException):
    def __init__(self, message: str, details: Optional[str] = None):
        super().__init__(message, details, fatal=True)