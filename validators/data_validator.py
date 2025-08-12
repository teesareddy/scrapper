from typing import Dict, Any, List, Optional, Union, Callable
from datetime import datetime
import re
from abc import ABC, abstractmethod

from ..core.result_structures import ValidationResult


class ValidatorRule(ABC):
    @abstractmethod
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        pass


class RequiredRule(ValidatorRule):
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        if value is None or (isinstance(value, str) and not value.strip()):
            result.add_error(f"Field '{field_name}' is required")
        return result


class TypeRule(ValidatorRule):
    def __init__(self, expected_type: type, allow_none: bool = False):
        self.expected_type = expected_type
        self.allow_none = allow_none
    
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        if value is None and self.allow_none:
            return result
        
        if not isinstance(value, self.expected_type):
            result.add_error(
                f"Field '{field_name}' must be of type {self.expected_type.__name__}, "
                f"got {type(value).__name__}"
            )
        return result


class RangeRule(ValidatorRule):
    def __init__(self, min_val: Optional[Union[int, float]] = None, 
                 max_val: Optional[Union[int, float]] = None):
        self.min_val = min_val
        self.max_val = max_val
    
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        if value is None:
            return result
        
        try:
            num_value = float(value)
            if self.min_val is not None and num_value < self.min_val:
                result.add_error(f"Field '{field_name}' must be >= {self.min_val}")
            if self.max_val is not None and num_value > self.max_val:
                result.add_error(f"Field '{field_name}' must be <= {self.max_val}")
        except (ValueError, TypeError):
            result.add_error(f"Field '{field_name}' must be a numeric value")
        
        return result


class LengthRule(ValidatorRule):
    def __init__(self, min_length: Optional[int] = None, 
                 max_length: Optional[int] = None):
        self.min_length = min_length
        self.max_length = max_length
    
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        if value is None:
            return result
        
        try:
            length = len(value)
            if self.min_length is not None and length < self.min_length:
                result.add_error(f"Field '{field_name}' must have at least {self.min_length} characters")
            if self.max_length is not None and length > self.max_length:
                result.add_error(f"Field '{field_name}' must have at most {self.max_length} characters")
        except TypeError:
            result.add_error(f"Field '{field_name}' must have a length")
        
        return result


class RegexRule(ValidatorRule):
    def __init__(self, pattern: str, message: Optional[str] = None):
        self.pattern = re.compile(pattern)
        self.message = message or f"Field must match pattern: {pattern}"
    
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        if value is None:
            return result
        
        if not isinstance(value, str):
            result.add_error(f"Field '{field_name}' must be a string for regex validation")
            return result
        
        if not self.pattern.match(value):
            result.add_error(f"Field '{field_name}': {self.message}")
        
        return result


class DateTimeRule(ValidatorRule):
    def __init__(self, format_string: str = "%Y-%m-%d", allow_future: bool = True):
        self.format_string = format_string
        self.allow_future = allow_future
    
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        if value is None:
            return result
        
        if isinstance(value, datetime):
            parsed_date = value
        elif isinstance(value, str):
            try:
                parsed_date = datetime.strptime(value, self.format_string)
            except ValueError:
                result.add_error(f"Field '{field_name}' must be a valid date in format {self.format_string}")
                return result
        else:
            result.add_error(f"Field '{field_name}' must be a datetime or string")
            return result
        
        if not self.allow_future and parsed_date > datetime.now():
            result.add_error(f"Field '{field_name}' cannot be in the future")
        
        return result


class URLRule(ValidatorRule):
    def __init__(self, require_https: bool = False):
        self.require_https = require_https
        self.url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        if value is None:
            return result
        
        if not isinstance(value, str):
            result.add_error(f"Field '{field_name}' must be a string")
            return result
        
        if not self.url_pattern.match(value):
            result.add_error(f"Field '{field_name}' must be a valid URL")
            return result
        
        if self.require_https and not value.startswith('https://'):
            result.add_error(f"Field '{field_name}' must use HTTPS")
        
        return result


class EmailRule(ValidatorRule):
    def __init__(self):
        self.email_pattern = re.compile(
            r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
    
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        if value is None:
            return result
        
        if not isinstance(value, str):
            result.add_error(f"Field '{field_name}' must be a string")
            return result
        
        if not self.email_pattern.match(value):
            result.add_error(f"Field '{field_name}' must be a valid email address")
        
        return result


class CustomRule(ValidatorRule):
    def __init__(self, validator_func: Callable[[Any], bool], 
                 error_message: str):
        self.validator_func = validator_func
        self.error_message = error_message
    
    def validate(self, value: Any, field_name: str) -> ValidationResult:
        result = ValidationResult(is_valid=True)
        try:
            if not self.validator_func(value):
                result.add_error(f"Field '{field_name}': {self.error_message}")
        except Exception as e:
            result.add_error(f"Field '{field_name}': Validation error - {str(e)}")
        return result


class DataValidator:
    def __init__(self):
        self.field_rules: Dict[str, List[ValidatorRule]] = {}
    
    def add_rule(self, field_name: str, rule: ValidatorRule):
        if field_name not in self.field_rules:
            self.field_rules[field_name] = []
        self.field_rules[field_name].append(rule)
    
    def add_required(self, field_name: str):
        self.add_rule(field_name, RequiredRule())
    
    def add_type_check(self, field_name: str, expected_type: type, allow_none: bool = False):
        self.add_rule(field_name, TypeRule(expected_type, allow_none))
    
    def add_range(self, field_name: str, min_val: Optional[Union[int, float]] = None, 
                  max_val: Optional[Union[int, float]] = None):
        self.add_rule(field_name, RangeRule(min_val, max_val))
    
    def add_length(self, field_name: str, min_length: Optional[int] = None, 
                   max_length: Optional[int] = None):
        self.add_rule(field_name, LengthRule(min_length, max_length))
    
    def add_regex(self, field_name: str, pattern: str, message: Optional[str] = None):
        self.add_rule(field_name, RegexRule(pattern, message))
    
    def add_datetime(self, field_name: str, format_string: str = "%Y-%m-%d", 
                     allow_future: bool = True):
        self.add_rule(field_name, DateTimeRule(format_string, allow_future))
    
    def add_url(self, field_name: str, require_https: bool = False):
        self.add_rule(field_name, URLRule(require_https))
    
    def add_email(self, field_name: str):
        self.add_rule(field_name, EmailRule())
    
    def add_custom(self, field_name: str, validator_func: Callable[[Any], bool], 
                   error_message: str):
        self.add_rule(field_name, CustomRule(validator_func, error_message))
    
    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        overall_result = ValidationResult(is_valid=True)
        
        for field_name, rules in self.field_rules.items():
            field_value = data.get(field_name)
            
            for rule in rules:
                rule_result = rule.validate(field_value, field_name)
                if not rule_result.is_valid:
                    overall_result.errors.extend(rule_result.errors)
                    overall_result.is_valid = False
                overall_result.warnings.extend(rule_result.warnings)
        
        return overall_result


class VenueDataValidator(DataValidator):
    def __init__(self):
        super().__init__()
        self._setup_common_venue_rules()
    
    def _setup_common_venue_rules(self):
        self.add_required("venue_name")
        self.add_type_check("venue_name", str)
        self.add_length("venue_name", min_length=1, max_length=200)
        
        self.add_required("event_title")
        self.add_type_check("event_title", str)
        self.add_length("event_title", min_length=1, max_length=300)
        
        self.add_type_check("event_url", str, allow_none=True)
        self.add_url("event_url")
        
        self.add_type_check("event_date", str, allow_none=True)
        
        self.add_type_check("price_range", dict, allow_none=True)
        
        self.add_type_check("seat_availability", dict, allow_none=True)


class PerformanceDataValidator(DataValidator):
    def __init__(self):
        super().__init__()
        self._setup_performance_rules()
    
    def _setup_performance_rules(self):
        self.add_required("performance_id")
        self.add_type_check("performance_id", str)
        
        self.add_required("venue_id")
        self.add_type_check("venue_id", str)
        
        self.add_required("title")
        self.add_type_check("title", str)
        self.add_length("title", min_length=1, max_length=300)
        
        self.add_type_check("performance_date", str, allow_none=True)
        
        self.add_type_check("duration_minutes", int, allow_none=True)
        self.add_range("duration_minutes", min_val=1, max_val=600)
        
        self.add_type_check("ticket_prices", list, allow_none=True)
        
        self.add_type_check("seating_chart", dict, allow_none=True)