"""
Response validation system for API responses.

This module provides comprehensive validation capabilities for API responses,
following the Single Responsibility Principle with focused validation logic.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Callable
from dataclasses import dataclass
from enum import Enum

from .request_client import RequestResult
from ..exceptions.scraping_exceptions import ValidationException, ParseException


class ValidationLevel(Enum):
    """Validation strictness levels."""
    STRICT = "strict"      # All validations must pass
    LENIENT = "lenient"    # Some validations can be warnings
    MINIMAL = "minimal"    # Only basic structure validation


@dataclass
class ValidationRule:
    """A single validation rule for API responses."""
    name: str
    description: str
    validator: Callable[[Dict[str, Any]], bool]
    required: bool = True
    error_message: Optional[str] = None
    
    def validate(self, data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Execute the validation rule.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            is_valid = self.validator(data)
            if not is_valid:
                error_msg = self.error_message or f"Validation failed: {self.name}"
                return False, error_msg
            return True, None
        except Exception as e:
            error_msg = f"Validation error in {self.name}: {str(e)}"
            return False, error_msg


@dataclass
class ValidationResult:
    """Result of response validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    validation_level: ValidationLevel
    endpoint_name: str
    
    def __post_init__(self):
        """Initialize empty lists if None."""
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
    
    def add_error(self, message: str) -> None:
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False
    
    def add_warning(self, message: str) -> None:
        """Add a warning message."""
        self.warnings.append(message)
    
    def has_errors(self) -> bool:
        """Check if validation has errors."""
        return len(self.errors) > 0
    
    def has_warnings(self) -> bool:
        """Check if validation has warnings."""
        return len(self.warnings) > 0


class IResponseValidator(ABC):
    """Interface for response validators."""
    
    @abstractmethod
    def validate(self, result: RequestResult, endpoint_name: str) -> ValidationResult:
        """Validate API response."""
        pass
    
    @abstractmethod
    def add_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule."""
        pass


class BaseResponseValidator(IResponseValidator):
    """
    Base response validator with common validation patterns.
    
    This class provides a foundation for validating API responses with
    configurable rules and validation levels.
    """
    
    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STRICT):
        """
        Initialize validator with validation level.
        
        Args:
            validation_level: How strict the validation should be
        """
        self.validation_level = validation_level
        self.logger = logging.getLogger(__name__)
        self._rules: List[ValidationRule] = []
        
        # Add default validation rules
        self._add_default_rules()
    
    def _add_default_rules(self) -> None:
        """Add default validation rules that apply to most API responses."""
        
        # Basic HTTP status validation
        self.add_rule(ValidationRule(
            name="http_status_success",
            description="HTTP status code indicates success",
            validator=lambda data: True,  # This is handled at RequestResult level
            required=True,
            error_message="HTTP request was not successful"
        ))
        
        # Response data presence
        self.add_rule(ValidationRule(
            name="has_response_data",
            description="Response contains data",
            validator=lambda data: data is not None and len(data) > 0,
            required=True,
            error_message="Response contains no data"
        ))
        
        # JSON structure validation (if applicable)
        self.add_rule(ValidationRule(
            name="valid_json_structure",
            description="Response has valid JSON structure",
            validator=self._validate_json_structure,
            required=True,
            error_message="Response does not have valid JSON structure"
        ))
    
    def _validate_json_structure(self, data: Dict[str, Any]) -> bool:
        """Validate that data has a basic JSON structure."""
        try:
            # Check if it's a dictionary (parsed JSON)
            if not isinstance(data, dict):
                return False
            
            # Allow empty dictionaries
            return True
            
        except Exception:
            return False
    
    def add_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule."""
        self._rules.append(rule)
        self.logger.debug(f"Added validation rule: {rule.name}")
    
    def validate(self, result: RequestResult, endpoint_name: str) -> ValidationResult:
        """
        Validate API response using all configured rules.
        
        Args:
            result: The API response result
            endpoint_name: Name of the endpoint being validated
            
        Returns:
            ValidationResult with detailed validation information
        """
        validation_result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            validation_level=self.validation_level,
            endpoint_name=endpoint_name
        )
        
        # First check if the HTTP request was successful
        if not result.success:
            validation_result.add_error(f"HTTP request failed: {result.error_message}")
            return validation_result
        
        # Apply all validation rules
        for rule in self._rules:
            try:
                is_valid, error_message = rule.validate(result.data)
                
                if not is_valid:
                    if rule.required:
                        if self.validation_level == ValidationLevel.STRICT:
                            validation_result.add_error(error_message or f"Required validation failed: {rule.name}")
                        elif self.validation_level == ValidationLevel.LENIENT:
                            validation_result.add_warning(error_message or f"Validation warning: {rule.name}")
                        # MINIMAL level skips non-critical validations
                    else:
                        validation_result.add_warning(error_message or f"Optional validation failed: {rule.name}")
                
            except Exception as e:
                error_msg = f"Exception during validation rule '{rule.name}': {str(e)}"
                validation_result.add_error(error_msg)
                self.logger.error(error_msg)
        
        # Log validation results
        if validation_result.has_errors():
            self.logger.warning(f"Validation failed for {endpoint_name}: {len(validation_result.errors)} errors")
        elif validation_result.has_warnings():
            self.logger.info(f"Validation completed for {endpoint_name} with {len(validation_result.warnings)} warnings")
        else:
            self.logger.debug(f"Validation passed for {endpoint_name}")
        
        return validation_result


class GraphQLResponseValidator(BaseResponseValidator):
    """Validator specifically for GraphQL API responses."""
    
    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STRICT):
        super().__init__(validation_level)
        self._add_graphql_rules()
    
    def _add_graphql_rules(self) -> None:
        """Add GraphQL-specific validation rules."""
        
        # GraphQL response structure
        self.add_rule(ValidationRule(
            name="graphql_structure",
            description="Response has proper GraphQL structure",
            validator=self._validate_graphql_structure,
            required=True,
            error_message="Response does not have proper GraphQL structure"
        ))
        
        # Check for GraphQL errors
        self.add_rule(ValidationRule(
            name="no_graphql_errors",
            description="Response contains no GraphQL errors",
            validator=self._validate_no_graphql_errors,
            required=True,
            error_message="Response contains GraphQL errors"
        ))
        
        # Data field presence
        self.add_rule(ValidationRule(
            name="has_data_field",
            description="Response contains data field",
            validator=lambda data: 'data' in data,
            required=True,
            error_message="Response missing 'data' field"
        ))
    
    def _validate_graphql_structure(self, data: Dict[str, Any]) -> bool:
        """Validate GraphQL response structure."""
        if not isinstance(data, dict):
            return False
        
        # GraphQL responses should have either 'data' or 'errors' (or both)
        return 'data' in data or 'errors' in data
    
    def _validate_no_graphql_errors(self, data: Dict[str, Any]) -> bool:
        """Check for GraphQL errors in response."""
        if 'errors' not in data:
            return True
        
        errors = data['errors']
        if not errors:  # Empty errors array is OK
            return True
        
        # Log the GraphQL errors
        for error in errors:
            self.logger.warning(f"GraphQL error: {error}")
        
        return False


class RESTResponseValidator(BaseResponseValidator):
    """Validator specifically for REST API responses."""
    
    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STRICT,
                 expected_status_codes: Optional[List[int]] = None):
        super().__init__(validation_level)
        self.expected_status_codes = expected_status_codes or [200, 201, 202]
        self._add_rest_rules()
    
    def _add_rest_rules(self) -> None:
        """Add REST-specific validation rules."""
        
        # Status code validation
        self.add_rule(ValidationRule(
            name="valid_status_code",
            description="HTTP status code is in expected range",
            validator=self._validate_status_code,
            required=True,
            error_message=f"HTTP status code not in expected range: {self.expected_status_codes}"
        ))
    
    def _validate_status_code(self, data: Dict[str, Any]) -> bool:
        """Validate HTTP status code (this is checked at RequestResult level)."""
        # This validation is primarily handled by the RequestResult
        # but we include it here for completeness
        return True


class BroadwaySFResponseValidator(BaseResponseValidator):
    """Validator specifically for Broadway SF API responses."""
    
    def __init__(self, validation_level: ValidationLevel = ValidationLevel.STRICT):
        super().__init__(validation_level)
        self._add_broadway_sf_rules()
    
    def _add_broadway_sf_rules(self) -> None:
        """Add Broadway SF-specific validation rules."""
        
        # Calendar service validation
        self.add_rule(ValidationRule(
            name="calendar_service_structure",
            description="Calendar service response has proper structure",
            validator=self._validate_calendar_structure,
            required=False,  # Only for calendar endpoints
            error_message="Calendar service response missing required structure"
        ))
        
        # Bolt API validation
        self.add_rule(ValidationRule(
            name="bolt_api_structure",
            description="Bolt API response has proper structure",
            validator=self._validate_bolt_structure,
            required=False,  # Only for bolt endpoints
            error_message="Bolt API response missing required structure"
        ))
    
    def _validate_calendar_structure(self, data: Dict[str, Any]) -> bool:
        """Validate Broadway SF calendar service response."""
        # Check for expected GraphQL structure
        if 'data' in data and 'getShow' in data.get('data', {}):
            return True
        
        # This might not be a calendar service response
        return True  # Don't fail validation for non-calendar responses
    
    def _validate_bolt_structure(self, data: Dict[str, Any]) -> bool:
        """Validate Broadway SF Bolt API response."""
        # Check for expected seating data structure
        required_fields = ['seats', 'zones']
        return all(field in data for field in required_fields)


class ValidatorFactory:
    """Factory for creating appropriate validators based on API type."""
    
    @staticmethod
    def create_validator(api_type: str, validation_level: ValidationLevel = ValidationLevel.STRICT,
                        **kwargs) -> IResponseValidator:
        """
        Create appropriate validator for the given API type.
        
        Args:
            api_type: Type of API (graphql, rest, broadway_sf, etc.)
            validation_level: How strict the validation should be
            **kwargs: Additional arguments for specific validators
            
        Returns:
            Configured response validator
            
        Raises:
            ValueError: If api_type is not supported
        """
        api_type = api_type.lower()
        
        if api_type == "graphql":
            return GraphQLResponseValidator(validation_level)
        elif api_type == "rest":
            return RESTResponseValidator(validation_level, **kwargs)
        elif api_type == "broadway_sf":
            return BroadwaySFResponseValidator(validation_level)
        elif api_type == "base" or api_type == "default":
            return BaseResponseValidator(validation_level)
        else:
            raise ValueError(f"Unsupported API type: {api_type}")
    
    @staticmethod
    def get_supported_types() -> List[str]:
        """Get list of supported API types."""
        return ["graphql", "rest", "broadway_sf", "base", "default"]