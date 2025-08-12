import re
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from decimal import Decimal, InvalidOperation
import logging
from urllib.parse import urljoin, urlparse

from ..exceptions.scraping_exceptions import DataExtractionException


class CommonExtractors:
    def __init__(self, base_url: str = ""):
        self.base_url = base_url
        self.logger = logging.getLogger(f"{__name__}.CommonExtractors")
    
    def safe_extract_text(self, element, selector: str = None, 
                         default: str = "", strip: bool = True) -> str:
        try:
            if element is None:
                return default
            
            if hasattr(element, 'inner_text'):
                text = element.inner_text()
            elif hasattr(element, 'text'):
                text = element.text
            elif hasattr(element, 'get_text'):
                text = element.get_text()
            else:
                text = str(element)
            
            return text.strip() if strip and text else text or default
        
        except Exception as e:
            self.logger.warning(f"Failed to extract text from element: {e}")
            return default
    
    def safe_extract_attribute(self, element, attribute: str, 
                              default: str = "") -> str:
        try:
            if element is None:
                return default
            
            if hasattr(element, 'get_attribute'):
                return element.get_attribute(attribute) or default
            elif hasattr(element, 'get'):
                return element.get(attribute) or default
            elif hasattr(element, attribute):
                return getattr(element, attribute) or default
            else:
                return default
        
        except Exception as e:
            self.logger.warning(f"Failed to extract attribute '{attribute}': {e}")
            return default
    
    def extract_price(self, text: str, currency_symbol: str = None) -> Optional[Decimal]:
        import os
        if currency_symbol is None:
            currency_symbol = os.getenv('DEFAULT_CURRENCY_SYMBOL', '$')
        if not text:
            return None
        
        try:
            price_pattern = rf'{re.escape(currency_symbol)}?(\d+(?:,\d{{3}})*(?:\.\d{{2}})?)'
            match = re.search(price_pattern, text.replace(',', ''))
            
            if match:
                price_str = match.group(1).replace(',', '')
                return Decimal(price_str)
            
            return None
        
        except (InvalidOperation, AttributeError) as e:
            self.logger.warning(f"Failed to extract price from '{text}': {e}")
            return None
    
    def extract_price_range(self, text: str, currency_symbol: str = None) -> Dict[str, Optional[Decimal]]:
        import os
        if currency_symbol is None:
            currency_symbol = os.getenv('DEFAULT_CURRENCY_SYMBOL', '$')
        if not text:
            return {"min_price": None, "max_price": None}
        
        prices = []
        price_pattern = rf'{re.escape(currency_symbol)}?(\d+(?:,\d{{3}})*(?:\.\d{{2}})?)'
        
        for match in re.finditer(price_pattern, text.replace(',', '')):
            try:
                price_str = match.group(1).replace(',', '')
                prices.append(Decimal(price_str))
            except InvalidOperation:
                continue
        
        if not prices:
            return {"min_price": None, "max_price": None}
        
        return {
            "min_price": min(prices),
            "max_price": max(prices) if len(prices) > 1 else min(prices)
        }
    
    def extract_date(self, text: str, formats: List[str] = None) -> Optional[datetime]:
        if not text:
            return None
        
        if formats is None:
            formats = [
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%B %d, %Y",
                "%b %d, %Y",
                "%A, %B %d, %Y",
                "%Y-%m-%d %H:%M:%S",
                "%m/%d/%Y %H:%M",
                "%Y-%m-%dT%H:%M:%S"
            ]
        
        text_clean = re.sub(r'\s+', ' ', text.strip())
        
        for date_format in formats:
            try:
                return datetime.strptime(text_clean, date_format)
            except ValueError:
                continue
        
        month_names = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        
        date_patterns = [
            r'(\w+)\s+(\d{1,2}),?\s+(\d{4})',
            r'(\d{1,2})\s+(\w+)\s+(\d{4})',
            r'(\d{4})-(\d{1,2})-(\d{1,2})',
            r'(\d{1,2})/(\d{1,2})/(\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, text_clean, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    if pattern == date_patterns[0]:
                        month_str, day, year = groups
                        month = month_names.get(month_str.lower())
                        if month:
                            return datetime(int(year), month, int(day))
                    elif pattern == date_patterns[1]:
                        day, month_str, year = groups
                        month = month_names.get(month_str.lower())
                        if month:
                            return datetime(int(year), month, int(day))
                    elif pattern == date_patterns[2]:
                        year, month, day = groups
                        return datetime(int(year), int(month), int(day))
                    elif pattern == date_patterns[3]:
                        month, day, year = groups
                        return datetime(int(year), int(month), int(day))
                except (ValueError, TypeError):
                    continue
        
        self.logger.warning(f"Could not parse date from: '{text}'")
        return None
    
    def extract_time(self, text: str) -> Optional[dict]:
        if not text:
            return None
        
        time_patterns = [
            r'(\d{1,2}):(\d{2})\s*(AM|PM)',
            r'(\d{1,2}):(\d{2})',
            r'(\d{1,2})\s*(AM|PM)'
        ]
        
        for pattern in time_patterns:
            match = re.search(pattern, text.upper())
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 3:
                        hour, minute, meridiem = groups
                        hour = int(hour)
                        minute = int(minute)
                        
                        if meridiem.upper() == 'PM' and hour != 12:
                            hour += 12
                        elif meridiem.upper() == 'AM' and hour == 12:
                            hour = 0
                    elif len(groups) == 2:
                        if groups[1].isdigit():
                            hour, minute = int(groups[0]), int(groups[1])
                        else:
                            hour, meridiem = int(groups[0]), groups[1]
                            minute = 0
                            if meridiem.upper() == 'PM' and hour != 12:
                                hour += 12
                            elif meridiem.upper() == 'AM' and hour == 12:
                                hour = 0
                    else:
                        continue
                    
                    return {
                        "hour": hour,
                        "minute": minute,
                        "formatted": f"{hour:02d}:{minute:02d}"
                    }
                
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def extract_duration(self, text: str) -> Optional[int]:
        if not text:
            return None
        
        duration_patterns = [
            r'(\d+)\s*hours?\s*(?:and\s*)?(\d+)?\s*minutes?',
            r'(\d+)\s*hrs?\s*(?:and\s*)?(\d+)?\s*mins?',
            r'(\d+)\s*h\s*(?:and\s*)?(\d+)?\s*m',
            r'(\d+)\s*minutes?',
            r'(\d+)\s*mins?',
            r'(\d+)\s*m(?!\w)',
            r'(\d+)h\s*(\d+)m',
            r'(\d{1,2}):(\d{2})\s*(?:hours?)?'
        ]
        
        for pattern in duration_patterns:
            match = re.search(pattern, text.lower())
            if match:
                try:
                    groups = match.groups()
                    if 'hour' in pattern or 'hr' in pattern or 'h' in pattern:
                        hours = int(groups[0])
                        minutes = int(groups[1]) if groups[1] else 0
                        return hours * 60 + minutes
                    elif 'minute' in pattern or 'min' in pattern or 'm' in pattern:
                        return int(groups[0])
                    elif ':' in pattern:
                        hours, minutes = int(groups[0]), int(groups[1])
                        return hours * 60 + minutes
                except (ValueError, IndexError):
                    continue
        
        return None
    
    def normalize_url(self, url: str) -> str:
        if not url:
            return ""
        
        if url.startswith('//'):
            return f"https:{url}"
        elif url.startswith('/'):
            return urljoin(self.base_url, url)
        elif not url.startswith(('http://', 'https://')):
            return f"https://{url}"
        
        return url
    
    def extract_phone_number(self, text: str) -> Optional[str]:
        if not text:
            return None
        
        phone_patterns = [
            r'\((\d{3})\)\s*(\d{3})-(\d{4})',
            r'(\d{3})-(\d{3})-(\d{4})',
            r'(\d{3})\.(\d{3})\.(\d{4})',
            r'(\d{3})\s+(\d{3})\s+(\d{4})',
            r'(\d{10})'
        ]
        
        for pattern in phone_patterns:
            match = re.search(pattern, text)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    return f"({groups[0]}) {groups[1]}-{groups[2]}"
                elif len(groups) == 1 and len(groups[0]) == 10:
                    phone = groups[0]
                    return f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"
        
        return None
    
    def extract_email(self, text: str) -> Optional[str]:
        if not text:
            return None
        
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        match = re.search(email_pattern, text)
        
        return match.group(0) if match else None
    
    def clean_text(self, text: str) -> str:
        if not text:
            return ""
        
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()
        text = text.replace('\u00a0', ' ')
        
        return text
    
    def extract_numbers(self, text: str) -> List[Union[int, float]]:
        if not text:
            return []
        
        numbers = []
        number_pattern = r'-?\d+(?:\.\d+)?'
        
        for match in re.finditer(number_pattern, text):
            try:
                num_str = match.group(0)
                if '.' in num_str:
                    numbers.append(float(num_str))
                else:
                    numbers.append(int(num_str))
            except ValueError:
                continue
        
        return numbers
    
    def extract_capacity_info(self, text: str) -> Dict[str, Any]:
        if not text:
            return {}
        
        result = {}
        
        capacity_patterns = [
            r'capacity[:\s]*(\d+)',
            r'seats[:\s]*(\d+)',
            r'max[:\s]*(\d+)',
            r'up\s+to\s+(\d+)',
            r'(\d+)\s*(?:people|guests|attendees)'
        ]
        
        for pattern in capacity_patterns:
            match = re.search(pattern, text.lower())
            if match:
                try:
                    result['total_capacity'] = int(match.group(1))
                    break
                except ValueError:
                    continue
        
        availability_patterns = [
            r'(\d+)\s*available',
            r'(\d+)\s*remaining',
            r'(\d+)\s*left',
            r'only\s+(\d+)'
        ]
        
        for pattern in availability_patterns:
            match = re.search(pattern, text.lower())
            if match:
                try:
                    result['available'] = int(match.group(1))
                    break
                except ValueError:
                    continue
        
        return result