"""
Comprehensive helper functions and utilities
"""
import re
import unicodedata
import hashlib
from typing import List, Dict, Optional, Any, Union
from datetime import datetime
import logging
import sys

def normalize_text(text: Union[str, None]) -> str:
    """Normalize text for consistent processing"""
    if not text:
        return ""
    
    # Convert to string and strip
    text = str(text).strip()
    
    # Remove accents and special characters
    text = unicodedata.normalize('NFD', text)
    text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
    
    # Convert to lowercase
    text = text.lower()
    
    # Replace multiple spaces with single space
    text = re.sub(r'\s+', ' ', text)
    
    return text.strip()

def extract_year_from_text(text: Union[str, None]) -> Optional[int]:
    """Extract year from text"""
    if not text:
        return None
    
    text = str(text)
    
    # Look for 4-digit years
    year_pattern = r'\b(19[8-9]\d|20[0-2]\d)\b'
    matches = re.findall(year_pattern, text)
    
    if matches:
        year = int(matches[0])
        current_year = datetime.now().year
        
        # Validate reasonable year range
        if 1980 <= year <= current_year + 1:
            return year
    
    return None

def extract_mileage_from_text(text: Union[str, None]) -> Optional[int]:
    """Extract mileage from text"""
    if not text:
        return None
    
    text = str(text).lower()
    
    # Pattern for mileage with various formats
    mileage_patterns = [
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*k?\s*miles?',
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*k\s*mi',
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*k(?:\s|$)',
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*km',  # Convert from km
        r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*kilometers?'
    ]
    
    for pattern in mileage_patterns:
        matches = re.findall(pattern, text)
        if matches:
            try:
                # Clean the number
                mileage_str = matches[0].replace(',', '')
                mileage = float(mileage_str)
                
                # Handle 'k' notation
                if 'k' in text and 'km' not in text:
                    mileage *= 1000
                
                # Convert km to miles
                if 'km' in text or 'kilometer' in text:
                    mileage *= 0.621371  # km to miles conversion
                
                mileage = int(mileage)
                
                # Validate reasonable range
                if 0 <= mileage <= 500000:
                    return mileage
                    
            except (ValueError, TypeError):
                continue
    
    return None

def extract_price_from_text(text: Union[str, None]) -> Optional[float]:
    """Extract price from text"""
    if not text:
        return None
    
    text = str(text).lower()
    
    # Price patterns for various currencies
    price_patterns = [
        r'[£$€¥]\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',  # Currency symbols
        r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*(?:pounds?|dollars?|euros?|yen)',  # Word currencies
        r'(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)',  # Just numbers (fallback)
    ]
    
    for pattern in price_patterns:
        matches = re.findall(pattern, text)
        if matches:
            try:
                # Clean the number
                price_str = matches[0].replace(',', '')
                price = float(price_str)
                
                # Validate reasonable range (£500 to £500,000)
                if 500 <= price <= 500000:
                    return price
                    
            except (ValueError, TypeError):
                continue
    
    return None

def validate_vehicle_data(data: Dict) -> List[str]:
    """Validate vehicle data and return list of errors"""
    errors = []
    
    # Required fields
    required_fields = ['make', 'model', 'year']
    for field in required_fields:
        if not data.get(field):
            errors.append(f"Missing required field: {field}")
    
    # Year validation
    year = data.get('year')
    if year:
        try:
            year = int(year)
            current_year = datetime.now().year
            if year < 1980 or year > current_year + 1:
                errors.append(f"Invalid year: {year} (must be between 1980 and {current_year + 1})")
        except (ValueError, TypeError):
            errors.append(f"Invalid year format: {year}")
    
    # Price validation
    price = data.get('price')
    if price is not None:
        try:
            price = float(price)
            if price < 0:
                errors.append("Price cannot be negative")
            elif price > 1000000:
                errors.append("Price seems unreasonably high")
        except (ValueError, TypeError):
            errors.append(f"Invalid price format: {price}")
    
    # Mileage validation
    mileage = data.get('mileage')
    if mileage is not None:
        try:
            mileage = int(mileage)
            if mileage < 0:
                errors.append("Mileage cannot be negative")
            elif mileage > 1000000:
                errors.append("Mileage seems unreasonably high")
        except (ValueError, TypeError):
            errors.append(f"Invalid mileage format: {mileage}")
    
    return errors

def clean_vehicle_data(data: Dict) -> Dict:
    """Clean and normalize vehicle data"""
    cleaned = data.copy()
    
    # Normalize text fields
    text_fields = ['make', 'model', 'fuel_type', 'color', 'transmission', 'drive_type']
    for field in text_fields:
        if field in cleaned and cleaned[field]:
            cleaned[field] = normalize_text(cleaned[field])
    
    # Convert numeric fields
    numeric_fields = {
        'year': int,
        'price': float,
        'mileage': int,
        'engine_size': int
    }
    
    for field, converter in numeric_fields.items():
        if field in cleaned and cleaned[field] is not None:
            try:
                cleaned[field] = converter(cleaned[field])
            except (ValueError, TypeError):
                # Keep original value if conversion fails
                pass
    
    return cleaned

def calculate_age_from_year(year: Optional[int]) -> int:
    """Calculate vehicle age from year"""
    if not year or year <= 0:
        return 0
    
    current_year = datetime.now().year
    return max(0, current_year - year)

def format_currency(amount: Optional[float], currency: str = 'GBP') -> str:
    """Format currency amount"""
    if amount is None:
        return 'N/A'
    
    currency_symbols = {
        'GBP': '£',
        'USD': '$',
        'EUR': '€',
        'JPY': '¥'
    }
    
    symbol = currency_symbols.get(currency, '£')
    
    if currency == 'JPY':
        # No decimal places for Yen
        return f"{symbol}{amount:,.0f}"
    else:
        return f"{symbol}{amount:,.2f}"

def generate_cache_key(*args) -> str:
    """Generate cache key from arguments"""
    key_string = '|'.join(str(arg) for arg in args)
    return hashlib.md5(key_string.encode()).hexdigest()

def calculate_percentage_change(old_value: float, new_value: float) -> float:
    """Calculate percentage change between two values"""
    if old_value == 0:
        return 0 if new_value == 0 else float('inf')
    
    return ((new_value - old_value) / old_value) * 100

def safe_divide(numerator: float, denominator: float, default: float = 0) -> float:
    """Safely divide two numbers, returning default if denominator is zero"""
    if denominator == 0:
        return default
    return numerator / denominator

def clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value between min and max"""
    return max(min_val, min(max_val, value))

def round_to_nearest(value: float, nearest: float = 1.0) -> float:
    """Round value to nearest specified increment"""
    return round(value / nearest) * nearest

def is_valid_url(url: str) -> bool:
    """Check if URL is valid"""
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return url_pattern.match(url) is not None

def clean_url(url: str) -> str:
    """Clean URL by removing tracking parameters"""
    if not url:
        return ""
    
    # Remove common tracking parameters
    tracking_params = [
        'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
        'gclid', 'fbclid', 'msclkid', '_ga', 'ref', 'source'
    ]
    
    # Parse URL and remove tracking parameters
    if '?' in url:
        base_url, query_string = url.split('?', 1)
        if query_string:
            params = []
            for param in query_string.split('&'):
                if '=' in param:
                    key, value = param.split('=', 1)
                    if key not in tracking_params:
                        params.append(param)
            
            if params:
                return base_url + '?' + '&'.join(params)
            else:
                return base_url
    
    return url

def get_make_brand_mapping() -> Dict[str, str]:
    """Get mapping of common make variations to standard names"""
    return {
        'vw': 'volkswagen',
        'merc': 'mercedes-benz',
        'mercedes': 'mercedes-benz',
        'benz': 'mercedes-benz',
        'beemer': 'bmw',
        'bmw': 'bmw',
        'audi': 'audi',
        'toyota': 'toyota',
        'honda': 'honda',
        'nissan': 'nissan',
        'ford': 'ford',
        'vauxhall': 'vauxhall',
        'opel': 'vauxhall',  # Opel is Vauxhall in UK
        'peugeot': 'peugeot',
        'citroen': 'citroën',
        'renault': 'renault',
        'hyundai': 'hyundai',
        'kia': 'kia',
        'mazda': 'mazda',
        'subaru': 'subaru',
        'mitsubishi': 'mitsubishi',
        'lexus': 'lexus',
        'infiniti': 'infiniti',
        'acura': 'honda',  # Acura is Honda luxury brand
        'seat': 'seat',
        'skoda': 'škoda',
        'volvo': 'volvo',
        'saab': 'saab',
        'jaguar': 'jaguar',
        'land rover': 'land rover',
        'range rover': 'land rover',
        'mini': 'mini',
        'fiat': 'fiat',
        'alfa romeo': 'alfa romeo',
        'maserati': 'maserati',
        'ferrari': 'ferrari',
        'lamborghini': 'lamborghini',
        'porsche': 'porsche',
        'bentley': 'bentley',
        'rolls royce': 'rolls-royce',
        'aston martin': 'aston martin'
    }

def standardize_fuel_type(fuel_type: str) -> str:
    """Standardize fuel type names"""
    if not fuel_type:
        return 'unknown'
    
    fuel_type = normalize_text(fuel_type)
    
    fuel_mapping = {
        'gas': 'petrol',
        'gasoline': 'petrol',
        'benzin': 'petrol',
        'unleaded': 'petrol',
        'petrol': 'petrol',
        'diesel': 'diesel',
        'electric': 'electric',
        'ev': 'electric',
        'battery': 'electric',
        'hybrid': 'hybrid',
        'hev': 'hybrid',
        'phev': 'plug-in hybrid',
        'plug-in hybrid': 'plug-in hybrid',
        'plugin hybrid': 'plug-in hybrid',
        'hydrogen': 'hydrogen',
        'fuel cell': 'hydrogen',
        'lpg': 'lpg',
        'cng': 'cng',
        'bi-fuel': 'bi-fuel'
    }
    
    return fuel_mapping.get(fuel_type, fuel_type)

def estimate_co2_emissions(year: int, engine_size: Optional[int], fuel_type: str) -> Optional[int]:
    """Estimate CO2 emissions based on vehicle characteristics"""
    if not year or not fuel_type:
        return None
    
    fuel_type = standardize_fuel_type(fuel_type)
    
    # Base emissions by fuel type (g/km)
    base_emissions = {
        'petrol': 150,
        'diesel': 120,
        'hybrid': 90,
        'plug-in hybrid': 50,
        'electric': 0,
        'hydrogen': 0
    }
    
    base = base_emissions.get(fuel_type, 150)
    
    # Adjust for engine size
    if engine_size and engine_size > 0:
        # Larger engines generally produce more emissions
        size_factor = 1 + ((engine_size - 1600) / 1000) * 0.2
        base = int(base * max(0.5, size_factor))
    
    # Adjust for year (newer cars are generally cleaner)
    if year >= 2020:
        base = int(base * 0.85)  # 15% reduction for newest cars
    elif year >= 2015:
        base = int(base * 0.90)  # 10% reduction
    elif year >= 2010:
        base = int(base * 0.95)  # 5% reduction
    elif year < 2000:
        base = int(base * 1.20)  # 20% increase for older cars
    
    return max(0, base)

def categorize_vehicle_by_price(price: float) -> str:
    """Categorize vehicle by price range"""
    if price < 5000:
        return 'budget'
    elif price < 15000:
        return 'economy'
    elif price < 30000:
        return 'mid-range'
    elif price < 50000:
        return 'premium'
    elif price < 100000:
        return 'luxury'
    else:
        return 'super-luxury'

def calculate_depreciation_rate(current_price: float, original_price: float, age_years: int) -> float:
    """Calculate annual depreciation rate"""
    if age_years <= 0 or original_price <= 0 or current_price <= 0:
        return 0.0
    
    # Calculate compound annual depreciation rate
    depreciation_ratio = current_price / original_price
    annual_rate = (depreciation_ratio ** (1 / age_years)) - 1
    
    return abs(annual_rate) * 100  # Return as percentage

def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Setup logger with consistent formatting"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(handler)
        logger.setLevel(level)
    
    return logger