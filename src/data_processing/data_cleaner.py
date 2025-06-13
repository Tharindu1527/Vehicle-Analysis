"""
Data cleaning and preprocessing module
Handles data validation, normalization, and quality checks
"""
import re
import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import unicodedata
import json

from src.utils.logger import setup_logger
from src.utils.helpers import (
    normalize_text, extract_year_from_text, extract_mileage_from_text,
    extract_price_from_text, validate_vehicle_data, clean_vehicle_data
)

logger = setup_logger(__name__)

class DataCleaner:
    """Comprehensive data cleaning and validation"""
    
    def __init__(self):
        self.validation_rules = self._load_validation_rules()
        self.brand_mappings = self._load_brand_mappings()
        self.model_mappings = self._load_model_mappings()
        
    def clean_uk_market_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Clean and validate UK market data"""
        logger.info(f"Cleaning {len(raw_data)} UK market records")
        
        cleaned_data = []
        errors = []
        
        for i, record in enumerate(raw_data):
            try:
                cleaned_record = self._clean_uk_record(record)
                
                # Validate cleaned record
                validation_errors = validate_vehicle_data(cleaned_record)
                if not validation_errors:
                    cleaned_data.append(cleaned_record)
                else:
                    errors.append({
                        'index': i,
                        'record': record,
                        'errors': validation_errors
                    })
                    
            except Exception as e:
                logger.error(f"Error cleaning UK record {i}: {str(e)}")
                errors.append({
                    'index': i,
                    'record': record,
                    'errors': [f"Cleaning error: {str(e)}"]
                })
        
        logger.info(f"Cleaned {len(cleaned_data)} records, {len(errors)} errors")
        return cleaned_data
    
    def clean_japan_auction_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Clean and validate Japan auction data"""
        logger.info(f"Cleaning {len(raw_data)} Japan auction records")
        
        cleaned_data = []
        errors = []
        
        for i, record in enumerate(raw_data):
            try:
                cleaned_record = self._clean_japan_record(record)
                
                # Validate cleaned record
                validation_errors = self._validate_auction_record(cleaned_record)
                if not validation_errors:
                    cleaned_data.append(cleaned_record)
                else:
                    errors.append({
                        'index': i,
                        'record': record,
                        'errors': validation_errors
                    })
                    
            except Exception as e:
                logger.error(f"Error cleaning Japan record {i}: {str(e)}")
                errors.append({
                    'index': i,
                    'record': record,
                    'errors': [f"Cleaning error: {str(e)}"]
                })
        
        logger.info(f"Cleaned {len(cleaned_data)} records, {len(errors)} errors")
        return cleaned_data
    
    def clean_government_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Clean and validate government data"""
        logger.info(f"Cleaning {len(raw_data)} government records")
        
        cleaned_data = []
        
        for record in raw_data:
            try:
                cleaned_record = self._clean_government_record(record)
                if cleaned_record:
                    cleaned_data.append(cleaned_record)
                    
            except Exception as e:
                logger.error(f"Error cleaning government record: {str(e)}")
        
        logger.info(f"Cleaned {len(cleaned_data)} government records")
        return cleaned_data
    
    def _clean_uk_record(self, record: Dict) -> Dict:
        """Clean individual UK market record"""
        cleaned = {}
        
        # Clean basic vehicle info
        cleaned['source'] = str(record.get('source', '')).strip().lower()
        cleaned['make'] = self._normalize_make(record.get('make', ''))
        cleaned['model'] = self._normalize_model(record.get('model', ''), cleaned.get('make', ''))
        cleaned['year'] = self._extract_and_validate_year(record.get('year'))
        cleaned['fuel_type'] = self._normalize_fuel_type(record.get('fuel_type', ''))
        
        # Clean pricing and mileage
        cleaned['price'] = self._extract_and_validate_price(record.get('price'))
        cleaned['mileage'] = self._extract_and_validate_mileage(record.get('mileage'))
        
        # Clean location and seller info
        cleaned['location'] = self._normalize_location(record.get('location', ''))
        cleaned['seller_type'] = self._normalize_seller_type(record.get('seller_type', ''))
        
        # Clean dates and URLs
        cleaned['listing_date'] = self._normalize_date(record.get('listing_date'))
        cleaned['days_listed'] = self._calculate_days_listed(
            record.get('days_listed'), 
            cleaned.get('listing_date')
        )
        cleaned['url'] = self._clean_url(record.get('url', ''))
        
        # Add metadata
        cleaned['cleaned_at'] = datetime.now().isoformat()
        cleaned['data_quality_score'] = self._calculate_quality_score(cleaned)
        
        return cleaned
    
    def _clean_japan_record(self, record: Dict) -> Dict:
        """Clean individual Japan auction record"""
        cleaned = {}
        
        # Clean basic vehicle info
        cleaned['source'] = str(record.get('source', '')).strip().lower()
        cleaned['make'] = self._normalize_make(record.get('make', ''))
        cleaned['model'] = self._normalize_model(record.get('model', ''), cleaned.get('make', ''))
        cleaned['year'] = self._extract_and_validate_year(record.get('year'))
        cleaned['fuel_type'] = self._normalize_fuel_type(record.get('fuel_type', ''))
        
        # Clean auction-specific data
        cleaned['hammer_price'] = self._validate_hammer_price(record.get('hammer_price'))
        cleaned['mileage'] = self._extract_and_validate_mileage(record.get('mileage'))
        
        # Clean grading
        cleaned['condition_grade'] = self._normalize_condition_grade(record.get('condition_grade', ''))
        cleaned['exterior_grade'] = self._normalize_exterior_grade(record.get('exterior_grade', ''))
        cleaned['interior_grade'] = self._normalize_interior_grade(record.get('interior_grade', ''))
        
        # Clean auction details
        cleaned['auction_date'] = self._normalize_date(record.get('auction_date'))
        cleaned['auction_house'] = self._normalize_auction_house(record.get('auction_house', ''))
        cleaned['lot_number'] = str(record.get('lot_number', '')).strip()
        
        # Clean technical specs
        cleaned['engine_size'] = self._validate_engine_size(record.get('engine_size'))
        cleaned['transmission'] = self._normalize_transmission(record.get('transmission', ''))
        cleaned['drive_type'] = self._normalize_drive_type(record.get('drive_type', ''))
        cleaned['color'] = self._normalize_color(record.get('color', ''))
        
        # Clean fees and costs
        cleaned['seller_fees'] = self._validate_fee(record.get('seller_fees', 0))
        cleaned['buyer_fees'] = self._validate_fee(record.get('buyer_fees', 0))
        cleaned['total_landed_cost_gbp'] = self._validate_landed_cost(
            record.get('total_landed_cost_gbp', 0)
        )
        
        # Add metadata
        cleaned['cleaned_at'] = datetime.now().isoformat()
        cleaned['data_quality_score'] = self._calculate_quality_score(cleaned)
        
        return cleaned
    
    def _clean_government_record(self, record: Dict) -> Optional[Dict]:
        """Clean individual government record"""
        try:
            cleaned = {}
            
            # Clean basic info
            cleaned['data_type'] = str(record.get('data_type', 'registration')).strip().lower()
            cleaned['make'] = self._normalize_make(record.get('make', ''))
            cleaned['model'] = self._normalize_model(record.get('model', ''), cleaned.get('make', ''))
            cleaned['year'] = self._extract_and_validate_year(record.get('year'))
            cleaned['fuel_type'] = self._normalize_fuel_type(record.get('fuel_type', ''))
            
            # Clean counts and regions
            cleaned['registration_count'] = self._validate_count(record.get('registration_count', 0))
            cleaned['region'] = self._normalize_region(record.get('region', ''))
            cleaned['month'] = self._validate_month(record.get('month'))
            
            # Additional fields
            cleaned['vehicle_category'] = self._normalize_vehicle_category(
                record.get('vehicle_category', '')
            )
            cleaned['body_type'] = self._normalize_body_type(record.get('body_type', ''))
            cleaned['engine_size'] = self._validate_engine_size(record.get('engine_size'))
            cleaned['co2_emissions'] = self._validate_emissions(record.get('co2_emissions'))
            
            return cleaned
            
        except Exception as e:
            logger.error(f"Error cleaning government record: {str(e)}")
            return None
    
    def _normalize_make(self, make: str) -> str:
        """Normalize vehicle make"""
        if not make:
            return ''
        
        # Clean and normalize
        make = normalize_text(str(make))
        
        # Apply brand mappings
        make = self.brand_mappings.get(make, make)
        
        # Capitalize properly
        return make.title() if make else ''
    
    def _normalize_model(self, model: str, make: str = '') -> str:
        """Normalize vehicle model"""
        if not model:
            return ''
        
        # Clean and normalize
        model = normalize_text(str(model))
        
        # Apply model mappings (make-specific)
        key = f"{make.lower()}_{model}"
        model = self.model_mappings.get(key, model)
        
        # Clean common variations
        model = re.sub(r'\b(mk|mark)\s*(\d+)\b', r'Mk\2', model, flags=re.IGNORECASE)
        model = re.sub(r'\b(\d+)\.(\d+)\b', r'\1.\2', model)  # Fix decimal spacing
        
        return model.title() if model else ''
    
    def _normalize_fuel_type(self, fuel_type: str) -> str:
        """Normalize fuel type"""
        if not fuel_type:
            return ''
        
        fuel_type = normalize_text(str(fuel_type))
        
        # Mapping for common variations
        fuel_mappings = {
            'gas': 'petrol',
            'gasoline': 'petrol',
            'benzin': 'petrol',
            'unleaded': 'petrol',
            'diesel': 'diesel',
            'electric': 'electric',
            'ev': 'electric',
            'battery': 'electric',
            'hybrid': 'hybrid',
            'hev': 'hybrid',
            'phev': 'plug-in hybrid',
            'plug-in hybrid': 'plug-in hybrid',
            'hydrogen': 'hydrogen',
            'fuel cell': 'hydrogen',
            'lpg': 'lpg',
            'cng': 'cng'
        }
        
        return fuel_mappings.get(fuel_type, fuel_type)
    
    def _extract_and_validate_year(self, year_data: Any) -> Optional[int]:
        """Extract and validate year"""
        if year_data is None:
            return None
        
        # If already integer
        if isinstance(year_data, int):
            year = year_data
        # If string, try to extract
        elif isinstance(year_data, str):
            year = extract_year_from_text(year_data)
        else:
            try:
                year = int(float(year_data))
            except (ValueError, TypeError):
                return None
        
        # Validate range
        current_year = datetime.now().year
        if year and 1980 <= year <= current_year + 1:
            return year
        
        return None
    
    def _extract_and_validate_price(self, price_data: Any) -> Optional[float]:
        """Extract and validate price"""
        if price_data is None:
            return None
        
        # If already numeric
        if isinstance(price_data, (int, float)):
            price = float(price_data)
        # If string, try to extract
        elif isinstance(price_data, str):
            price = extract_price_from_text(price_data)
        else:
            try:
                price = float(price_data)
            except (ValueError, TypeError):
                return None
        
        # Validate range (£500 to £500,000)
        if price and 500 <= price <= 500000:
            return round(price, 2)
        
        return None
    
    def _extract_and_validate_mileage(self, mileage_data: Any) -> Optional[int]:
        """Extract and validate mileage"""
        if mileage_data is None:
            return None
        
        # If already numeric
        if isinstance(mileage_data, (int, float)):
            mileage = int(mileage_data)
        # If string, try to extract
        elif isinstance(mileage_data, str):
            mileage = extract_mileage_from_text(mileage_data)
        else:
            try:
                mileage = int(float(mileage_data))
            except (ValueError, TypeError):
                return None
        
        # Validate range (0 to 500,000 miles)
        if mileage is not None and 0 <= mileage <= 500000:
            return mileage
        
        return None
    
    def _normalize_condition_grade(self, grade: str) -> str:
        """Normalize auction condition grade"""
        if not grade:
            return ''
        
        grade = str(grade).strip().upper()
        
        # Handle numeric grades (1-5 scale)
        if re.match(r'^[1-5](\.[05])?$', grade):
            return grade
        
        # Handle letter grades (R, A, B, C, etc.)
        if re.match(r'^[RABCS]$', grade):
            return grade
        
        # Handle combinations
        if re.match(r'^[1-5][RABCS]?$', grade):
            return grade
        
        return grade
    
    def _normalize_location(self, location: str) -> str:
        """Normalize location"""
        if not location:
            return ''
        
        location = normalize_text(str(location))
        
        # Clean common location formats
        location = re.sub(r'\b(uk|united kingdom|england|scotland|wales|ni)\b', '', location)
        location = re.sub(r'\s+', ' ', location).strip()
        
        return location.title() if location else ''
    
    def _normalize_date(self, date_data: Any) -> Optional[str]:
        """Normalize date to ISO format"""
        if not date_data:
            return None
        
        date_str = str(date_data).strip()
        
        # Try common date formats
        formats = [
            '%Y-%m-%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%d-%m-%Y',
            '%Y/%m/%d'
        ]
        
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.split('.')[0], fmt)  # Remove microseconds
                return dt.date().isoformat()
            except ValueError