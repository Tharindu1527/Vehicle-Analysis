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
    
    def _normalize_exterior_grade(self, grade: str) -> str:
        """Normalize exterior grade"""
        if not grade:
            return ''
        
        grade = str(grade).strip().upper()
        
        # Common grades: A, B, C, D, E, S, R
        if re.match(r'^[ABCDESR]$', grade):
            return grade
        
        # Numeric equivalents
        grade_mapping = {
            '1': 'S', '2': 'A', '3': 'B', '4': 'C', '5': 'D', '6': 'E'
        }
        
        return grade_mapping.get(grade, grade)
    
    def _normalize_interior_grade(self, grade: str) -> str:
        """Normalize interior grade"""
        return self._normalize_exterior_grade(grade)  # Same logic
    
    def _normalize_location(self, location: str) -> str:
        """Normalize location"""
        if not location:
            return ''
        
        location = normalize_text(str(location))
        
        # Clean common location formats
        location = re.sub(r'\b(uk|united kingdom|england|scotland|wales|ni)\b', '', location)
        location = re.sub(r'\s+', ' ', location).strip()
        
        return location.title() if location else ''
    
    def _normalize_seller_type(self, seller_type: str) -> str:
        """Normalize seller type"""
        if not seller_type:
            return ''
        
        seller_type = normalize_text(str(seller_type))
        
        # Standardize seller types
        if seller_type in ['dealer', 'dealership', 'garage', 'motor trader']:
            return 'dealer'
        elif seller_type in ['private', 'individual', 'owner']:
            return 'private'
        elif seller_type in ['trade', 'trader', 'wholesale']:
            return 'trade'
        else:
            return seller_type
    
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
            except ValueError:
                continue
        
        return None
    
    def _calculate_days_listed(self, days_listed: Any, listing_date: Optional[str]) -> Optional[int]:
        """Calculate days listed"""
        # If days_listed is provided directly
        if days_listed is not None:
            try:
                days = int(days_listed)
                if 0 <= days <= 365:  # Reasonable range
                    return days
            except (ValueError, TypeError):
                pass
        
        # Calculate from listing date
        if listing_date:
            try:
                listing_dt = datetime.fromisoformat(listing_date)
                days = (datetime.now().date() - listing_dt.date()).days
                if days >= 0:
                    return days
            except (ValueError, TypeError):
                pass
        
        return None
    
    def _clean_url(self, url: str) -> str:
        """Clean URL by removing tracking parameters"""
        if not url:
            return ''
        
        # Remove common tracking parameters
        tracking_params = [
            'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
            'gclid', 'fbclid', 'msclkid', '_ga', 'ref', 'source'
        ]
        
        try:
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
        except Exception:
            return url
    
    def _normalize_auction_house(self, auction_house: str) -> str:
        """Normalize auction house name"""
        if not auction_house:
            return ''
        
        auction_house = normalize_text(str(auction_house))
        
        # Standardize auction house names
        house_mappings = {
            'uss': 'USS',
            'ussauto': 'USS',
            'uss auto': 'USS',
            'ju': 'JU',
            'ju-net': 'JU',
            'aucnet': 'AucNet',
            'auc net': 'AucNet',
            'taa': 'TAA',
            'laa': 'LAA',
            'jaa': 'JAA'
        }
        
        return house_mappings.get(auction_house, auction_house.title())
    
    def _validate_hammer_price(self, hammer_price: Any) -> Optional[float]:
        """Validate hammer price (in JPY)"""
        if hammer_price is None:
            return None
        
        try:
            price = float(hammer_price)
            # Reasonable range for Japanese auction prices (¥50,000 to ¥10,000,000)
            if 50000 <= price <= 10000000:
                return price
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _validate_engine_size(self, engine_size: Any) -> Optional[int]:
        """Validate engine size (in cc)"""
        if engine_size is None:
            return None
        
        try:
            size = int(engine_size)
            # Reasonable range (250cc to 8000cc)
            if 250 <= size <= 8000:
                return size
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _normalize_transmission(self, transmission: str) -> str:
        """Normalize transmission type"""
        if not transmission:
            return ''
        
        transmission = normalize_text(str(transmission))
        
        # Standardize transmission types
        trans_mappings = {
            'auto': 'automatic',
            'at': 'automatic',
            'automatic': 'automatic',
            'mt': 'manual',
            'man': 'manual',
            'manual': 'manual',
            'cvt': 'cvt',
            'dct': 'dct',
            'dual clutch': 'dct',
            'semi-auto': 'semi-automatic',
            'tiptronic': 'semi-automatic'
        }
        
        return trans_mappings.get(transmission, transmission)
    
    def _normalize_drive_type(self, drive_type: str) -> str:
        """Normalize drive type"""
        if not drive_type:
            return ''
        
        drive_type = normalize_text(str(drive_type))
        
        # Standardize drive types
        drive_mappings = {
            '2wd': '2WD',
            'fwd': 'FWD',
            'ff': 'FWD',
            'rwd': 'RWD',
            'fr': 'RWD',
            '4wd': '4WD',
            'awd': 'AWD',
            '4x4': '4WD'
        }
        
        return drive_mappings.get(drive_type, drive_type.upper())
    
    def _normalize_color(self, color: str) -> str:
        """Normalize color"""
        if not color:
            return ''
        
        color = normalize_text(str(color))
        
        # Standardize common colors
        color_mappings = {
            'white': 'White',
            'black': 'Black',
            'silver': 'Silver',
            'grey': 'Grey',
            'gray': 'Grey',
            'red': 'Red',
            'blue': 'Blue',
            'green': 'Green',
            'yellow': 'Yellow',
            'orange': 'Orange',
            'brown': 'Brown',
            'purple': 'Purple',
            'pink': 'Pink',
            'gold': 'Gold',
            'bronze': 'Bronze'
        }
        
        return color_mappings.get(color, color.title())
    
    def _validate_fee(self, fee: Any) -> float:
        """Validate fee amount"""
        if fee is None:
            return 0.0
        
        try:
            fee_amount = float(fee)
            if fee_amount >= 0:
                return fee_amount
        except (ValueError, TypeError):
            pass
        
        return 0.0
    
    def _validate_landed_cost(self, cost: Any) -> Optional[float]:
        """Validate landed cost"""
        if cost is None:
            return None
        
        try:
            cost_amount = float(cost)
            # Reasonable range for landed cost (£1,000 to £100,000)
            if 1000 <= cost_amount <= 100000:
                return cost_amount
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _validate_count(self, count: Any) -> int:
        """Validate count (registration count, etc.)"""
        if count is None:
            return 0
        
        try:
            count_val = int(count)
            if count_val >= 0:
                return count_val
        except (ValueError, TypeError):
            pass
        
        return 0
    
    def _normalize_region(self, region: str) -> str:
        """Normalize UK region"""
        if not region:
            return ''
        
        region = normalize_text(str(region))
        
        # UK region mappings
        region_mappings = {
            'london': 'London',
            'south east': 'South East',
            'south west': 'South West',
            'west midlands': 'West Midlands',
            'east midlands': 'East Midlands',
            'north west': 'North West',
            'north east': 'North East',
            'yorkshire': 'Yorkshire and the Humber',
            'east of england': 'East of England',
            'scotland': 'Scotland',
            'wales': 'Wales',
            'northern ireland': 'Northern Ireland'
        }
        
        return region_mappings.get(region, region.title())
    
    def _validate_month(self, month: Any) -> Optional[int]:
        """Validate month (1-12)"""
        if month is None:
            return None
        
        try:
            month_val = int(month)
            if 1 <= month_val <= 12:
                return month_val
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _normalize_vehicle_category(self, category: str) -> str:
        """Normalize vehicle category"""
        if not category:
            return ''
        
        category = normalize_text(str(category))
        
        # Category mappings
        category_mappings = {
            'car': 'Car',
            'suv': 'SUV',
            'truck': 'Truck',
            'van': 'Van',
            'motorcycle': 'Motorcycle',
            'bus': 'Bus',
            'trailer': 'Trailer'
        }
        
        return category_mappings.get(category, category.title())
    
    def _normalize_body_type(self, body_type: str) -> str:
        """Normalize body type"""
        if not body_type:
            return ''
        
        body_type = normalize_text(str(body_type))
        
        # Body type mappings
        body_mappings = {
            'hatchback': 'Hatchback',
            'saloon': 'Saloon',
            'sedan': 'Saloon',
            'estate': 'Estate',
            'wagon': 'Estate',
            'suv': 'SUV',
            'coupe': 'Coupe',
            'convertible': 'Convertible',
            'cabriolet': 'Convertible',
            'mpv': 'MPV',
            'van': 'Van',
            'pickup': 'Pickup'
        }
        
        return body_mappings.get(body_type, body_type.title())
    
    def _validate_emissions(self, emissions: Any) -> Optional[int]:
        """Validate CO2 emissions (g/km)"""
        if emissions is None:
            return None
        
        try:
            emissions_val = int(emissions)
            if 0 <= emissions_val <= 500:  # Reasonable range
                return emissions_val
        except (ValueError, TypeError):
            pass
        
        return None
    
    def _calculate_quality_score(self, record: Dict) -> float:
        """Calculate data quality score (0-100)"""
        try:
            score = 0
            max_score = 0
            
            # Required fields scoring
            required_fields = ['make', 'model', 'year']
            for field in required_fields:
                max_score += 20
                if record.get(field):
                    score += 20
            
            # Optional but important fields
            important_fields = ['price', 'mileage', 'fuel_type']
            for field in important_fields:
                max_score += 10
                if record.get(field):
                    score += 10
            
            # Additional fields
            additional_fields = ['location', 'seller_type', 'url']
            for field in additional_fields:
                max_score += 5
                if record.get(field):
                    score += 5
            
            # Bonus for completeness
            filled_fields = sum(1 for v in record.values() if v is not None and v != '')
            total_fields = len(record)
            if total_fields > 0:
                completeness_bonus = (filled_fields / total_fields) * 15
                score += completeness_bonus
                max_score += 15
            
            return round((score / max_score) * 100, 1) if max_score > 0 else 0.0
            
        except Exception as e:
            logger.error(f"Error calculating quality score: {str(e)}")
            return 0.0
    
    def _validate_auction_record(self, record: Dict) -> List[str]:
        """Validate auction-specific record"""
        errors = []
        
        # Basic validation
        basic_errors = validate_vehicle_data(record)
        errors.extend(basic_errors)
        
        # Auction-specific validation
        if not record.get('hammer_price'):
            errors.append("Missing hammer price")
        
        if not record.get('auction_date'):
            errors.append("Missing auction date")
        
        if not record.get('condition_grade'):
            errors.append("Missing condition grade")
        
        return errors
    
    def _load_validation_rules(self) -> Dict:
        """Load validation rules"""
        return {
            'year_range': (1980, datetime.now().year + 1),
            'price_range': (500, 500000),
            'mileage_range': (0, 500000),
            'hammer_price_range': (50000, 10000000),
            'engine_size_range': (250, 8000)
        }
    
    def _load_brand_mappings(self) -> Dict[str, str]:
        """Load brand name mappings"""
        return {
            'vw': 'Volkswagen',
            'volkswagen': 'Volkswagen',
            'merc': 'Mercedes-Benz',
            'mercedes': 'Mercedes-Benz',
            'benz': 'Mercedes-Benz',
            'mercedes-benz': 'Mercedes-Benz',
            'bmw': 'BMW',
            'beemer': 'BMW',
            'audi': 'Audi',
            'toyota': 'Toyota',
            'honda': 'Honda',
            'nissan': 'Nissan',
            'ford': 'Ford',
            'vauxhall': 'Vauxhall',
            'opel': 'Vauxhall',  # Opel is Vauxhall in UK
            'peugeot': 'Peugeot',
            'citroen': 'Citroën',
            'renault': 'Renault',
            'hyundai': 'Hyundai',
            'kia': 'Kia',
            'mazda': 'Mazda',
            'subaru': 'Subaru',
            'mitsubishi': 'Mitsubishi',
            'lexus': 'Lexus',
            'infiniti': 'Infiniti',
            'acura': 'Honda',  # Acura is Honda luxury brand
            'seat': 'SEAT',
            'skoda': 'Škoda',
            'volvo': 'Volvo',
            'saab': 'Saab',
            'jaguar': 'Jaguar',
            'land rover': 'Land Rover',
            'range rover': 'Land Rover',
            'mini': 'MINI',
            'fiat': 'Fiat',
            'alfa romeo': 'Alfa Romeo',
            'maserati': 'Maserati',
            'ferrari': 'Ferrari',
            'lamborghini': 'Lamborghini',
            'porsche': 'Porsche',
            'bentley': 'Bentley',
            'rolls royce': 'Rolls-Royce',
            'aston martin': 'Aston Martin',
            'tesla': 'Tesla',
            'suzuki': 'Suzuki',
            'daihatsu': 'Daihatsu',
            'isuzu': 'Isuzu'
        }
    
    def _load_model_mappings(self) -> Dict[str, str]:
        """Load model name mappings (make-specific)"""
        return {
            # Toyota models
            'toyota_prius': 'Prius',
            'toyota_corolla': 'Corolla',
            'toyota_camry': 'Camry',
            'toyota_rav4': 'RAV4',
            'toyota_highlander': 'Highlander',
            'toyota_land cruiser': 'Land Cruiser',
            'toyota_hilux': 'Hilux',
            'toyota_yaris': 'Yaris',
            'toyota_auris': 'Auris',
            'toyota_avensis': 'Avensis',
            
            # Honda models
            'honda_civic': 'Civic',
            'honda_accord': 'Accord',
            'honda_cr-v': 'CR-V',
            'honda_hr-v': 'HR-V',
            'honda_jazz': 'Jazz',
            'honda_pilot': 'Pilot',
            'honda_fit': 'Jazz',  # Fit is Jazz in some markets
            
            # BMW models
            'bmw_3 series': '3 Series',
            'bmw_5 series': '5 Series',
            'bmw_x3': 'X3',
            'bmw_x5': 'X5',
            'bmw_1 series': '1 Series',
            'bmw_7 series': '7 Series',
            
            # Mercedes models
            'mercedes-benz_c class': 'C-Class',
            'mercedes-benz_e class': 'E-Class',
            'mercedes-benz_s class': 'S-Class',
            'mercedes-benz_a class': 'A-Class',
            'mercedes-benz_gle': 'GLE',
            'mercedes-benz_glc': 'GLC',
            
            # Audi models
            'audi_a3': 'A3',
            'audi_a4': 'A4',
            'audi_a6': 'A6',
            'audi_q3': 'Q3',
            'audi_q5': 'Q5',
            'audi_q7': 'Q7',
            
            # Nissan models
            'nissan_qashqai': 'Qashqai',
            'nissan_x-trail': 'X-Trail',
            'nissan_micra': 'Micra',
            'nissan_juke': 'Juke',
            'nissan_leaf': 'Leaf',
            
            # Ford models
            'ford_focus': 'Focus',
            'ford_fiesta': 'Fiesta',
            'ford_mondeo': 'Mondeo',
            'ford_kuga': 'Kuga',
            'ford_mustang': 'Mustang',
            
            # Volkswagen models
            'volkswagen_golf': 'Golf',
            'volkswagen_passat': 'Passat',
            'volkswagen_polo': 'Polo',
            'volkswagen_tiguan': 'Tiguan',
            'volkswagen_touareg': 'Touareg'
        }
    
    def get_cleaning_statistics(self, original_data: List[Dict], cleaned_data: List[Dict]) -> Dict:
        """Get cleaning statistics"""
        try:
            total_original = len(original_data)
            total_cleaned = len(cleaned_data)
            
            # Calculate field completeness
            field_completeness = {}
            if cleaned_data:
                for field in cleaned_data[0].keys():
                    non_null_count = sum(1 for record in cleaned_data if record.get(field) is not None and record.get(field) != '')
                    field_completeness[field] = round((non_null_count / total_cleaned) * 100, 1) if total_cleaned > 0 else 0
            
            # Calculate quality scores
            quality_scores = [record.get('data_quality_score', 0) for record in cleaned_data if record.get('data_quality_score')]
            avg_quality_score = round(sum(quality_scores) / len(quality_scores), 1) if quality_scores else 0
            
            return {
                'total_original_records': total_original,
                'total_cleaned_records': total_cleaned,
                'records_removed': total_original - total_cleaned,
                'cleaning_success_rate': round((total_cleaned / total_original) * 100, 1) if total_original > 0 else 0,
                'average_quality_score': avg_quality_score,
                'field_completeness': field_completeness,
                'quality_distribution': {
                    'excellent': sum(1 for score in quality_scores if score >= 90),
                    'good': sum(1 for score in quality_scores if 70 <= score < 90),
                    'fair': sum(1 for score in quality_scores if 50 <= score < 70),
                    'poor': sum(1 for score in quality_scores if score < 50)
                }
            }
            
        except Exception as e:
            logger.error(f"Error calculating cleaning statistics: {str(e)}")
            return {}
    
    def export_cleaning_report(self, statistics: Dict, output_file: str = None) -> str:
        """Export cleaning report"""
        try:
            report = []
            report.append("=" * 50)
            report.append("DATA CLEANING REPORT")
            report.append("=" * 50)
            report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report.append("")
            
            # Summary statistics
            report.append("SUMMARY STATISTICS")
            report.append("-" * 20)
            report.append(f"Original Records: {statistics.get('total_original_records', 0):,}")
            report.append(f"Cleaned Records: {statistics.get('total_cleaned_records', 0):,}")
            report.append(f"Records Removed: {statistics.get('records_removed', 0):,}")
            report.append(f"Success Rate: {statistics.get('cleaning_success_rate', 0)}%")
            report.append(f"Average Quality Score: {statistics.get('average_quality_score', 0)}")
            report.append("")
            
            # Field completeness
            if statistics.get('field_completeness'):
                report.append("FIELD COMPLETENESS")
                report.append("-" * 20)
                for field, completeness in sorted(statistics['field_completeness'].items()):
                    report.append(f"{field:20}: {completeness:6.1f}%")
                report.append("")
            
            # Quality distribution
            if statistics.get('quality_distribution'):
                report.append("QUALITY DISTRIBUTION")
                report.append("-" * 20)
                quality_dist = statistics['quality_distribution']
                report.append(f"Excellent (90%+): {quality_dist.get('excellent', 0):,}")
                report.append(f"Good (70-89%):   {quality_dist.get('good', 0):,}")
                report.append(f"Fair (50-69%):   {quality_dist.get('fair', 0):,}")
                report.append(f"Poor (<50%):     {quality_dist.get('poor', 0):,}")
            
            report_text = "\n".join(report)
            
            # Save to file if specified
            if output_file:
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(report_text)
                logger.info(f"Cleaning report saved to {output_file}")
            
            return report_text
            
        except Exception as e:
            logger.error(f"Error generating cleaning report: {str(e)}")
            return ""
    
    def validate_data_consistency(self, uk_data: List[Dict], japan_data: List[Dict]) -> Dict:
        """Validate consistency between UK and Japan data"""
        try:
            consistency_report = {
                'make_model_matches': 0,
                'year_matches': 0,
                'fuel_type_matches': 0,
                'total_comparisons': 0,
                'inconsistencies': []
            }
            
            # Create lookup for efficient comparison
            uk_lookup = {}
            for record in uk_data:
                key = f"{record.get('make', '').lower()}_{record.get('model', '').lower()}_{record.get('year', 0)}"
                if key not in uk_lookup:
                    uk_lookup[key] = []
                uk_lookup[key].append(record)
            
            # Compare Japan data against UK data
            for japan_record in japan_data:
                make = japan_record.get('make', '').lower()
                model = japan_record.get('model', '').lower()
                year = japan_record.get('year', 0)
                key = f"{make}_{model}_{year}"
                
                if key in uk_lookup:
                    consistency_report['total_comparisons'] += 1
                    
                    # Check make/model match
                    if make and model:
                        consistency_report['make_model_matches'] += 1
                    
                    # Check year match
                    if year:
                        consistency_report['year_matches'] += 1
                    
                    # Check fuel type consistency
                    japan_fuel = japan_record.get('fuel_type', '').lower()
                    uk_fuels = [r.get('fuel_type', '').lower() for r in uk_lookup[key]]
                    if japan_fuel in uk_fuels:
                        consistency_report['fuel_type_matches'] += 1
                    elif japan_fuel and uk_fuels:
                        # Record inconsistency
                        consistency_report['inconsistencies'].append({
                            'vehicle': f"{make} {model} ({year})",
                            'japan_fuel_type': japan_fuel,
                            'uk_fuel_types': list(set(uk_fuels))
                        })
            
            # Calculate consistency rates
            total = consistency_report['total_comparisons']
            if total > 0:
                consistency_report['make_model_consistency_rate'] = round((consistency_report['make_model_matches'] / total) * 100, 1)
                consistency_report['year_consistency_rate'] = round((consistency_report['year_matches'] / total) * 100, 1)
                consistency_report['fuel_type_consistency_rate'] = round((consistency_report['fuel_type_matches'] / total) * 100, 1)
            
            return consistency_report
            
        except Exception as e:
            logger.error(f"Error validating data consistency: {str(e)}")
            return {}
    
    def clean_batch(self, data_batch: List[Dict], data_type: str = 'uk') -> List[Dict]:
        """Clean a batch of data efficiently"""
        try:
            if data_type.lower() == 'uk':
                return self.clean_uk_market_data(data_batch)
            elif data_type.lower() == 'japan':
                return self.clean_japan_auction_data(data_batch)
            elif data_type.lower() == 'government':
                return self.clean_government_data(data_batch)
            else:
                logger.warning(f"Unknown data type: {data_type}")
                return []
                
        except Exception as e:
            logger.error(f"Error cleaning batch: {str(e)}")
            return []
    
    def get_data_profile(self, data: List[Dict]) -> Dict:
        """Generate data profile/summary"""
        try:
            if not data:
                return {}
            
            profile = {
                'total_records': len(data),
                'fields': list(data[0].keys()) if data else [],
                'field_statistics': {},
                'data_types': {},
                'missing_values': {},
                'unique_values': {}
            }
            
            # Analyze each field
            for field in profile['fields']:
                values = [record.get(field) for record in data]
                non_null_values = [v for v in values if v is not None and v != '']
                
                profile['field_statistics'][field] = {
                    'total_values': len(values),
                    'non_null_values': len(non_null_values),
                    'null_percentage': round(((len(values) - len(non_null_values)) / len(values)) * 100, 1) if values else 0
                }
                
                # Data type analysis
                if non_null_values:
                    sample_value = non_null_values[0]
                    if isinstance(sample_value, bool):
                        profile['data_types'][field] = 'boolean'
                    elif isinstance(sample_value, int):
                        profile['data_types'][field] = 'integer'
                    elif isinstance(sample_value, float):
                        profile['data_types'][field] = 'float'
                    elif isinstance(sample_value, str):
                        profile['data_types'][field] = 'string'
                    else:
                        profile['data_types'][field] = 'mixed'
                    
                    # Unique values (for categorical fields)
                    unique_vals = list(set(non_null_values))
                    if len(unique_vals) <= 20:  # Only show for fields with reasonable number of unique values
                        profile['unique_values'][field] = unique_vals[:10]  # Show first 10
                        profile['field_statistics'][field]['unique_count'] = len(unique_vals)
            
            return profile
            
        except Exception as e:
            logger.error(f"Error generating data profile: {str(e)}")
            return {}

# Utility functions for external use
def clean_vehicle_record(record: Dict, record_type: str = 'uk') -> Dict:
    """Clean a single vehicle record"""
    cleaner = DataCleaner()
    
    if record_type.lower() == 'uk':
        return cleaner._clean_uk_record(record)
    elif record_type.lower() == 'japan':
        return cleaner._clean_japan_record(record)
    else:
        return record

def validate_vehicle_record(record: Dict) -> List[str]:
    """Validate a single vehicle record"""
    return validate_vehicle_data(record)

def normalize_vehicle_make(make: str) -> str:
    """Normalize vehicle make"""
    cleaner = DataCleaner()
    return cleaner._normalize_make(make)

def normalize_vehicle_model(model: str, make: str = '') -> str:
    """Normalize vehicle model"""
    cleaner = DataCleaner()
    return cleaner._normalize_model(model, make)

def calculate_record_quality_score(record: Dict) -> float:
    """Calculate quality score for a record"""
    cleaner = DataCleaner()
    return cleaner._calculate_quality_score(record)