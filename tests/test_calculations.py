import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch
import asyncio

from src.utils.helpers import (
    normalize_text, extract_year_from_text, extract_mileage_from_text,
    extract_price_from_text, validate_vehicle_data, clean_vehicle_data,
    calculate_age_from_year, format_currency, generate_cache_key
)
from src.data_processing.profitability_calculator import ProfitabilityCalculator

class TestHelperFunctions:
    
    def test_normalize_text(self):
        """Test text normalization"""
        assert normalize_text("Toyota Prius") == "toyota prius"
        assert normalize_text("  BMW   3-Series  ") == "bmw 3-series"
        assert normalize_text("Volkswagen") == "volkswagen"
        assert normalize_text("") == ""
        assert normalize_text(None) == ""
        assert normalize_text("Citroën") == "citroen"  # Accent removal
        assert normalize_text("MERCEDES-BENZ") == "mercedes-benz"
    
    def test_extract_year_from_text(self):
        """Test year extraction"""
        assert extract_year_from_text("2020 Toyota Prius") == 2020
        assert extract_year_from_text("Honda Civic 2018") == 2018
        assert extract_year_from_text("BMW 3 Series (2019)") == 2019
        assert extract_year_from_text("Old car from 1975") == 1975
        assert extract_year_from_text("No year here") is None
        assert extract_year_from_text("2050 future car") is None  # Future year
        assert extract_year_from_text("1970 vintage") is None  # Too old
        assert extract_year_from_text("") is None
        assert extract_year_from_text(None) is None
    
    def test_extract_mileage_from_text(self):
        """Test mileage extraction"""
        assert extract_mileage_from_text("50,000 miles") == 50000
        assert extract_mileage_from_text("75k miles") == 75000
        assert extract_mileage_from_text("100,000 km") == 62137  # Converted to miles
        assert extract_mileage_from_text("25K mi") == 25000
        assert extract_mileage_from_text("No mileage") is None
        assert extract_mileage_from_text("1,000,000 miles") is None  # Too high
        assert extract_mileage_from_text("15.5k miles") == 15500
        assert extract_mileage_from_text("") is None
        assert extract_mileage_from_text(None) is None
    
    def test_extract_price_from_text(self):
        """Test price extraction"""
        assert extract_price_from_text("£25,000") == 25000.0
        assert extract_price_from_text("$30,000.50") == 30000.50
        assert extract_price_from_text("15000 pounds") == 15000.0
        assert extract_price_from_text("€20,000") == 20000.0
        assert extract_price_from_text("No price") is None
        assert extract_price_from_text("£50") is None  # Too low
        assert extract_price_from_text("£1,000,000") is None  # Too high
        assert extract_price_from_text("") is None
        assert extract_price_from_text(None) is None
    
    def test_validate_vehicle_data(self):
        """Test vehicle data validation"""
        valid_data = {
            'make': 'Toyota',
            'model': 'Prius',
            'year': 2020,
            'price': 25000,
            'mileage': 50000
        }
        
        invalid_data = {
            'make': '',
            'year': 2050,
            'price': -1000,
            'mileage': 2000000
        }
        
        empty_data = {}
        
        assert validate_vehicle_data(valid_data) == []
        
        errors = validate_vehicle_data(invalid_data)
        assert len(errors) > 0
        assert any('make' in error for error in errors)
        assert any('year' in error for error in errors)
        assert any('price' in error for error in errors)
        assert any('mileage' in error for error in errors)
        
        empty_errors = validate_vehicle_data(empty_data)
        assert len(empty_errors) >= 3  # Missing make, model, year
    
    def test_clean_vehicle_data(self):
        """Test vehicle data cleaning"""
        dirty_data = {
            'make': '  TOYOTA  ',
            'model': 'Prius Hybrid',
            'year': '2020',
            'price': '25000.00',
            'mileage': '50000',
            'fuel_type': 'HYBRID',
            'extra_field': 'should_remain'
        }
        
        clean_data = clean_vehicle_data(dirty_data)
        
        assert clean_data['make'] == 'toyota'
        assert clean_data['model'] == 'prius hybrid'
        assert clean_data['year'] == 2020
        assert clean_data['price'] == 25000.0
        assert clean_data['mileage'] == 50000
        assert clean_data['fuel_type'] == 'hybrid'
        assert clean_data['extra_field'] == 'should_remain'
    
    def test_calculate_age_from_year(self):
        """Test age calculation"""
        current_year = datetime.now().year
        assert calculate_age_from_year(current_year) == 0
        assert calculate_age_from_year(current_year - 10) == 10
        assert calculate_age_from_year(None) == 0
        assert calculate_age_from_year(0) == 0
    
    def test_format_currency(self):
        """Test currency formatting"""
        assert format_currency(25000, 'GBP') == '£25,000.00'
        assert format_currency(25000, 'USD') == '$25,000.00'
        assert format_currency(2500000, 'JPY') == '¥2,500,000'
        assert format_currency(None) == 'N/A'
        assert format_currency(1234.56, 'EUR') == '€1,234.56'
    
    def test_generate_cache_key(self):
        """Test cache key generation"""
        key1 = generate_cache_key('toyota', 'prius', 2020)
        key2 = generate_cache_key('toyota', 'prius', 2020)
        key3 = generate_cache_key('honda', 'civic', 2020)
        
        assert key1 == key2  # Same inputs should generate same key
        assert key1 != key3  # Different inputs should generate different keys
        assert len(key1) == 32  # MD5 hash length

class TestProfitabilityCalculations:
    
    def setup_method(self):
        self.calc = ProfitabilityCalculator()
        self.calc.db = Mock()
    
    @pytest.mark.asyncio
    async def test_calculate_individual_profit(self):
        """Test individual profit calculation"""
        match_data = {
            'make': 'Toyota',
            'model': 'Prius',
            'year': 2020,
            'fuel_type': 'hybrid',
            'avg_uk_price': 25000,
            'avg_landed_cost': 20000,
            'min_landed_cost': 19000,
            'max_uk_price': 27000,
            'min_uk_price': 23000,
            'avg_days_listed': 30,
            'uk_listing_count': 15,
            'japan_auction_count': 8,
            'avg_condition_grade': 4.0
        }
        
        # Mock the risk and demand score calculations
        with patch.object(self.calc, '_calculate_risk_score', return_value=20.0):
            with patch.object(self.calc, '_calculate_demand_score', return_value=75.0):
                result = await self.calc._calculate_individual_profit(match_data)
        
        assert result is not None
        assert result['gross_profit'] == 5000
        assert result['profit_margin_percent'] == 20.0
        assert result['roi_percent'] == 25.0
        assert result['best_case_profit'] == 6000  # 25000 - 19000
        assert 'overall_score' in result
        assert 'annualized_roi_percent' in result
    
    @pytest.mark.asyncio
    async def test_calculate_individual_profit_invalid_data(self):
        """Test profit calculation with invalid data"""
        invalid_data = {
            'make': 'Toyota',
            'model': 'Prius',
            'year': 2020,
            'fuel_type': 'hybrid',
            'avg_uk_price': 0,  # Invalid
            'avg_landed_cost': 20000,
            'avg_days_listed': 30
        }
        
        result = await self.calc._calculate_individual_profit(invalid_data)
        assert result is None
    
    @pytest.mark.asyncio
    async def test_calculate_risk_score(self):
        """Test risk score calculation"""
        match_data = {
            'max_uk_price': 30000,
            'min_uk_price': 20000,
            'avg_uk_price': 25000,
            'uk_listing_count': 15,
            'year': 2020,
            'avg_condition_grade': 4.0
        }
        
        risk_score = await self.calc._calculate_risk_score(match_data)
        
        assert 0 <= risk_score <= 100
        assert isinstance(risk_score, float)
        
        # Test with high volatility
        high_volatility_data = match_data.copy()
        high_volatility_data.update({
            'max_uk_price': 40000,
            'min_uk_price': 15000,
            'uk_listing_count': 3,  # Low liquidity
            'year': 2005,  # Older vehicle
            'avg_condition_grade': 2.5  # Poor condition
        })
        
        high_risk_score = await self.calc._calculate_risk_score(high_volatility_data)
        assert high_risk_score > risk_score
    
    @pytest.mark.asyncio
    async def test_calculate_demand_score(self):
        """Test demand score calculation"""
        match_data = {
            'uk_listing_count': 25,
            'avg_days_listed': 20,
            'fuel_type': 'hybrid'
        }
        
        demand_score = await self.calc._calculate_demand_score(match_data)
        
        assert 0 <= demand_score <= 100
        assert isinstance(demand_score, float)
        
        # Test with low demand indicators
        low_demand_data = {
            'uk_listing_count': 3,
            'avg_days_listed': 90,
            'fuel_type': 'diesel'
        }
        
        low_demand_score = await self.calc._calculate_demand_score(low_demand_data)
        assert low_demand_score < demand_score
    
    def test_calculate_overall_score(self):
        """Test overall score calculation"""
        # High performing vehicle
        high_score = self.calc._calculate_overall_score(
            profit_margin=25.0,
            roi=30.0,
            risk_score=20.0,
            demand_score=80.0,
            days_listed=15.0
        )
        
        # Low performing vehicle
        low_score = self.calc._calculate_overall_score(
            profit_margin=5.0,
            roi=8.0,
            risk_score=70.0,
            demand_score=30.0,
            days_listed=60.0
        )
        
        assert 0 <= high_score <= 100
        assert 0 <= low_score <= 100
        assert high_score > low_score