import pytest
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
import asyncio

from src.data_processing.profitability_calculator import ProfitabilityCalculator
from src.data_processing.scoring_engine import ScoringEngine
from src.data_processing.data_cleaner import DataCleaner

class TestDataCleaner:
    
    def setup_method(self):
        self.cleaner = DataCleaner()
    
    def test_clean_uk_market_data(self):
        """Test UK market data cleaning"""
        raw_data = [
            {
                'source': 'AutoTrader',
                'make': '  TOYOTA  ',
                'model': 'prius',
                'year': '2020',
                'price': '£25,000',
                'mileage': '50k miles',
                'fuel_type': 'HYBRID',
                'location': 'London, UK',
                'seller_type': 'dealer',
                'listing_date': '2024-01-01',
                'url': 'https://autotrader.co.uk/test?utm_source=test'
            },
            {
                'source': 'Motors',
                'make': 'Honda',
                'model': 'Civic',
                'year': 2019,
                'price': 20000,
                'mileage': 30000,
                'fuel_type': 'petrol'
            }
        ]
        
        cleaned_data = self.cleaner.clean_uk_market_data(raw_data)
        
        assert len(cleaned_data) == 2
        
        # Check first record
        first_record = cleaned_data[0]
        assert first_record['make'] == 'Toyota'
        assert first_record['model'] == 'Prius'
        assert first_record['year'] == 2020
        assert first_record['price'] == 25000.0
        assert first_record['mileage'] == 50000
        assert first_record['fuel_type'] == 'hybrid'
        assert 'utm_source' not in first_record['url']  # Tracking removed
        assert first_record['data_quality_score'] > 0
    
    def test_clean_japan_auction_data(self):
        """Test Japan auction data cleaning"""
        raw_data = [
            {
                'source': 'USS',
                'make': 'toyota',
                'model': 'PRIUS',
                'year': 2020,
                'hammer_price': 2500000,
                'mileage': 25000,
                'condition_grade': '4',
                'exterior_grade': 'A',
                'interior_grade': 'A',
                'auction_date': '2024-01-01',
                'auction_house': 'uss tokyo',
                'fuel_type': 'hybrid'
            }
        ]
        
        cleaned_data = self.cleaner.clean_japan_auction_data(raw_data)
        
        assert len(cleaned_data) == 1
        
        record = cleaned_data[0]
        assert record['make'] == 'Toyota'
        assert record['model'] == 'Prius'
        assert record['hammer_price'] == 2500000
        assert record['condition_grade'] == '4'
        assert record['auction_house'] == 'USS Tokyo'
    
    def test_normalize_make(self):
        """Test make normalization"""
        assert self.cleaner._normalize_make('toyota') == 'Toyota'
        assert self.cleaner._normalize_make('BMW') == 'BMW'
        assert self.cleaner._normalize_make('  vw  ') == 'Volkswagen'
        assert self.cleaner._normalize_make('merc') == 'Mercedes-Benz'
        assert self.cleaner._normalize_make('') == ''
    
    def test_normalize_fuel_type(self):
        """Test fuel type normalization"""
        assert self.cleaner._normalize_fuel_type('gas') == 'petrol'
        assert self.cleaner._normalize_fuel_type('HYBRID') == 'hybrid'
        assert self.cleaner._normalize_fuel_type('EV') == 'electric'
        assert self.cleaner._normalize_fuel_type('diesel') == 'diesel'
    
    def test_extract_and_validate_year(self):
        """Test year extraction and validation"""
        assert self.cleaner._extract_and_validate_year(2020) == 2020
        assert self.cleaner._extract_and_validate_year('2020') == 2020
        assert self.cleaner._extract_and_validate_year('2050') is None  # Future
        assert self.cleaner._extract_and_validate_year('1970') is None  # Too old
        assert self.cleaner._extract_and_validate_year('invalid') is None
    
    def test_validate_hammer_price(self):
        """Test hammer price validation"""
        assert self.cleaner._validate_hammer_price(2500000) == 2500000
        assert self.cleaner._validate_hammer_price('2500000') == 2500000
        assert self.cleaner._validate_hammer_price(30000) is None  # Too low
        assert self.cleaner._validate_hammer_price(60000000) is None  # Too high
        assert self.cleaner._validate_hammer_price('invalid') is None
    
    def test_calculate_quality_score(self):
        """Test data quality score calculation"""
        high_quality_record = {
            'make': 'Toyota',
            'model': 'Prius',
            'year': 2020,
            'price': 25000,
            'mileage': 50000,
            'url': 'https://example.com'
        }
        
        low_quality_record = {
            'make': 'Toyota',
            'model': '',
            'year': None,
            'price': None
        }
        
        high_score = self.cleaner._calculate_quality_score(high_quality_record)
        low_score = self.cleaner._calculate_quality_score(low_quality_record)
        
        assert high_score > low_score
        assert 0 <= high_score <= 100
        assert 0 <= low_score <= 100

class TestScoringEngine:
    
    def setup_method(self):
        self.engine = ScoringEngine()
        self.engine.db = Mock()
        self.engine.profitability_calc = Mock()
    
    def test_encode_trend(self):
        """Test trend encoding"""
        assert self.engine._encode_trend('growing') == 1.0
        assert self.engine._encode_trend('stable') == 0.5
        assert self.engine._encode_trend('declining') == 0.0
        assert self.engine._encode_trend('unknown') == 0.5
    
    def test_encode_competition(self):
        """Test competition encoding"""
        assert self.engine._encode_competition('low') == 1.0
        assert self.engine._encode_competition('medium') == 0.5
        assert self.engine._encode_competition('high') == 0.0
    
    def test_encode_volatility(self):
        """Test volatility encoding"""
        assert self.engine._encode_volatility('low') == 1.0
        assert self.engine._encode_volatility('medium') == 0.5
        assert self.engine._encode_volatility('high') == 0.0
    
    def test_encode_supply_risk(self):
        """Test supply risk encoding"""
        assert self.engine._encode_supply_risk('low') == 1.0
        assert self.engine._encode_supply_risk('medium') == 0.5
        assert self.engine._encode_supply_risk('high') == 0.0
    
    def test_calculate_ml_score(self):
        """Test ML score calculation"""
        # High-performing vehicle features
        high_features = [
            25.0,  # profit_margin_percent
            30.0,  # roi_percent
            20.0,  # avg_days_to_sell
            15.0,  # uk_listing_count
            8.0,   # japan_auction_count
            25.0,  # risk_score
            80.0,  # demand_score
            1.0,   # ulez_compliant
            40.0,  # regional_concentration
            1.0,   # registration_trend (growing)
            1.0,   # competition_level (low)
            3.0,   # vehicle_age
            1.0,   # market_volatility (low)
            1.0    # supply_chain_risk (low)
        ]
        
        # Low-performing vehicle features
        low_features = [
            5.0,   # profit_margin_percent
            8.0,   # roi_percent
            60.0,  # avg_days_to_sell
            3.0,   # uk_listing_count
            2.0,   # japan_auction_count
            75.0,  # risk_score
            30.0,  # demand_score
            0.0,   # ulez_compliant
            10.0,  # regional_concentration
            0.0,   # registration_trend (declining)
            0.0,   # competition_level (high)
            15.0,  # vehicle_age
            0.0,   # market_volatility (high)
            0.0    # supply_chain_risk (high)
        ]
        
        high_score = self.engine._calculate_ml_score(high_features)
        low_score = self.engine._calculate_ml_score(low_features)
        
        assert 0 <= high_score <= 100
        assert 0 <= low_score <= 100
        assert high_score > low_score
    
    def test_calculate_final_score(self):
        """Test final score calculation"""
        high_result = {
            'overall_score': 80.0,
            'ml_score': 85.0,
            'ulez_compliant': {'ulez_compliant': True},
            'registration_trend': {'trend': 'growing'},
            'competition_analysis': {'competition_level': 'low'},
            'market_volatility': {'volatility': 'low'},
            'supply_chain_risk': {'supply_risk': 'low'}
        }
        
        low_result = {
            'overall_score': 40.0,
            'ml_score': 35.0,
            'ulez_compliant': {'ulez_compliant': False},
            'registration_trend': {'trend': 'declining'},
            'competition_analysis': {'competition_level': 'high'},
            'market_volatility': {'volatility': 'high'},
            'supply_chain_risk': {'supply_risk': 'high'}
        }
        
        high_score = self.engine._calculate_final_score(high_result)
        low_score = self.engine._calculate_final_score(low_result)
        
        assert 0 <= high_score <= 100
        assert 0 <= low_score <= 100
        assert high_score > low_score
    
    def test_calculate_confidence_interval(self):
        """Test confidence interval calculation"""
        high_confidence_result = {
            'final_recommendation_score': 75.0,
            'uk_listing_count': 20,
            'japan_auction_count': 10,
            'registration_trend': {'confidence': 'high'},
            'market_volatility': {'volatility': 'low'},
            'supply_chain_risk': {'supply_risk': 'low'}
        }
        
        low_confidence_result = {
            'final_recommendation_score': 75.0,
            'uk_listing_count': 2,
            'japan_auction_count': 1,
            'registration_trend': {'confidence': 'low'},
            'market_volatility': {'volatility': 'high'},
            'supply_chain_risk': {'supply_risk': 'high'}
        }
        
        high_ci = self.engine._calculate_confidence_interval(high_confidence_result)
        low_ci = self.engine._calculate_confidence_interval(low_confidence_result)
        
        assert high_ci['confidence_level'] > low_ci['confidence_level']
        assert high_ci['margin_of_error'] < low_ci['margin_of_error']
        assert high_ci['lower_bound'] <= 75.0 <= high_ci['upper_bound']
    
    def test_generate_action_items(self):
        """Test action item generation"""
        result = {
            'profit_margin_percent': 30.0,
            'avg_days_to_sell': 10.0,
            'competition_analysis': {'competition_level': 'low'},
            'ulez_compliant': {'ulez_compliant': True},
            'seasonal_factors': {'seasonal_pattern': 'winter_peak'},
            'supply_chain_risk': {'supply_risk': 'low'}
        }
        
        actions = self.engine._generate_action_items(result)
        
        assert len(actions) > 0
        assert any('high profit potential' in action.lower() for action in actions)
        assert any('fast-moving' in action.lower() for action in actions)
        assert any('low competition' in action.lower() for action in actions)
    
    def test_generate_risk_warnings(self):
        """Test risk warning generation"""
        high_risk_result = {
            'market_volatility': {'volatility': 'high'},
            'supply_chain_risk': {'supply_risk': 'high'},
            'competition_analysis': {'competition_level': 'high'},
            'registration_trend': {'trend': 'declining'},
            'risk_score': 75.0
        }
        
        warnings = self.engine._generate_risk_warnings(high_risk_result)
        
        assert len(warnings) > 0
        assert any('volatility' in warning.lower() for warning in warnings)
        assert any('supply' in warning.lower() for warning in warnings)
        assert any('competition' in warning.lower() for warning in warnings)
    
    def test_calculate_confidence_level(self):
        """Test confidence level calculation"""
        high_confidence_result = {
            'uk_listing_count': 15,
            'japan_auction_count': 8,
            'registration_trend': {'confidence': 'high'},
            'seasonal_factors': {'seasonal_pattern': 'winter_peak'},
            'competition_analysis': {'competitor_count': 5},
            'market_volatility': {'volatility': 'low'}
        }
        
        low_confidence_result = {
            'uk_listing_count': 2,
            'japan_auction_count': 1,
            'registration_trend': {'confidence': 'low'},
            'seasonal_factors': {'seasonal_pattern': 'unknown'},
            'competition_analysis': {'competitor_count': 0},
            'market_volatility': {'volatility': 'unknown'}
        }
        
        high_confidence = self.engine._calculate_confidence_level(high_confidence_result)
        low_confidence = self.engine._calculate_confidence_level(low_confidence_result)
        
        assert high_confidence == 'High'
        assert low_confidence == 'Low'

class TestIntegrationScenarios:
    
    @pytest.mark.asyncio
    async def test_complete_analysis_workflow(self):
        """Test complete analysis workflow"""
        # Mock data
        uk_data = [
            {
                'make': 'Toyota',
                'model': 'Prius',
                'year': 2020,
                'price': 25000,
                'fuel_type': 'hybrid'
            }
        ]
        
        japan_data = [
            {
                'make': 'Toyota',
                'model': 'Prius',
                'year': 2020,
                'hammer_price': 2000000,
                'fuel_type': 'hybrid'
            }
        ]
        
        gov_data = [
            {
                'make': 'Toyota',
                'model': 'Prius',
                'registration_count': 150,
                'region': 'London'
            }
        ]
        
        # Create engine with mocked dependencies
        engine = ScoringEngine()
        engine.db = Mock()
        engine.profitability_calc = Mock()
        
        # Mock profitability calculation
        mock_profitability_results = [
            {
                'make': 'Toyota',
                'model': 'Prius',
                'year': 2020,
                'fuel_type': 'hybrid',
                'profit_margin_percent': 20.0,
                'roi_percent': 25.0,
                'risk_score': 30.0,
                'demand_score': 75.0,
                'overall_score': 70.0
            }
        ]
        
        engine.profitability_calc.calculate_profitability_matrix.return_value = mock_profitability_results
        
        # Mock database operations
        engine.db.execute = AsyncMock()
        engine.db.commit = AsyncMock()
        
        # Run analysis
        with patch.object(engine, '_enhance_with_market_intelligence', side_effect=lambda x, y: x):
            results = await engine.analyze_profitability(uk_data, japan_data, gov_data)
        
        assert len(results) == 1
        result = results[0]
        assert 'ml_score' in result
        assert 'final_recommendation_score' in result
        assert 'recommendation_category' in result

if __name__ == '__main__':
    pytest.main([__file__])