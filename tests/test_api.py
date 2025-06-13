import pytest
import asyncio
from unittest.mock import Mock, patch
import aiohttp

from src.api.uk_market_api import UKMarketAPI
from src.api.japan_auction_api import JapanAuctionAPI
from src.api.government_data_api import GovernmentDataAPI
from src.api.exchange_rate_api import ExchangeRateAPI

class TestUKMarketAPI:
    
    @pytest.mark.asyncio
    async def test_vehicle_listings_success(self):
        """Test successful vehicle listings retrieval"""
        with patch('aiohttp.ClientSession.get') as mock_get:
            # Mock response
            mock_response = Mock()
            mock_response.status = 200
            mock_response.json = asyncio.coroutine(lambda: {
                'vehicles': [
                    {
                        'make': 'Toyota',
                        'model': 'Prius',
                        'year': 2020,
                        'price': 25000,
                        'mileage': 30000
                    }
                ]
            })
            mock_get.return_value.__aenter__.return_value = mock_response
            
            async with UKMarketAPI() as api:
                listings = await api.get_vehicle_listings('Toyota', 'Prius')
                
            assert len(listings) == 1
            assert listings[0]['make'] == 'Toyota'
            assert listings[0]['model'] == 'Prius'
    
    @pytest.mark.asyncio
    async def test_vehicle_listings_api_error(self):
        """Test handling of API errors"""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = Mock()
            mock_response.status = 500
            mock_get.return_value.__aenter__.return_value = mock_response
            
            async with UKMarketAPI() as api:
                listings = await api.get_vehicle_listings('Toyota', 'Prius')
                
            assert listings == []

class TestJapanAuctionAPI:
    
    @pytest.mark.asyncio
    async def test_auction_results_success(self):
        """Test successful auction results retrieval"""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = Mock()
            mock_response.status = 200
            mock_response.json = asyncio.coroutine(lambda: {
                'results': [
                    {
                        'maker': 'Toyota',
                        'model': 'Prius',
                        'year': 2020,
                        'hammer_price': 2500000,  # JPY
                        'grade': '4'
                    }
                ]
            })
            mock_get.return_value.__aenter__.return_value = mock_response
            
            async with JapanAuctionAPI() as api:
                results = await api.get_auction_results('Toyota', 'Prius')
                
            assert len(results) == 1
            assert results[0]['make'] == 'Toyota'
            assert results[0]['hammer_price'] == 2500000
    
    @pytest.mark.asyncio
    async def test_landed_cost_calculation(self):
        """Test landed cost calculation"""
        api = JapanAuctionAPI()
        
        with patch.object(api, '_get_exchange_rate', return_value=0.0055):
            vehicle_data = {'category': 'SUV', 'year': 2020}
            costs = await api.calculate_landed_cost(2500000, vehicle_data)
            
            assert 'total_landed_cost_gbp' in costs
            assert costs['total_landed_cost_gbp'] > 0
            assert 'hammer_price' in costs
            assert 'uk_vat' in costs

class TestExchangeRateAPI:
    
    @pytest.mark.asyncio
    async def test_exchange_rates_success(self):
        """Test successful exchange rate retrieval"""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = Mock()
            mock_response.status = 200
            mock_response.json = asyncio.coroutine(lambda: {
                'rates': {
                    'GBP': 0.0055,
                    'USD': 0.0067,
                    'EUR': 0.0062
                }
            })
            mock_get.return_value.__aenter__.return_value = mock_response
            
            async with ExchangeRateAPI() as api:
                rates = await api.get_current_rates('JPY')
                
            assert 'GBP' in rates
            assert rates['GBP'] == 0.0055
            assert 'source' in rates
    
    @pytest.mark.asyncio
    async def test_currency_conversion(self):
        """Test currency conversion"""
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = Mock()
            mock_response.status = 200
            mock_response.json = asyncio.coroutine(lambda: {
                'rates': {'GBP': 0.0055}
            })
            mock_get.return_value.__aenter__.return_value = mock_response
            
            async with ExchangeRateAPI() as api:
                gbp_amount = await api.convert_currency(1000000, 'JPY', 'GBP')
                
            assert gbp_amount == 5500.0