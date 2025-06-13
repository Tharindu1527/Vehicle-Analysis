"""
UK Market Data API Integration
Uses AutoTrader, Motors.co.uk APIs and web-based APIs
"""
import aiohttp
import asyncio
from typing import List, Dict, Optional
from datetime import datetime
import json

from utils.logger import setup_logger
from utils.config import Config

logger = setup_logger(__name__)

class UKMarketAPI:
    def __init__(self):
        self.config = Config()
        self.session = None
        
        # API endpoints (replace with actual API keys and endpoints)
        self.autotrader_api = "https://api.autotrader.co.uk/v1"
        self.motors_api = "https://api.motors.co.uk/v2"
        self.cargurus_api = "https://api.cargurus.co.uk/v1"
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_vehicle_listings(self, make: str = None, model: str = None, 
                                 year_from: int = None, year_to: int = None) -> List[Dict]:
        """Fetch vehicle listings from UK platforms"""
        listings = []
        
        try:
            # AutoTrader API call
            autotrader_data = await self._fetch_autotrader_listings(make, model, year_from, year_to)
            listings.extend(autotrader_data)
            
            # Motors.co.uk API call
            motors_data = await self._fetch_motors_listings(make, model, year_from, year_to)
            listings.extend(motors_data)
            
            # CarGurus API call
            cargurus_data = await self._fetch_cargurus_listings(make, model, year_from, year_to)
            listings.extend(cargurus_data)
            
            logger.info(f"Collected {len(listings)} listings from UK platforms")
            return listings
            
        except Exception as e:
            logger.error(f"Error fetching UK market data: {str(e)}")
            return []

    async def _fetch_autotrader_listings(self, make, model, year_from, year_to) -> List[Dict]:
        """Fetch from AutoTrader API"""
        headers = {
            'Authorization': f'Bearer {self.config.get("AUTOTRADER_API_KEY")}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'make': make,
            'model': model,
            'yearFrom': year_from,
            'yearTo': year_to,
            'pageSize': 100
        }
        
        try:
            async with self.session.get(
                f"{self.autotrader_api}/vehicles/search",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_autotrader_response(data)
                else:
                    logger.warning(f"AutoTrader API returned status {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error calling AutoTrader API: {str(e)}")
            return []

    async def _fetch_motors_listings(self, make, model, year_from, year_to) -> List[Dict]:
        """Fetch from Motors.co.uk API"""
        headers = {
            'X-API-Key': self.config.get("MOTORS_API_KEY"),
            'Content-Type': 'application/json'
        }
        
        params = {
            'make': make,
            'model': model,
            'year_min': year_from,
            'year_max': year_to,
            'limit': 100
        }
        
        try:
            async with self.session.get(
                f"{self.motors_api}/adverts/search",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_motors_response(data)
                else:
                    logger.warning(f"Motors API returned status {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error calling Motors API: {str(e)}")
            return []

    async def _fetch_cargurus_listings(self, make, model, year_from, year_to) -> List[Dict]:
        """Fetch from CarGurus API"""
        headers = {
            'Authorization': f'Bearer {self.config.get("CARGURUS_API_KEY")}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'make': make,
            'model': model,
            'minYear': year_from,
            'maxYear': year_to,
            'resultsPerPage': 100
        }
        
        try:
            async with self.session.get(
                f"{self.cargurus_api}/listings",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_cargurus_response(data)
                else:
                    logger.warning(f"CarGurus API returned status {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error calling CarGurus API: {str(e)}")
            return []

    def _parse_autotrader_response(self, data: Dict) -> List[Dict]:
        """Parse AutoTrader API response"""
        listings = []
        
        for item in data.get('vehicles', []):
            listing = {
                'source': 'autotrader',
                'make': item.get('make'),
                'model': item.get('model'),
                'year': item.get('year'),
                'mileage': item.get('mileage'),
                'price': item.get('price'),
                'fuel_type': item.get('fuelType'),
                'location': item.get('location'),
                'listing_date': item.get('dateAdded'),
                'url': item.get('url'),
                'seller_type': item.get('sellerType'),
                'days_listed': self._calculate_days_listed(item.get('dateAdded'))
            }
            listings.append(listing)
            
        return listings

    def _parse_motors_response(self, data: Dict) -> List[Dict]:
        """Parse Motors.co.uk API response"""
        listings = []
        
        for item in data.get('results', []):
            listing = {
                'source': 'motors',
                'make': item.get('make'),
                'model': item.get('model'),
                'year': item.get('year'),
                'mileage': item.get('mileage'),
                'price': item.get('price'),
                'fuel_type': item.get('fuel_type'),
                'location': item.get('location'),
                'listing_date': item.get('created_date'),
                'url': item.get('advert_url'),
                'seller_type': item.get('seller_type'),
                'days_listed': self._calculate_days_listed(item.get('created_date'))
            }
            listings.append(listing)
            
        return listings

    def _parse_cargurus_response(self, data: Dict) -> List[Dict]:
        """Parse CarGurus API response"""
        listings = []
        
        for item in data.get('listings', []):
            listing = {
                'source': 'cargurus',
                'make': item.get('make'),
                'model': item.get('model'),
                'year': item.get('year'),
                'mileage': item.get('mileage'),
                'price': item.get('askingPrice'),
                'fuel_type': item.get('fuelType'),
                'location': item.get('dealerLocation'),
                'listing_date': item.get('listingDate'),
                'url': item.get('listingUrl'),
                'seller_type': 'dealer',
                'days_listed': self._calculate_days_listed(item.get('listingDate'))
            }
            listings.append(listing)
            
        return listings

    def _calculate_days_listed(self, listing_date: str) -> int:
        """Calculate days since listing was posted"""
        try:
            if not listing_date:
                return 0
            
            listing_dt = datetime.fromisoformat(listing_date.replace('Z', '+00:00'))
            now = datetime.now()
            return (now - listing_dt).days
        except:
            return 0

    async def get_popular_models(self, limit: int = 50) -> List[Dict]:
        """Get most popular vehicle models in UK"""
        try:
            # This would call a trending/popular vehicles endpoint
            headers = {
                'Authorization': f'Bearer {self.config.get("AUTOTRADER_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            async with self.session.get(
                f"{self.autotrader_api}/vehicles/trending",
                headers=headers,
                params={'limit': limit}
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('trending_models', [])
                    
        except Exception as e:
            logger.error(f"Error fetching popular models: {str(e)}")
            
        return []

    async def get_price_history(self, make: str, model: str, year: int) -> List[Dict]:
        """Get price history for specific vehicle"""
        try:
            headers = {
                'Authorization': f'Bearer {self.config.get("AUTOTRADER_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'make': make,
                'model': model,
                'year': year
            }
            
            async with self.session.get(
                f"{self.autotrader_api}/vehicles/price-history",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('price_history', [])
                    
        except Exception as e:
            logger.error(f"Error fetching price history: {str(e)}")
            
        return []