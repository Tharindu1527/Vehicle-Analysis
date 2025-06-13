"""
Exchange Rate API Integration
Fetches real-time exchange rates for cost calculations
"""
import aiohttp
import asyncio
from typing import Dict, Optional
from datetime import datetime

from src.utils.logger import setup_logger
from src.utils.config import Config

logger = setup_logger(__name__)

class ExchangeRateAPI:
    def __init__(self):
        self.config = Config()
        self.session = None
        
        # Exchange rate API endpoints
        self.primary_api = "https://api.exchangerate-api.com/v4/latest"
        self.backup_api = "https://api.fixer.io/latest"
        self.xe_api = "https://xecdapi.xe.com/v1/convert_from"
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_current_rates(self, base_currency: str = 'JPY') -> Dict:
        """Get current exchange rates"""
        try:
            # Try primary API first
            rates = await self._fetch_from_exchangerate_api(base_currency)
            if rates:
                return rates
                
            # Try backup API
            rates = await self._fetch_from_fixer_api(base_currency)
            if rates:
                return rates
                
            # Try XE API as last resort
            rates = await self._fetch_from_xe_api(base_currency)
            if rates:
                return rates
                
            # Return fallback rates if all APIs fail
            return self._get_fallback_rates()
            
        except Exception as e:
            logger.error(f"Error fetching exchange rates: {str(e)}")
            return self._get_fallback_rates()

    async def _fetch_from_exchangerate_api(self, base_currency: str) -> Optional[Dict]:
        """Fetch from ExchangeRate-API"""
        try:
            async with self.session.get(f"{self.primary_api}/{base_currency}") as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'GBP': data['rates'].get('GBP', 0.0055),
                        'USD': data['rates'].get('USD', 0.0067),
                        'EUR': data['rates'].get('EUR', 0.0062),
                        'timestamp': datetime.now().isoformat(),
                        'source': 'exchangerate-api'
                    }
        except Exception as e:
            logger.warning(f"ExchangeRate-API failed: {str(e)}")
        return None

    async def _fetch_from_fixer_api(self, base_currency: str) -> Optional[Dict]:
        """Fetch from Fixer.io API"""
        try:
            headers = {'access_key': self.config.get('FIXER_API_KEY')}
            params = {'base': base_currency, 'symbols': 'GBP,USD,EUR'}
            
            async with self.session.get(self.backup_api, headers=headers, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('success'):
                        return {
                            'GBP': data['rates'].get('GBP', 0.0055),
                            'USD': data['rates'].get('USD', 0.0067),
                            'EUR': data['rates'].get('EUR', 0.0062),
                            'timestamp': datetime.now().isoformat(),
                            'source': 'fixer'
                        }
        except Exception as e:
            logger.warning(f"Fixer API failed: {str(e)}")
        return None

    async def _fetch_from_xe_api(self, base_currency: str) -> Optional[Dict]:
        """Fetch from XE API"""
        try:
            headers = {
                'Authorization': f'Basic {self.config.get("XE_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            currencies = ['GBP', 'USD', 'EUR']
            rates = {}
            
            for currency in currencies:
                params = {'from': base_currency, 'to': currency, 'amount': 1}
                async with self.session.get(self.xe_api, headers=headers, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        rates[currency] = data.get('to', [{}])[0].get('mid', 0)
            
            if rates:
                return {
                    **rates,
                    'timestamp': datetime.now().isoformat(),
                    'source': 'xe'
                }
                
        except Exception as e:
            logger.warning(f"XE API failed: {str(e)}")
        return None

    def _get_fallback_rates(self) -> Dict:
        """Fallback exchange rates if all APIs fail"""
        return {
            'GBP': 0.0055,  # Approximate JPY to GBP
            'USD': 0.0067,  # Approximate JPY to USD  
            'EUR': 0.0062,  # Approximate JPY to EUR
            'timestamp': datetime.now().isoformat(),
            'source': 'fallback'
        }

    async def convert_currency(self, amount: float, from_currency: str, to_currency: str) -> float:
        """Convert amount from one currency to another"""
        try:
            rates = await self.get_current_rates(from_currency)
            rate = rates.get(to_currency, 0)
            return amount * rate if rate > 0 else 0
        except Exception as e:
            logger.error(f"Error converting currency: {str(e)}")
            return 0