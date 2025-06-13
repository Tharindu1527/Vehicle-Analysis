"""
Main data collection orchestrator
Coordinates data collection from all API sources
"""
import asyncio
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json

from api.uk_market_api import UKMarketAPI
from api.japan_auction_api import JapanAuctionAPI
from api.government_data_api import GovernmentDataAPI
from api.exchange_rate_api import ExchangeRateAPI
from utils.logger import setup_logger
from database.connection import DatabaseConnection

logger = setup_logger(__name__)

class DataCollector:
    def __init__(self):
        self.db = DatabaseConnection()
        
    async def collect_uk_market_data(self, make: str = None, model: str = None) -> List[Dict]:
        """Collect comprehensive UK market data"""
        logger.info("Starting UK market data collection")
        
        uk_data = []
        
        try:
            async with UKMarketAPI() as uk_api:
                # Get vehicle listings
                listings = await uk_api.get_vehicle_listings(make, model)
                uk_data.extend(listings)
                
                # Get popular models if no specific make/model requested
                if not make and not model:
                    popular_models = await uk_api.get_popular_models()
                    
                    # Collect data for top 20 popular models
                    for model_data in popular_models[:20]:
                        model_listings = await uk_api.get_vehicle_listings(
                            model_data.get('make'), 
                            model_data.get('model')
                        )
                        uk_data.extend(model_listings)
                        
                        # Add price history
                        price_history = await uk_api.get_price_history(
                            model_data.get('make'),
                            model_data.get('model'),
                            datetime.now().year
                        )
                        
                        # Attach price history to listings
                        for listing in model_listings:
                            listing['price_history'] = price_history
                
                # Store in database
                await self._store_uk_data(uk_data)
                
                logger.info(f"Collected {len(uk_data)} UK market data points")
                return uk_data
                
        except Exception as e:
            logger.error(f"Error collecting UK market data: {str(e)}")
            return []

    async def collect_japan_auction_data(self, make: str = None, model: str = None) -> List[Dict]:
        """Collect comprehensive Japan auction data"""
        logger.info("Starting Japan auction data collection")
        
        japan_data = []
        
        try:
            async with JapanAuctionAPI() as japan_api:
                # Get recent auction results
                auction_results = await japan_api.get_auction_results(make, model)
                
                # Calculate landed costs for each result
                for result in auction_results:
                    if result.get('hammer_price'):
                        landed_cost = await japan_api.calculate_landed_cost(
                            result['hammer_price'], 
                            result
                        )
                        result['landed_cost_breakdown'] = landed_cost
                        result['total_landed_cost_gbp'] = landed_cost.get('total_landed_cost_gbp', 0)
                
                japan_data.extend(auction_results)
                
                # Get upcoming auctions for market intelligence
                upcoming = await japan_api.get_upcoming_auctions(make, model)
                for item in upcoming:
                    item['data_type'] = 'upcoming'
                japan_data.extend(upcoming)
                
                # Store in database
                await self._store_japan_data(japan_data)
                
                logger.info(f"Collected {len(japan_data)} Japan auction data points")
                return japan_data
                
        except Exception as e:
            logger.error(f"Error collecting Japan auction data: {str(e)}")
            return []

    async def collect_government_data(self) -> List[Dict]:
        """Collect government and regulatory data"""
        logger.info("Starting government data collection")
        
        gov_data = []
        
        try:
            async with GovernmentDataAPI() as gov_api:
                # Get registration statistics
                registration_data = await gov_api.get_vehicle_registration_data()
                gov_data.extend(registration_data)
                
                # Get regional popularity data
                regional_data = await gov_api.get_popular_models_by_region()
                gov_data.extend(regional_data)
                
                # Get import statistics
                import_stats = await gov_api.get_import_statistics()
                gov_data.extend(import_stats)
                
                # Get emission compliance data
                emission_data = await gov_api.get_emission_compliance_data()
                gov_data.extend(emission_data)
                
                # Store in database
                await self._store_government_data(gov_data)
                
                logger.info(f"Collected {len(gov_data)} government data points")
                return gov_data
                
        except Exception as e:
            logger.error(f"Error collecting government data: {str(e)}")
            return []

    async def collect_exchange_rates(self) -> Dict:
        """Collect current exchange rates"""
        try:
            async with ExchangeRateAPI() as rate_api:
                rates = await rate_api.get_current_rates()
                await self._store_exchange_rates(rates)
                return rates
        except Exception as e:
            logger.error(f"Error collecting exchange rates: {str(e)}")
            return {}

    async def _store_uk_data(self, data: List[Dict]):
        """Store UK market data in database"""
        try:
            query = """
            INSERT INTO uk_market_data 
            (source, make, model, year, mileage, price, fuel_type, location, 
             listing_date, days_listed, seller_type, url, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
            price = excluded.price,
            days_listed = excluded.days_listed,
            updated_at = CURRENT_TIMESTAMP
            """
            
            for item in data:
                await self.db.execute(query, (
                    item.get('source'),
                    item.get('make'),
                    item.get('model'),
                    item.get('year'),
                    item.get('mileage'),
                    item.get('price'),
                    item.get('fuel_type'),
                    item.get('location'),
                    item.get('listing_date'),
                    item.get('days_listed'),
                    item.get('seller_type'),
                    item.get('url'),
                    datetime.now()
                ))
                
            await self.db.commit()
            logger.info(f"Stored {len(data)} UK market records")
            
        except Exception as e:
            logger.error(f"Error storing UK data: {str(e)}")

    async def _store_japan_data(self, data: List[Dict]):
        """Store Japan auction data in database"""
        try:
            query = """
            INSERT INTO japan_auction_data 
            (source, make, model, year, mileage, hammer_price, condition_grade,
             auction_date, auction_house, total_landed_cost_gbp, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, auction_house, lot_number, auction_date) DO UPDATE SET
            hammer_price = excluded.hammer_price,
            total_landed_cost_gbp = excluded.total_landed_cost_gbp,
            updated_at = CURRENT_TIMESTAMP
            """
            
            for item in data:
                if item.get('data_type') != 'upcoming':  # Only store completed auctions
                    await self.db.execute(query, (
                        item.get('source'),
                        item.get('make'),
                        item.get('model'),
                        item.get('year'),
                        item.get('mileage'),
                        item.get('hammer_price'),
                        item.get('condition_grade'),
                        item.get('auction_date'),
                        item.get('auction_house'),
                        item.get('total_landed_cost_gbp'),
                        datetime.now()
                    ))
                    
            await self.db.commit()
            logger.info(f"Stored {len([d for d in data if d.get('data_type') != 'upcoming'])} Japan auction records")
            
        except Exception as e:
            logger.error(f"Error storing Japan data: {str(e)}")

    async def _store_government_data(self, data: List[Dict]):
        """Store government data in database"""
        try:
            query = """
            INSERT INTO government_data 
            (data_type, make, model, year, fuel_type, registration_count, 
             region, month, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(data_type, make, model, year, region, month) DO UPDATE SET
            registration_count = excluded.registration_count,
            updated_at = CURRENT_TIMESTAMP
            """
            
            for item in data:
                await self.db.execute(query, (
                    'registration',
                    item.get('make'),
                    item.get('model'),
                    item.get('year'),
                    item.get('fuel_type'),
                    item.get('registration_count'),
                    item.get('region'),
                    item.get('month'),
                    datetime.now()
                ))
                
            await self.db.commit()
            logger.info(f"Stored {len(data)} government data records")
            
        except Exception as e:
            logger.error(f"Error storing government data: {str(e)}")

    async def _store_exchange_rates(self, rates: Dict):
        """Store exchange rates in database"""
        try:
            query = """
            INSERT INTO exchange_rates (base_currency, target_currency, rate, date_recorded)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(base_currency, target_currency, date_recorded) DO UPDATE SET
            rate = excluded.rate
            """
            
            for currency, rate in rates.items():
                if currency != 'timestamp':
                    await self.db.execute(query, (
                        'JPY',
                        currency,
                        rate,
                        datetime.now().date()
                    ))
                    
            await self.db.commit()
            logger.info(f"Stored exchange rates for {len(rates)} currencies")
            
        except Exception as e:
            logger.error(f"Error storing exchange rates: {str(e)}")