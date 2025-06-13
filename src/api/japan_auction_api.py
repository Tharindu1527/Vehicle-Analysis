"""
Japan Auction Data API Integration
Integrates with USS, JU, and other Japanese auction platforms
"""
import aiohttp
import asyncio
from typing import List, Dict, Optional
from datetime import datetime
import json

from utils.logger import setup_logger
from utils.config import Config

logger = setup_logger(__name__)

class JapanAuctionAPI:
    def __init__(self):
        self.config = Config()
        self.session = None
        
        # API endpoints for Japanese auction houses
        self.uss_api = "https://api.uss-auction.com/v1"
        self.ju_api = "https://api.ju-net.co.jp/v1"
        self.aucnet_api = "https://api.aucnet.jp/v1"
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_auction_results(self, make: str = None, model: str = None,
                                year_from: int = None, year_to: int = None) -> List[Dict]:
        """Fetch recent auction results from Japanese platforms"""
        results = []
        
        try:
            # USS Auction results
            uss_results = await self._fetch_uss_results(make, model, year_from, year_to)
            results.extend(uss_results)
            
            # JU Auction results
            ju_results = await self._fetch_ju_results(make, model, year_from, year_to)
            results.extend(ju_results)
            
            # AucNet results
            aucnet_results = await self._fetch_aucnet_results(make, model, year_from, year_to)
            results.extend(aucnet_results)
            
            logger.info(f"Collected {len(results)} auction results from Japan")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching Japan auction data: {str(e)}")
            return []

    async def _fetch_uss_results(self, make, model, year_from, year_to) -> List[Dict]:
        """Fetch from USS Auction API"""
        headers = {
            'Authorization': f'Bearer {self.config.get("USS_API_KEY")}',
            'Content-Type': 'application/json'
        }
        
        params = {
            'maker': make,
            'model': model,
            'year_from': year_from,
            'year_to': year_to,
            'status': 'sold',
            'limit': 100
        }
        
        try:
            async with self.session.get(
                f"{self.uss_api}/auction/results",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_uss_response(data)
                else:
                    logger.warning(f"USS API returned status {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error calling USS API: {str(e)}")
            return []

    async def _fetch_ju_results(self, make, model, year_from, year_to) -> List[Dict]:
        """Fetch from JU Auction API"""
        headers = {
            'X-API-Key': self.config.get("JU_API_KEY"),
            'Content-Type': 'application/json'
        }
        
        params = {
            'make': make,
            'model': model,
            'model_year_min': year_from,
            'model_year_max': year_to,
            'auction_status': 'completed',
            'per_page': 100
        }
        
        try:
            async with self.session.get(
                f"{self.ju_api}/vehicles/sold",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_ju_response(data)
                else:
                    logger.warning(f"JU API returned status {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error calling JU API: {str(e)}")
            return []

    async def _fetch_aucnet_results(self, make, model, year_from, year_to) -> List[Dict]:
        """Fetch from AucNet API"""
        headers = {
            'Authorization': f'Bearer {self.config.get("AUCNET_API_KEY")}',
            'Accept': 'application/json'
        }
        
        params = {
            'brand': make,
            'model': model,
            'year_start': year_from,
            'year_end': year_to,
            'result_type': 'sold',
            'count': 100
        }
        
        try:
            async with self.session.get(
                f"{self.aucnet_api}/search/results",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_aucnet_response(data)
                else:
                    logger.warning(f"AucNet API returned status {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error calling AucNet API: {str(e)}")
            return []

    def _parse_uss_response(self, data: Dict) -> List[Dict]:
        """Parse USS Auction API response"""
        results = []
        
        for item in data.get('results', []):
            result = {
                'source': 'uss',
                'make': item.get('maker'),
                'model': item.get('model'),
                'year': item.get('year'),
                'mileage': item.get('mileage'),
                'hammer_price': item.get('hammer_price'),
                'condition_grade': item.get('grade'),
                'exterior_grade': item.get('exterior_grade'),
                'interior_grade': item.get('interior_grade'),
                'auction_date': item.get('auction_date'),
                'auction_house': item.get('auction_house'),
                'lot_number': item.get('lot_number'),
                'engine_size': item.get('engine_cc'),
                'fuel_type': item.get('fuel'),
                'transmission': item.get('transmission'),
                'drive_type': item.get('drive'),
                'color': item.get('color'),
                'seller_fees': item.get('seller_fee', 0),
                'buyer_fees': item.get('buyer_fee', 0)
            }
            results.append(result)
            
        return results

    def _parse_ju_response(self, data: Dict) -> List[Dict]:
        """Parse JU Auction API response"""
        results = []
        
        for item in data.get('vehicles', []):
            result = {
                'source': 'ju',
                'make': item.get('make'),
                'model': item.get('model'),
                'year': item.get('model_year'),
                'mileage': item.get('odometer'),
                'hammer_price': item.get('sale_price'),
                'condition_grade': item.get('overall_grade'),
                'exterior_grade': item.get('exterior_grade'),
                'interior_grade': item.get('interior_grade'),
                'auction_date': item.get('auction_date'),
                'auction_house': item.get('branch_name'),
                'lot_number': item.get('lot_no'),
                'engine_size': item.get('displacement'),
                'fuel_type': item.get('fuel_type'),
                'transmission': item.get('transmission'),
                'drive_type': item.get('drivetrain'),
                'color': item.get('exterior_color'),
                'seller_fees': item.get('seller_commission', 0),
                'buyer_fees': item.get('buyer_commission', 0)
            }
            results.append(result)
            
        return results

    def _parse_aucnet_response(self, data: Dict) -> List[Dict]:
        """Parse AucNet API response"""
        results = []
        
        for item in data.get('items', []):
            result = {
                'source': 'aucnet',
                'make': item.get('brand'),
                'model': item.get('model'),
                'year': item.get('year'),
                'mileage': item.get('mileage'),
                'hammer_price': item.get('winning_price'),
                'condition_grade': item.get('grade'),
                'exterior_grade': item.get('exterior_condition'),
                'interior_grade': item.get('interior_condition'),
                'auction_date': item.get('auction_date'),
                'auction_house': item.get('venue'),
                'lot_number': item.get('lot_id'),
                'engine_size': item.get('engine_displacement'),
                'fuel_type': item.get('fuel'),
                'transmission': item.get('transmission_type'),
                'drive_type': item.get('drive_system'),
                'color': item.get('body_color'),
                'seller_fees': 0,  # AucNet typically doesn't separate these
                'buyer_fees': item.get('total_fees', 0)
            }
            results.append(result)
            
        return results

    async def get_upcoming_auctions(self, make: str = None, model: str = None) -> List[Dict]:
        """Get upcoming auction listings"""
        upcoming = []
        
        try:
            # USS upcoming auctions
            headers = {
                'Authorization': f'Bearer {self.config.get("USS_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'maker': make,
                'model': model,
                'status': 'upcoming'
            }
            
            async with self.session.get(
                f"{self.uss_api}/auction/upcoming",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    upcoming.extend(data.get('upcoming', []))
                    
        except Exception as e:
            logger.error(f"Error fetching upcoming auctions: {str(e)}")
            
        return upcoming

    async def calculate_landed_cost(self, hammer_price: float, vehicle_data: Dict) -> Dict:
        """Calculate total landed cost for a vehicle from Japan"""
        try:
            # Base costs
            costs = {
                'hammer_price': hammer_price,
                'auction_fees': hammer_price * 0.08,  # Typical 8% auction fee
                'transport_to_port': 25000,  # ¥25,000 typical
                'export_certificate': 5000,   # ¥5,000
                'radiation_certificate': 3000,  # ¥3,000
                'freight_cost': self._calculate_freight_cost(vehicle_data),
                'uk_import_duty': 0,  # Usually 0% from Japan
                'uk_vat': 0,  # Calculated later
                'port_handling': 150,  # £150 GBP
                'transport_from_port': 200,  # £200 GBP
                'iva_test': 250,  # £250 GBP
                'registration_fees': 55,  # £55 GBP
                'conversion_costs': self._calculate_conversion_costs(vehicle_data)
            }
            
            # Convert JPY to GBP
            exchange_rate = await self._get_exchange_rate()
            
            jpy_total = (costs['hammer_price'] + costs['auction_fees'] + 
                        costs['transport_to_port'] + costs['export_certificate'] + 
                        costs['radiation_certificate'])
            
            gbp_from_jpy = jpy_total / exchange_rate
            
            # Calculate UK VAT (20% on total CIF value)
            cif_value = gbp_from_jpy + costs['freight_cost']
            costs['uk_vat'] = cif_value * 0.20
            
            # Total landed cost
            total_landed_cost = (gbp_from_jpy + costs['freight_cost'] + 
                               costs['uk_vat'] + costs['port_handling'] + 
                               costs['transport_from_port'] + costs['iva_test'] + 
                               costs['registration_fees'] + costs['conversion_costs'])
            
            costs['total_landed_cost_gbp'] = total_landed_cost
            costs['exchange_rate_used'] = exchange_rate
            
            return costs
            
        except Exception as e:
            logger.error(f"Error calculating landed cost: {str(e)}")
            return {}

    def _calculate_freight_cost(self, vehicle_data: Dict) -> float:
        """Calculate freight cost based on vehicle type"""
        # Base freight cost from Japan to UK
        base_cost = 800  # £800 for standard car
        
        # Adjust for vehicle size
        if vehicle_data.get('category') == 'SUV':
            base_cost += 200
        elif vehicle_data.get('category') == 'Truck':
            base_cost += 400
        elif vehicle_data.get('category') == 'Van':
            base_cost += 300
            
        return base_cost

    def _calculate_conversion_costs(self, vehicle_data: Dict) -> float:
        """Calculate conversion costs for UK compliance"""
        costs = 0
        
        # Speedometer conversion
        costs += 150
        
        # Fog light installation
        costs += 100
        
        # Side mirror adjustment
        costs += 50
        
        # Headlight adjustment
        costs += 75
        
        # Additional costs for specific vehicle types
        if vehicle_data.get('year', 0) > 2015:
            costs += 200  # Additional compliance costs for newer vehicles
            
        return costs

    async def _get_exchange_rate(self) -> float:
        """Get current JPY to GBP exchange rate"""
        try:
            # Use a financial API for real-time exchange rates
            async with self.session.get(
                f"https://api.exchangerate-api.com/v4/latest/JPY"
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data['rates']['GBP']
        except:
            pass
            
        # Fallback rate if API fails
        return 0.0055  # Approximate JPY to GBP rate