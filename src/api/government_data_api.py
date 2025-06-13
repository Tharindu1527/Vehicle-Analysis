"""
UK Government Data API Integration
Fetches DVLA and other government vehicle statistics
"""
import aiohttp
import asyncio
from typing import List, Dict, Optional
import json
from datetime import datetime, timedelta

from src.utils.logger import setup_logger
from src.utils.config import Config

logger = setup_logger(__name__)

class GovernmentDataAPI:
    def __init__(self):
        self.config = Config()
        self.session = None
        
        # Government API endpoints
        self.dvla_api = "https://api.dvla.gov.uk/v1"
        self.gov_data_api = "https://api.gov.uk/v1"
        self.dft_api = "https://api.dft.gov.uk/v1"
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_vehicle_registration_data(self, year: int = None) -> List[Dict]:
        """Get vehicle registration statistics from DVLA"""
        try:
            headers = {
                'Authorization': f'Bearer {self.config.get("DVLA_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'year': year or datetime.now().year,
                'data_type': 'registrations'
            }
            
            async with self.session.get(
                f"{self.dvla_api}/vehicle-enquiry/registrations",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_registration_data(data)
                else:
                    logger.warning(f"DVLA API returned status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching registration data: {str(e)}")
            return []

    async def get_popular_models_by_region(self) -> List[Dict]:
        """Get popular vehicle models by UK region"""
        try:
            headers = {
                'Authorization': f'Bearer {self.config.get("DVLA_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            async with self.session.get(
                f"{self.dvla_api}/statistics/popular-models",
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('regional_data', [])
                else:
                    logger.warning(f"DVLA API returned status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching regional data: {str(e)}")
            return []

    async def get_import_statistics(self) -> List[Dict]:
        """Get vehicle import statistics"""
        try:
            headers = {
                'X-API-Key': self.config.get("GOV_DATA_API_KEY"),
                'Content-Type': 'application/json'
            }
            
            async with self.session.get(
                f"{self.gov_data_api}/trade/vehicle-imports",
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('import_data', [])
                else:
                    logger.warning(f"Gov Data API returned status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching import statistics: {str(e)}")
            return []

    async def get_emission_compliance_data(self) -> List[Dict]:
        """Get ULEZ and emission compliance data"""
        try:
            headers = {
                'Authorization': f'Bearer {self.config.get("DFT_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            async with self.session.get(
                f"{self.dft_api}/emissions/compliance",
                headers=headers
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('compliance_data', [])
                else:
                    logger.warning(f"DFT API returned status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching emission data: {str(e)}")
            return []

    async def get_mot_test_data(self, make: str = None, model: str = None) -> List[Dict]:
        """Get MOT test data for reliability analysis"""
        try:
            headers = {
                'X-API-Key': self.config.get("DVLA_API_KEY"),
                'Content-Type': 'application/json'
            }
            
            params = {
                'make': make,
                'model': model,
                'test_result': 'all',
                'date_from': (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            }
            
            async with self.session.get(
                f"{self.dvla_api}/mot-history/search",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_mot_data(data)
                else:
                    logger.warning(f"MOT API returned status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching MOT data: {str(e)}")
            return []

    async def get_vehicle_tax_rates(self, year: int = None) -> List[Dict]:
        """Get vehicle tax rates by emissions"""
        try:
            headers = {
                'Authorization': f'Bearer {self.config.get("DFT_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'year': year or datetime.now().year
            }
            
            async with self.session.get(
                f"{self.dft_api}/vehicle-tax/rates",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('tax_rates', [])
                else:
                    logger.warning(f"DFT Tax API returned status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching tax rates: {str(e)}")
            return []

    async def get_insurance_group_data(self, make: str = None, model: str = None) -> List[Dict]:
        """Get insurance group ratings"""
        try:
            headers = {
                'X-API-Key': self.config.get("ABI_API_KEY"),
                'Content-Type': 'application/json'
            }
            
            params = {
                'make': make,
                'model': model
            }
            
            async with self.session.get(
                "https://api.abi.org.uk/insurance-groups",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('insurance_groups', [])
                else:
                    logger.warning(f"ABI API returned status {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching insurance group data: {str(e)}")
            return []

    def _parse_registration_data(self, data: Dict) -> List[Dict]:
        """Parse DVLA registration data"""
        registrations = []
        
        for item in data.get('registrations', []):
            registration = {
                'make': item.get('make'),
                'model': item.get('model'),
                'year': item.get('year_of_manufacture'),
                'fuel_type': item.get('fuel_type'),
                'registration_count': item.get('count'),
                'region': item.get('dvla_region'),
                'month': item.get('registration_month'),
                'vehicle_category': item.get('vehicle_category'),
                'body_type': item.get('body_type'),
                'engine_size': item.get('engine_capacity'),
                'co2_emissions': item.get('co2_emissions')
            }
            registrations.append(registration)
            
        return registrations

    def _parse_mot_data(self, data: Dict) -> List[Dict]:
        """Parse MOT test data"""
        mot_results = []
        
        for item in data.get('tests', []):
            mot_result = {
                'make': item.get('make'),
                'model': item.get('model'),
                'year': item.get('year_of_manufacture'),
                'test_result': item.get('test_result'),
                'test_date': item.get('completed_date'),
                'odometer_value': item.get('odometer_value'),
                'defects_count': len(item.get('defects', [])),
                'advisories_count': len(item.get('advisories', [])),
                'test_class': item.get('test_class_id'),
                'fuel_type': item.get('fuel_type')
            }
            mot_results.append(mot_result)
            
        return mot_results

    async def check_ulez_compliance(self, make: str, model: str, year: int) -> Dict:
        """Check if vehicle is ULEZ compliant"""
        try:
            headers = {
                'X-API-Key': self.config.get("TFL_API_KEY"),
                'Content-Type': 'application/json'
            }
            
            params = {
                'make': make,
                'model': model,
                'year': year
            }
            
            async with self.session.get(
                "https://api.tfl.gov.uk/Vehicle/UlezCompliance",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'ulez_compliant': data.get('compliant', False),
                        'charge_amount': data.get('charge', 0),
                        'compliance_date': data.get('compliance_date'),
                        'exemption_code': data.get('exemption_code'),
                        'vehicle_type': data.get('vehicle_type')
                    }
                    
        except Exception as e:
            logger.error(f"Error checking ULEZ compliance: {str(e)}")
            
        return {
            'ulez_compliant': False, 
            'charge_amount': 12.50, 
            'compliance_date': None,
            'exemption_code': None,
            'vehicle_type': 'car'
        }

    async def check_clean_air_zone_compliance(self, make: str, model: str, year: int, city: str = 'birmingham') -> Dict:
        """Check Clean Air Zone compliance for various UK cities"""
        try:
            headers = {
                'X-API-Key': self.config.get("CAZ_API_KEY"),
                'Content-Type': 'application/json'
            }
            
            params = {
                'make': make,
                'model': model,
                'year': year,
                'city': city
            }
            
            async with self.session.get(
                f"https://api.{city}.gov.uk/caz/vehicle-compliance",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'caz_compliant': data.get('compliant', False),
                        'daily_charge': data.get('daily_charge', 0),
                        'city': city,
                        'exempt': data.get('exempt', False)
                    }
                    
        except Exception as e:
            logger.error(f"Error checking CAZ compliance for {city}: {str(e)}")
            
        return {
            'caz_compliant': False,
            'daily_charge': 8.00,  # Typical CAZ charge
            'city': city,
            'exempt': False
        }

    async def get_depreciation_data(self, make: str, model: str) -> Dict:
        """Get vehicle depreciation data from government sources"""
        try:
            headers = {
                'Authorization': f'Bearer {self.config.get("DVLA_API_KEY")}',
                'Content-Type': 'application/json'
            }
            
            params = {
                'make': make,
                'model': model,
                'years': 5  # Last 5 years of data
            }
            
            async with self.session.get(
                f"{self.dvla_api}/vehicle-values/depreciation",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'annual_depreciation_rate': data.get('annual_rate', 0.15),
                        'depreciation_curve': data.get('depreciation_by_year', []),
                        'residual_value_3_years': data.get('residual_3_year', 0.6),
                        'residual_value_5_years': data.get('residual_5_year', 0.4)
                    }
                    
        except Exception as e:
            logger.error(f"Error fetching depreciation data: {str(e)}")
            
        return {
            'annual_depreciation_rate': 0.15,  # 15% default
            'depreciation_curve': [],
            'residual_value_3_years': 0.6,
            'residual_value_5_years': 0.4
        }

    async def get_recall_data(self, make: str, model: str, year: int) -> List[Dict]:
        """Get vehicle recall information"""
        try:
            headers = {
                'X-API-Key': self.config.get("DVSA_API_KEY"),
                'Content-Type': 'application/json'
            }
            
            params = {
                'make': make,
                'model': model,
                'year_from': year - 2,
                'year_to': year + 2
            }
            
            async with self.session.get(
                "https://api.dvsa.gov.uk/recalls/search",
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('recalls', [])
                    
        except Exception as e:
            logger.error(f"Error fetching recall data: {str(e)}")
            
        return []