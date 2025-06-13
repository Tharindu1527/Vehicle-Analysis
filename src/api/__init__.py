"""
API package for Vehicle Import Analyzer
Provides integration with external APIs for data collection
"""
import sys
import os

# Add parent directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from .uk_market_api import UKMarketAPI
from .japan_auction_api import JapanAuctionAPI
from .government_data_api import GovernmentDataAPI
from .exchange_rate_api import ExchangeRateAPI

__version__ = "1.0.0"
__author__ = "Vehicle Import Analyzer Team"

# Package-level exports
__all__ = [
    'UKMarketAPI',
    'JapanAuctionAPI', 
    'GovernmentDataAPI',
    'ExchangeRateAPI',
    'APIManager',
    'create_api_manager',
    'get_all_apis',
    'APIError',
    'RateLimitError',
    'AuthenticationError'
]

# Custom exceptions
class APIError(Exception):
    """Base exception for API errors"""
    pass

class RateLimitError(APIError):
    """Rate limit exceeded error"""
    pass

class AuthenticationError(APIError):
    """Authentication/authorization error"""
    pass

class ConnectionError(APIError):
    """Connection error"""
    pass

class TimeoutError(APIError):
    """Request timeout error"""
    pass

# API Manager
class APIManager:
    """Centralized API management"""
    
    def __init__(self):
        self.apis = {}
        self.rate_limiters = {}
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'rate_limited_requests': 0,
            'avg_response_time': 0,
            'apis_status': {}
        }
        self._initialize_apis()
    
    def _initialize_apis(self):
        """Initialize all API clients"""
        try:
            from utils.logger import setup_logger
            logger = setup_logger('api')
        except ImportError:
            import logging
            logger = logging.getLogger('api')
        
        try:
            # Initialize UK Market APIs
            self.apis['autotrader'] = UKMarketAPI()
            self.apis['motors'] = UKMarketAPI()
            self.apis['cargurus'] = UKMarketAPI()
            
            # Initialize Japan Auction APIs
            self.apis['uss'] = JapanAuctionAPI()
            self.apis['ju'] = JapanAuctionAPI()
            self.apis['aucnet'] = JapanAuctionAPI()
            
            # Initialize Government APIs
            self.apis['dvla'] = GovernmentDataAPI()
            self.apis['tfl'] = GovernmentDataAPI()
            self.apis['dft'] = GovernmentDataAPI()
            
            # Initialize Exchange Rate API
            self.apis['exchange_rates'] = ExchangeRateAPI()
            
            logger.info(f"Initialized {len(self.apis)} API clients")
            
        except Exception as e:
            logger.error(f"Error initializing APIs: {str(e)}")
            raise APIError(f"API initialization failed: {str(e)}") from e

# Simplified configuration for initial setup
class SimpleAPIConfig:
    """Simplified API configuration"""
    
    def __init__(self):
        self.configs = self._get_default_configs()
    
    def _get_default_configs(self):
        """Get default configurations without complex imports"""
        return {
            'autotrader': {
                'base_url': 'https://api.autotrader.co.uk/v1',
                'rate_limit': 1000,
                'timeout': 30
            },
            'motors': {
                'base_url': 'https://api.motors.co.uk/v2',
                'rate_limit': 500,
                'timeout': 30
            },
            'uss': {
                'base_url': 'https://api.uss-auction.com/v1',
                'rate_limit': 200,
                'timeout': 30
            }
        }
    
    def get_config(self, api_name):
        """Get configuration for specific API"""
        return self.configs.get(api_name, {})

# Rate limiting
class RateLimiter:
    """Simple rate limiter for API calls"""
    
    def __init__(self, max_requests=100, time_window=3600):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    def can_make_request(self):
        """Check if request can be made within rate limit"""
        import time
        
        current_time = time.time()
        
        # Remove old requests outside time window
        self.requests = [req_time for req_time in self.requests 
                        if current_time - req_time < self.time_window]
        
        # Check if we can make another request
        return len(self.requests) < self.max_requests
    
    def record_request(self):
        """Record a new request"""
        import time
        
        if self.can_make_request():
            self.requests.append(time.time())
            return True
        else:
            raise RateLimitError(f"Rate limit exceeded: {self.max_requests} requests per {self.time_window} seconds")

# Factory functions
def create_api_manager():
    """Create new API manager instance"""
    return APIManager()

def get_all_apis():
    """Get all available API clients"""
    manager = create_api_manager()
    return manager.apis

# Mock API for development
class MockAPI:
    """Mock API for testing and development"""
    
    def __init__(self, name, responses=None):
        self.name = name
        self.responses = responses or {}
        self.call_count = 0
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    
    async def get_data(self, endpoint, **kwargs):
        """Get mock data for endpoint"""
        self.call_count += 1
        
        if endpoint in self.responses:
            return self.responses[endpoint]
        
        # Return default mock data
        return {
            'success': True,
            'data': [],
            'meta': {
                'api': self.name,
                'endpoint': endpoint,
                'mock': True
            }
        }

# Global instances (simplified)
_api_manager = None
_api_config = SimpleAPIConfig()

def get_api_manager():
    """Get global API manager instance"""
    global _api_manager
    if _api_manager is None:
        _api_manager = APIManager()
    return _api_manager

def get_api_config():
    """Get global API config instance"""
    return _api_config

# Package initialization (simplified)
def initialize_package():
    """Initialize API package"""
    try:
        from utils.logger import setup_logger
        logger = setup_logger('api')
        logger.info("API package initialized")
    except ImportError:
        print("API package initialized (simplified mode)")

# Don't auto-initialize to avoid import issues
# initialize_package()