"""
Test suite for Vehicle Import Analyzer
Provides comprehensive testing utilities and fixtures
"""

import pytest
import asyncio
import os
import sys
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta
import tempfile

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Test configuration
TEST_CONFIG = {
    'DATABASE_PATH': ':memory:',  # Use in-memory database for tests
    'LOG_LEVEL': 'DEBUG',
    'FLASK_ENV': 'testing',
    'SECRET_KEY': 'test-secret-key'
}

# Test data fixtures
SAMPLE_UK_MARKET_DATA = [
    {
        'source': 'autotrader',
        'make': 'Toyota',
        'model': 'Prius',
        'year': 2020,
        'price': 25000,
        'mileage': 50000,
        'fuel_type': 'hybrid',
        'location': 'London',
        'seller_type': 'dealer',
        'url': 'https://autotrader.co.uk/test-listing'
    },
    {
        'source': 'motors',
        'make': 'Honda',
        'model': 'Civic',
        'year': 2019,
        'price': 20000,
        'mileage': 30000,
        'fuel_type': 'petrol',
        'location': 'Birmingham',
        'seller_type': 'private',
        'url': 'https://motors.co.uk/test-listing'
    }
]

SAMPLE_JAPAN_AUCTION_DATA = [
    {
        'source': 'uss',
        'make': 'Toyota',
        'model': 'Prius',
        'year': 2020,
        'hammer_price': 2000000,
        'mileage': 25000,
        'condition_grade': '4',
        'fuel_type': 'hybrid',
        'auction_house': 'USS Tokyo',
        'total_landed_cost_gbp': 18000
    },
    {
        'source': 'ju',
        'make': 'Honda',
        'model': 'Civic',
        'year': 2019,
        'hammer_price': 1800000,
        'mileage': 20000,
        'condition_grade': '3.5',
        'fuel_type': 'petrol',
        'auction_house': 'JU Osaka',
        'total_landed_cost_gbp': 16500
    }
]

SAMPLE_GOVERNMENT_DATA = [
    {
        'data_type': 'registration',
        'make': 'Toyota',
        'model': 'Prius',
        'year': 2020,
        'fuel_type': 'hybrid',
        'registration_count': 1250,
        'region': 'London',
        'month': 6
    }
]

# Test utilities
class TestDatabase:
    """Test database utility class"""
    
    def __init__(self):
        self.temp_db = None
        self.db_path = None
    
    def __enter__(self):
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.db_path = self.temp_db.name
        self.temp_db.close()
        return self.db_path
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db_path and os.path.exists(self.db_path):
            os.unlink(self.db_path)

class AsyncTestCase:
    """Base class for async test cases"""
    
    def setup_method(self):
        """Setup method called before each test"""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
    
    def teardown_method(self):
        """Teardown method called after each test"""
        self.loop.close()

# Test fixtures
@pytest.fixture
def sample_uk_data():
    """Fixture providing sample UK market data"""
    return SAMPLE_UK_MARKET_DATA.copy()

@pytest.fixture
def sample_japan_data():
    """Fixture providing sample Japan auction data"""
    return SAMPLE_JAPAN_AUCTION_DATA.copy()

@pytest.fixture
def sample_government_data():
    """Fixture providing sample government data"""
    return SAMPLE_GOVERNMENT_DATA.copy()

@pytest.fixture
def test_config():
    """Fixture providing test configuration"""
    return TEST_CONFIG.copy()

@pytest.fixture
def temp_database():
    """Fixture providing temporary test database"""
    with TestDatabase() as db_path:
        yield db_path

@pytest.fixture
def mock_api_response():
    """Fixture providing mock API response"""
    mock_response = Mock()
    mock_response.status = 200
    mock_response.json = AsyncMock(return_value={'success': True, 'data': []})
    return mock_response

@pytest.fixture
def mock_database():
    """Fixture providing mock database connection"""
    mock_db = Mock()
    mock_db.connect = AsyncMock()
    mock_db.disconnect = AsyncMock()
    mock_db.execute = AsyncMock(return_value=1)
    mock_db.fetchone = AsyncMock(return_value=None)
    mock_db.fetchall = AsyncMock(return_value=[])
    mock_db.commit = AsyncMock()
    mock_db.rollback = AsyncMock()
    return mock_db

@pytest.fixture
def event_loop():
    """Fixture providing event loop for async tests"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

# Test helpers
def create_test_vehicle(make='Toyota', model='Prius', year=2020, **kwargs):
    """Create test vehicle data"""
    vehicle = {
        'make': make,
        'model': model,
        'year': year,
        'fuel_type': 'hybrid',
        'price': 25000,
        'mileage': 50000
    }
    vehicle.update(kwargs)
    return vehicle

def create_test_profitability_result(**kwargs):
    """Create test profitability analysis result"""
    result = {
        'make': 'Toyota',
        'model': 'Prius',
        'year': 2020,
        'fuel_type': 'hybrid',
        'avg_uk_selling_price': 25000,
        'avg_landed_cost': 20000,
        'gross_profit': 5000,
        'profit_margin_percent': 20.0,
        'roi_percent': 25.0,
        'risk_score': 30.0,
        'demand_score': 75.0,
        'overall_score': 70.0,
        'final_recommendation_score': 72.5,
        'recommendation_category': 'Recommended',
        'priority': 'High'
    }
    result.update(kwargs)
    return result

def assert_valid_vehicle_data(data, required_fields=None):
    """Assert that vehicle data is valid"""
    if required_fields is None:
        required_fields = ['make', 'model', 'year']
    
    for field in required_fields:
        assert field in data, f"Missing required field: {field}"
        assert data[field] is not None, f"Field {field} cannot be None"
    
    if 'year' in data:
        assert isinstance(data['year'], int), "Year must be integer"
        assert 1980 <= data['year'] <= datetime.now().year + 1, "Year out of valid range"
    
    if 'price' in data and data['price'] is not None:
        assert data['price'] > 0, "Price must be positive"
    
    if 'mileage' in data and data['mileage'] is not None:
        assert data['mileage'] >= 0, "Mileage cannot be negative"

def assert_valid_profitability_result(result):
    """Assert that profitability result is valid"""
    required_fields = [
        'make', 'model', 'year', 'fuel_type',
        'profit_margin_percent', 'roi_percent',
        'risk_score', 'demand_score', 'overall_score'
    ]
    
    for field in required_fields:
        assert field in result, f"Missing required field: {field}"
    
    # Validate score ranges
    score_fields = ['risk_score', 'demand_score', 'overall_score']
    for field in score_fields:
        if result.get(field) is not None:
            assert 0 <= result[field] <= 100, f"{field} must be between 0 and 100"

# Mock classes for testing
class MockAPIResponse:
    """Mock API response class"""
    
    def __init__(self, status=200, data=None):
        self.status = status
        self._data = data or {}
    
    async def json(self):
        return self._data
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

class MockSession:
    """Mock aiohttp session class"""
    
    def __init__(self, responses=None):
        self.responses = responses or []
        self.request_count = 0
    
    def get(self, url, **kwargs):
        response = self.responses[self.request_count % len(self.responses)] if self.responses else MockAPIResponse()
        self.request_count += 1
        return response
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

# Test data generators
def generate_test_uk_listings(count=10):
    """Generate test UK market listings"""
    makes = ['Toyota', 'Honda', 'BMW', 'Mercedes', 'Audi']
    models = ['Prius', 'Civic', '3 Series', 'C-Class', 'A4']
    fuel_types = ['petrol', 'diesel', 'hybrid', 'electric']
    
    listings = []
    for i in range(count):
        make = makes[i % len(makes)]
        model = models[i % len(models)]
        listings.append({
            'source': 'test',
            'make': make,
            'model': model,
            'year': 2015 + (i % 8),
            'price': 15000 + (i * 2000),
            'mileage': 20000 + (i * 5000),
            'fuel_type': fuel_types[i % len(fuel_types)],
            'location': f'City_{i}',
            'seller_type': 'dealer' if i % 2 == 0 else 'private'
        })
    
    return listings

def generate_test_japan_auctions(count=10):
    """Generate test Japan auction results"""
    makes = ['Toyota', 'Honda', 'Mazda', 'Subaru', 'Mitsubishi']
    models = ['Prius', 'Civic', 'CX-5', 'Impreza', 'Outlander']
    grades = ['3', '3.5', '4', '4.5', '5']
    
    auctions = []
    for i in range(count):
        make = makes[i % len(makes)]
        model = models[i % len(models)]
        auctions.append({
            'source': 'test_auction',
            'make': make,
            'model': model,
            'year': 2015 + (i % 8),
            'hammer_price': 1500000 + (i * 200000),
            'mileage': 15000 + (i * 3000),
            'condition_grade': grades[i % len(grades)],
            'auction_house': f'Test_Auction_{i % 3}',
            'total_landed_cost_gbp': 15000 + (i * 1500)
        })
    
    return auctions

# Test configuration
pytest_plugins = []

def pytest_configure(config):
    """Configure pytest"""
    # Add custom markers
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "api: marks tests that require API access"
    )

def pytest_collection_modifyitems(config, items):
    """Modify test collection"""
    # Add markers to tests based on their names
    for item in items:
        if "slow" in item.nodeid:
            item.add_marker(pytest.mark.slow)
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        if "api" in item.nodeid:
            item.add_marker(pytest.mark.api)

# Version info
__version__ = "1.0.0"
__test_framework__ = "pytest"

# Package exports
__all__ = [
    'SAMPLE_UK_MARKET_DATA',
    'SAMPLE_JAPAN_AUCTION_DATA',
    'SAMPLE_GOVERNMENT_DATA',
    'TestDatabase',
    'AsyncTestCase',
    'MockAPIResponse',
    'MockSession',
    'create_test_vehicle',
    'create_test_profitability_result',
    'assert_valid_vehicle_data',
    'assert_valid_profitability_result',
    'generate_test_uk_listings',
    'generate_test_japan_auctions'
]