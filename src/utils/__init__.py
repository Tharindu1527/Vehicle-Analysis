"""
Utilities package for Vehicle Import Analyzer
Provides helper functions, configuration management, and logging utilities
"""

from .config import Config
from .logger import setup_logger, get_logger, VehicleAnalyzerLogger, LoggedOperation, log_performance
from .helpers import (
    normalize_text,
    extract_year_from_text,
    extract_mileage_from_text,
    extract_price_from_text,
    validate_vehicle_data,
    clean_vehicle_data,
    calculate_age_from_year,
    format_currency,
    generate_cache_key,
    calculate_percentage_change,
    safe_divide,
    clamp,
    round_to_nearest,
    is_valid_url,
    clean_url,
    get_make_brand_mapping,
    standardize_fuel_type,
    estimate_co2_emissions,
    categorize_vehicle_by_price,
    calculate_depreciation_rate
)

__version__ = "1.0.0"
__author__ = "Vehicle Import Analyzer Team"

# Package-level exports
__all__ = [
    # Configuration
    'Config',
    
    # Logging
    'setup_logger',
    'get_logger',
    'VehicleAnalyzerLogger',
    'LoggedOperation',
    'log_performance',
    
    # Helper functions
    'normalize_text',
    'extract_year_from_text',
    'extract_mileage_from_text',
    'extract_price_from_text',
    'validate_vehicle_data',
    'clean_vehicle_data',
    'calculate_age_from_year',
    'format_currency',
    'generate_cache_key',
    'calculate_percentage_change',
    'safe_divide',
    'clamp',
    'round_to_nearest',
    'is_valid_url',
    'clean_url',
    'get_make_brand_mapping',
    'standardize_fuel_type',
    'estimate_co2_emissions',
    'categorize_vehicle_by_price',
    'calculate_depreciation_rate'
]

# Initialize package-level configuration
_config = None
_logger = None

def get_config():
    """Get package-level configuration instance"""
    global _config
    if _config is None:
        _config = Config()
    return _config

def get_package_logger():
    """Get package-level logger instance"""
    global _logger
    if _logger is None:
        _logger = setup_logger('utils')
    return _logger

# Package initialization
def initialize_utils(config_file=None, log_level=None):
    """Initialize utilities package with custom configuration"""
    global _config, _logger
    
    if config_file:
        _config = Config(config_file)
    else:
        _config = Config()
    
    if log_level:
        import os
        os.environ['LOG_LEVEL'] = log_level
    
    _logger = setup_logger('utils')
    _logger.info("Utils package initialized")

# Convenience functions
def quick_validate(data):
    """Quick validation for vehicle data"""
    return len(validate_vehicle_data(data)) == 0

def quick_clean(data):
    """Quick cleaning for vehicle data"""
    return clean_vehicle_data(data)

def quick_format_price(price, currency='GBP'):
    """Quick price formatting"""
    return format_currency(price, currency)