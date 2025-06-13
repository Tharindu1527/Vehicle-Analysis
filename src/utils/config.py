"""
Complete configuration management module
"""
import os
import yaml
from typing import Any, Dict, Optional, List
import logging

class Config:
    """Configuration manager with environment variable support"""
    
    def __init__(self, config_file: str = 'config/config.yaml'):
        self.config_data = {}
        self.config_file = config_file
        self.load_config(config_file)
        self.load_env_variables()
        self._setup_defaults()
    
    def load_config(self, config_file: str):
        """Load configuration from YAML file"""
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as file:
                    self.config_data = yaml.safe_load(file) or {}
                logging.info(f"Loaded configuration from {config_file}")
            else:
                logging.warning(f"Configuration file {config_file} not found, using defaults")
                self.config_data = {}
        except Exception as e:
            logging.error(f"Error loading config file {config_file}: {e}")
            self.config_data = {}
    
    def load_env_variables(self):
        """Load configuration from environment variables"""
        env_mapping = {
            # Database
            'DATABASE_PATH': 'DATABASE_PATH',
            'DATABASE_TIMEOUT': 'DATABASE_TIMEOUT',
            
            # UK Market APIs
            'AUTOTRADER_API_KEY': 'AUTOTRADER_API_KEY',
            'MOTORS_API_KEY': 'MOTORS_API_KEY',
            'CARGURUS_API_KEY': 'CARGURUS_API_KEY',
            
            # Japan Auction APIs
            'USS_API_KEY': 'USS_API_KEY',
            'JU_API_KEY': 'JU_API_KEY',
            'AUCNET_API_KEY': 'AUCNET_API_KEY',
            
            # Government APIs
            'DVLA_API_KEY': 'DVLA_API_KEY',
            'GOV_DATA_API_KEY': 'GOV_DATA_API_KEY',
            'DFT_API_KEY': 'DFT_API_KEY',
            'TFL_API_KEY': 'TFL_API_KEY',
            'ABI_API_KEY': 'ABI_API_KEY',
            'DVSA_API_KEY': 'DVSA_API_KEY',
            'CAZ_API_KEY': 'CAZ_API_KEY',
            
            # Exchange Rate APIs
            'FIXER_API_KEY': 'FIXER_API_KEY',
            'XE_API_KEY': 'XE_API_KEY',
            
            # Application
            'SECRET_KEY': 'SECRET_KEY',
            'FLASK_ENV': 'FLASK_ENV',
            'FLASK_DEBUG': 'FLASK_DEBUG',
            
            # AWS (for backups)
            'AWS_ACCESS_KEY': 'AWS_ACCESS_KEY',
            'AWS_SECRET_KEY': 'AWS_SECRET_KEY',
            
            # Logging
            'LOG_LEVEL': 'LOG_LEVEL',
            'LOG_FILE': 'LOG_FILE'
        }
        
        for config_key, env_key in env_mapping.items():
            env_value = os.getenv(env_key)
            if env_value:
                # Handle boolean values
                if env_value.lower() in ('true', 'false'):
                    env_value = env_value.lower() == 'true'
                # Handle numeric values
                elif env_value.isdigit():
                    env_value = int(env_value)
                elif env_value.replace('.', '').isdigit():
                    env_value = float(env_value)
                
                self.config_data[config_key] = env_value
    
    def _setup_defaults(self):
        """Setup default configuration values"""
        defaults = {
            # Database defaults
            'DATABASE_PATH': './data/vehicle_import_analyzer.db',
            'DATABASE_TIMEOUT': 30,
            
            # API rate limits (requests per hour)
            'AUTOTRADER_RATE_LIMIT': 1000,
            'MOTORS_RATE_LIMIT': 500,
            'CARGURUS_RATE_LIMIT': 750,
            'USS_RATE_LIMIT': 200,
            'JU_RATE_LIMIT': 150,
            'AUCNET_RATE_LIMIT': 100,
            
            # Data collection intervals (hours)
            'UK_MARKET_UPDATE_INTERVAL': 6,
            'JAPAN_AUCTION_UPDATE_INTERVAL': 12,
            'GOVERNMENT_DATA_UPDATE_INTERVAL': 24,
            'EXCHANGE_RATE_UPDATE_INTERVAL': 1,
            
            # Analysis settings
            'MINIMUM_PROFIT_MARGIN': 5.0,
            'MINIMUM_ROI': 10.0,
            'MAXIMUM_DAYS_TO_SELL': 90,
            'MINIMUM_UK_LISTINGS': 3,
            'MINIMUM_JAPAN_AUCTIONS': 3,
            'MAXIMUM_VEHICLE_AGE': 20,
            
            # Cost calculations (in respective currencies)
            'JAPAN_AUCTION_FEE_PERCENT': 8.0,
            'JAPAN_TRANSPORT_TO_PORT_JPY': 25000,
            'JAPAN_EXPORT_CERTIFICATE_JPY': 5000,
            'JAPAN_RADIATION_CERTIFICATE_JPY': 3000,
            'UK_BASE_FREIGHT_GBP': 800,
            'UK_SUV_SURCHARGE_GBP': 200,
            'UK_TRUCK_SURCHARGE_GBP': 400,
            'UK_VAN_SURCHARGE_GBP': 300,
            'UK_IMPORT_DUTY_PERCENT': 0.0,
            'UK_VAT_PERCENT': 20.0,
            'UK_PORT_HANDLING_GBP': 150,
            'UK_TRANSPORT_FROM_PORT_GBP': 200,
            'UK_IVA_TEST_GBP': 250,
            'UK_REGISTRATION_FEES_GBP': 55,
            'UK_SPEEDOMETER_CONVERSION_GBP': 150,
            'UK_FOG_LIGHTS_GBP': 100,
            'UK_SIDE_MIRRORS_GBP': 50,
            'UK_HEADLIGHT_ADJUSTMENT_GBP': 75,
            'UK_COMPLIANCE_MODERN_GBP': 200,
            
            # Scoring weights
            'PROFIT_MARGIN_WEIGHT': 0.25,
            'ROI_WEIGHT': 0.25,
            'RISK_WEIGHT': 0.20,
            'DEMAND_WEIGHT': 0.20,
            'SPEED_WEIGHT': 0.10,
            
            # Alert thresholds
            'HIGH_PROFIT_MARGIN_THRESHOLD': 25.0,
            'LOW_COMPETITION_THRESHOLD': 5,
            'FAST_SELLING_DAYS_THRESHOLD': 14,
            'DATA_FRESHNESS_HOURS': 12,
            
            # Dashboard settings
            'DASHBOARD_PORT': 5000,
            'DASHBOARD_HOST': '0.0.0.0',
            'DASHBOARD_DEBUG': False,
            'DASHBOARD_AUTO_REFRESH_MINUTES': 30,
            'DEFAULT_PAGE_SIZE': 20,
            'MAX_PAGE_SIZE': 100,
            
            # Logging
            'LOG_LEVEL': 'INFO',
            'LOG_FILE_MAX_SIZE_MB': 10,
            'LOG_BACKUP_COUNT': 5,
            'LOG_DIRECTORY': 'logs',
            
            # Security
            'SECRET_KEY': 'dev-secret-key-change-in-production',
            'FLASK_ENV': 'development',
            'FLASK_DEBUG': False,
            
            # Data retention (days)
            'RAW_DATA_RETENTION_DAYS': 90,
            'ANALYSIS_RESULTS_RETENTION_DAYS': 365,
            'LOG_RETENTION_DAYS': 30,
            
            # Batch sizes
            'UK_LISTINGS_BATCH_SIZE': 100,
            'JAPAN_AUCTIONS_BATCH_SIZE': 50,
            'GOVERNMENT_RECORDS_BATCH_SIZE': 200,
            
            # API timeouts (seconds)
            'API_TIMEOUT_SECONDS': 30,
            'API_RETRY_ATTEMPTS': 3,
            'API_RETRY_DELAY_SECONDS': 5,
            
            # ML model settings
            'ML_MODEL_ENABLED': True,
            'ML_RETRAIN_DAYS': 30,
            'ML_MIN_TRAINING_SAMPLES': 100,
            
            # Backup settings
            'BACKUP_ENABLED': True,
            'BACKUP_SCHEDULE': '0 2 * * *',  # Daily at 2 AM
            'BACKUP_RETENTION_DAYS': 30,
            'BACKUP_COMPRESSION': True
        }
        
        # Apply defaults only if not already set
        for key, value in defaults.items():
            if key not in self.config_data:
                self.config_data[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with dot notation support"""
        try:
            # Support dot notation (e.g., 'database.timeout')
            if '.' in key:
                keys = key.split('.')
                value = self.config_data
                for k in keys:
                    if isinstance(value, dict) and k in value:
                        value = value[k]
                    else:
                        return default
                return value
            else:
                return self.config_data.get(key, default)
        except Exception:
            return default
    
    def set(self, key: str, value: Any):
        """Set configuration value with dot notation support"""
        try:
            if '.' in key:
                keys = key.split('.')
                config = self.config_data
                for k in keys[:-1]:
                    if k not in config:
                        config[k] = {}
                    config = config[k]
                config[keys[-1]] = value
            else:
                self.config_data[key] = value
        except Exception as e:
            logging.error(f"Error setting config key {key}: {e}")
    
    def get_database_config(self) -> Dict:
        """Get database configuration"""
        return {
            'path': self.get('DATABASE_PATH'),
            'timeout': self.get('DATABASE_TIMEOUT'),
            'check_same_thread': False
        }
    
    def get_api_keys(self) -> Dict:
        """Get all API keys"""
        return {
            'autotrader': self.get('AUTOTRADER_API_KEY'),
            'motors': self.get('MOTORS_API_KEY'),
            'cargurus': self.get('CARGURUS_API_KEY'),
            'uss': self.get('USS_API_KEY'),
            'ju': self.get('JU_API_KEY'),
            'aucnet': self.get('AUCNET_API_KEY'),
            'dvla': self.get('DVLA_API_KEY'),
            'gov_data': self.get('GOV_DATA_API_KEY'),
            'dft': self.get('DFT_API_KEY'),
            'tfl': self.get('TFL_API_KEY'),
            'abi': self.get('ABI_API_KEY'),
            'dvsa': self.get('DVSA_API_KEY'),
            'caz': self.get('CAZ_API_KEY'),
            'fixer': self.get('FIXER_API_KEY'),
            'xe': self.get('XE_API_KEY')
        }
    
    def get_rate_limits(self) -> Dict:
        """Get API rate limits"""
        return {
            'autotrader': self.get('AUTOTRADER_RATE_LIMIT'),
            'motors': self.get('MOTORS_RATE_LIMIT'),
            'cargurus': self.get('CARGURUS_RATE_LIMIT'),
            'uss': self.get('USS_RATE_LIMIT'),
            'ju': self.get('JU_RATE_LIMIT'),
            'aucnet': self.get('AUCNET_RATE_LIMIT')
        }
    
    def get_update_intervals(self) -> Dict:
        """Get data update intervals"""
        return {
            'uk_market_hours': self.get('UK_MARKET_UPDATE_INTERVAL'),
            'japan_auction_hours': self.get('JAPAN_AUCTION_UPDATE_INTERVAL'),
            'government_data_hours': self.get('GOVERNMENT_DATA_UPDATE_INTERVAL'),
            'exchange_rates_hours': self.get('EXCHANGE_RATE_UPDATE_INTERVAL')
        }
    
    def get_cost_structure(self) -> Dict:
        """Get cost calculation structure"""
        return {
            'japan': {
                'auction_fee_percent': self.get('JAPAN_AUCTION_FEE_PERCENT'),
                'transport_to_port_jpy': self.get('JAPAN_TRANSPORT_TO_PORT_JPY'),
                'export_certificate_jpy': self.get('JAPAN_EXPORT_CERTIFICATE_JPY'),
                'radiation_certificate_jpy': self.get('JAPAN_RADIATION_CERTIFICATE_JPY')
            },
            'shipping': {
                'base_freight_gbp': self.get('UK_BASE_FREIGHT_GBP'),
                'suv_surcharge_gbp': self.get('UK_SUV_SURCHARGE_GBP'),
                'truck_surcharge_gbp': self.get('UK_TRUCK_SURCHARGE_GBP'),
                'van_surcharge_gbp': self.get('UK_VAN_SURCHARGE_GBP')
            },
            'uk': {
                'import_duty_percent': self.get('UK_IMPORT_DUTY_PERCENT'),
                'vat_percent': self.get('UK_VAT_PERCENT'),
                'port_handling_gbp': self.get('UK_PORT_HANDLING_GBP'),
                'transport_from_port_gbp': self.get('UK_TRANSPORT_FROM_PORT_GBP'),
                'iva_test_gbp': self.get('UK_IVA_TEST_GBP'),
                'registration_fees_gbp': self.get('UK_REGISTRATION_FEES_GBP')
            },
            'conversion': {
                'speedometer_gbp': self.get('UK_SPEEDOMETER_CONVERSION_GBP'),
                'fog_lights_gbp': self.get('UK_FOG_LIGHTS_GBP'),
                'side_mirrors_gbp': self.get('UK_SIDE_MIRRORS_GBP'),
                'headlight_adjustment_gbp': self.get('UK_HEADLIGHT_ADJUSTMENT_GBP'),
                'compliance_modern_gbp': self.get('UK_COMPLIANCE_MODERN_GBP')
            }
        }
    
    def get_scoring_weights(self) -> Dict:
        """Get scoring algorithm weights"""
        return {
            'profit_margin': self.get('PROFIT_MARGIN_WEIGHT'),
            'roi': self.get('ROI_WEIGHT'),
            'risk': self.get('RISK_WEIGHT'),
            'demand': self.get('DEMAND_WEIGHT'),
            'speed': self.get('SPEED_WEIGHT')
        }
    
    def get_analysis_thresholds(self) -> Dict:
        """Get analysis thresholds"""
        return {
            'minimum_profit_margin': self.get('MINIMUM_PROFIT_MARGIN'),
            'minimum_roi': self.get('MINIMUM_ROI'),
            'maximum_days_to_sell': self.get('MAXIMUM_DAYS_TO_SELL'),
            'minimum_uk_listings': self.get('MINIMUM_UK_LISTINGS'),
            'minimum_japan_auctions': self.get('MINIMUM_JAPAN_AUCTIONS'),
            'maximum_vehicle_age': self.get('MAXIMUM_VEHICLE_AGE')
        }
    
    def get_alert_thresholds(self) -> Dict:
        """Get alert thresholds"""
        return {
            'high_profit_margin': self.get('HIGH_PROFIT_MARGIN_THRESHOLD'),
            'low_competition_threshold': self.get('LOW_COMPETITION_THRESHOLD'),
            'fast_selling_days': self.get('FAST_SELLING_DAYS_THRESHOLD'),
            'data_freshness_hours': self.get('DATA_FRESHNESS_HOURS')
        }
    
    def get_dashboard_config(self) -> Dict:
        """Get dashboard configuration"""
        return {
            'port': self.get('DASHBOARD_PORT'),
            'host': self.get('DASHBOARD_HOST'),
            'debug': self.get('DASHBOARD_DEBUG'),
            'auto_refresh_minutes': self.get('DASHBOARD_AUTO_REFRESH_MINUTES'),
            'pagination': {
                'default_page_size': self.get('DEFAULT_PAGE_SIZE'),
                'max_page_size': self.get('MAX_PAGE_SIZE')
            }
        }
    
    def get_logging_config(self) -> Dict:
        """Get logging configuration"""
        return {
            'level': self.get('LOG_LEVEL'),
            'file_max_size_mb': self.get('LOG_FILE_MAX_SIZE_MB'),
            'backup_count': self.get('LOG_BACKUP_COUNT'),
            'log_directory': self.get('LOG_DIRECTORY')
        }
    
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.get('FLASK_ENV', '').lower() == 'production'
    
    def is_debug(self) -> bool:
        """Check if debug mode is enabled"""
        return self.get('FLASK_DEBUG', False) or self.get('DASHBOARD_DEBUG', False)
    
    def validate_config(self) -> List[str]:
        """Validate configuration and return list of issues"""
        issues = []
        
        # Check required API keys (warn if missing)
        critical_apis = ['AUTOTRADER_API_KEY', 'USS_API_KEY']
        for api_key in critical_apis:
            if not self.get(api_key):
                issues.append(f"Missing critical API key: {api_key}")
        
        # Check database path
        db_path = self.get('DATABASE_PATH')
        if db_path:
            db_dir = os.path.dirname(db_path)
            if db_dir and not os.path.exists(db_dir):
                try:
                    os.makedirs(db_dir, exist_ok=True)
                except OSError as e:
                    issues.append(f"Cannot create database directory {db_dir}: {e}")
        
        # Check log directory
        log_dir = self.get('LOG_DIRECTORY')
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir, exist_ok=True)
            except OSError as e:
                issues.append(f"Cannot create log directory {log_dir}: {e}")
        
        # Validate numeric ranges
        numeric_validations = [
            ('DASHBOARD_PORT', 1, 65535),
            ('DATABASE_TIMEOUT', 1, 300),
            ('MINIMUM_PROFIT_MARGIN', 0, 100),
            ('UK_VAT_PERCENT', 0, 50),
            ('API_TIMEOUT_SECONDS', 1, 300)
        ]
        
        for key, min_val, max_val in numeric_validations:
            value = self.get(key)
            if value is not None:
                try:
                    num_value = float(value)
                    if not (min_val <= num_value <= max_val):
                        issues.append(f"{key} value {num_value} not in valid range [{min_val}, {max_val}]")
                except (ValueError, TypeError):
                    issues.append(f"{key} is not a valid number: {value}")
        
        return issues
    
    def save_config(self, filepath: Optional[str] = None) -> bool:
        """Save current configuration to file"""
        try:
            save_path = filepath or self.config_file
            
            # Ensure directory exists
            config_dir = os.path.dirname(save_path)
            if config_dir:
                os.makedirs(config_dir, exist_ok=True)
            
            # Don't save sensitive information
            safe_config = self.config_data.copy()
            sensitive_keys = [
                key for key in safe_config.keys() 
                if 'API_KEY' in key or 'SECRET' in key or 'PASSWORD' in key
            ]
            
            for key in sensitive_keys:
                if safe_config[key]:
                    safe_config[key] = '[REDACTED]'
            
            with open(save_path, 'w', encoding='utf-8') as file:
                yaml.dump(safe_config, file, default_flow_style=False, sort_keys=True)
            
            logging.info(f"Configuration saved to {save_path}")
            return True
            
        except Exception as e:
            logging.error(f"Error saving configuration: {e}")
            return False
    
    def reload_config(self):
        """Reload configuration from file and environment"""
        self.config_data.clear()
        self.load_config(self.config_file)
        self.load_env_variables()
        self._setup_defaults()
        logging.info("Configuration reloaded")
    
    def get_all_config(self) -> Dict:
        """Get all configuration (for debugging, excludes sensitive data)"""
        safe_config = self.config_data.copy()
        
        # Redact sensitive information
        for key in safe_config.keys():
            if any(sensitive in key.upper() for sensitive in ['API_KEY', 'SECRET', 'PASSWORD', 'TOKEN']):
                if safe_config[key]:
                    safe_config[key] = '[REDACTED]'
        
        return safe_config
    
    def __str__(self) -> str:
        """String representation of config (safe)"""
        return f"Config(file={self.config_file}, keys={len(self.config_data)})"
    
    def __repr__(self) -> str:
        """Detailed string representation"""
        return f"Config(file='{self.config_file}', loaded_keys={list(self.config_data.keys())})"