import os
import yaml
from typing import Any, Dict, Optional

class Config:
    """Configuration manager"""
    
    def __init__(self, config_file: str = 'config/config.yaml'):
        self.config_data = {}
        self.load_config(config_file)
        self.load_env_variables()
    
    def load_config(self, config_file: str):
        """Load configuration from YAML file"""
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r') as file:
                    self.config_data = yaml.safe_load(file) or {}
        except Exception as e:
            print(f"Warning: Could not load config file {config_file}: {e}")
            self.config_data = {}
    
    def load_env_variables(self):
        """Load configuration from environment variables"""
        env_mapping = {
            'DATABASE_PATH': 'DATABASE_PATH',
            'AUTOTRADER_API_KEY': 'AUTOTRADER_API_KEY',
            'MOTORS_API_KEY': 'MOTORS_API_KEY',
            'CARGURUS_API_KEY': 'CARGURUS_API_KEY',
            'USS_API_KEY': 'USS_API_KEY',
            'JU_API_KEY': 'JU_API_KEY',
            'AUCNET_API_KEY': 'AUCNET_API_KEY',
            'DVLA_API_KEY': 'DVLA_API_KEY',
            'GOV_DATA_API_KEY': 'GOV_DATA_API_KEY',
            'DFT_API_KEY': 'DFT_API_KEY',
            'TFL_API_KEY': 'TFL_API_KEY',
            'FIXER_API_KEY': 'FIXER_API_KEY',
            'XE_API_KEY': 'XE_API_KEY',
            'SECRET_KEY': 'SECRET_KEY'
        }
        
        for config_key, env_key in env_mapping.items():
            env_value = os.getenv(env_key)
            if env_value:
                self.config_data[config_key] = env_value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self.config_data.get(key, default)
    
    def set(self, key: str, value: Any):
        """Set configuration value"""
        self.config_data[key] = value
    
    def get_database_config(self) -> Dict:
        """Get database configuration"""
        return {
            'path': self.get('DATABASE_PATH', 'vehicle_import_analyzer.db'),
            'timeout': self.get('DATABASE_TIMEOUT', 30),
            'check_same_thread': False
        }
    
    def get_api_keys(self) -> Dict:
        """Get all API keys"""
        return {
            'autotrader': self.get('AUTOTRADER_API_KEY'),
            'motors': self.get('MOTORS_API_KEY'),
            'cargurus': self.get('CARGURUS_API_KEY'),
            'uss': self.get('USS_API_KEY'),
            '