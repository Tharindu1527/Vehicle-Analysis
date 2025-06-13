"""
Dashboard package for Vehicle Import Analyzer
Provides web interface, API endpoints, and visualization components
"""

from .app import create_app

__version__ = "1.0.0"
__author__ = "Vehicle Import Analyzer Team"

# Package-level exports
__all__ = [
    'create_app',
    'create_dashboard',
    'run_dashboard',
    'get_dashboard_config',
    'DashboardError'
]

# Custom exceptions
class DashboardError(Exception):
    """Base exception for dashboard errors"""
    pass

class ConfigurationError(DashboardError):
    """Dashboard configuration error"""
    pass

class TemplateError(DashboardError):
    """Template rendering error"""
    pass

class APIError(DashboardError):
    """API endpoint error"""
    pass

# Dashboard configuration
DEFAULT_CONFIG = {
    'host': '0.0.0.0',
    'port': 5000,
    'debug': False,
    'threaded': True,
    'auto_refresh_minutes': 30,
    'pagination': {
        'default_page_size': 20,
        'max_page_size': 100
    },
    'cache_timeout': 300,  # 5 minutes
    'api_rate_limit': 1000,  # requests per hour
    'enable_cors': True,
    'static_folder': 'static',
    'template_folder': 'templates'
}

# Dashboard factory
def create_dashboard(config=None):
    """Create dashboard application with configuration"""
    from ..utils.config import Config
    from ..utils.logger import setup_logger
    
    logger = setup_logger('dashboard')
    
    try:
        # Load configuration
        app_config = Config()
        dashboard_config = app_config.get_dashboard_config()
        
        # Override with provided config
        if config:
            dashboard_config.update(config)
        
        # Create Flask app
        app = create_app()
        
        # Apply configuration
        app.config.update({
            'SECRET_KEY': app_config.get('SECRET_KEY'),
            'DEBUG': dashboard_config.get('debug', False),
            'TESTING': False,
            'DASHBOARD_CONFIG': dashboard_config
        })
        
        logger.info("Dashboard application created successfully")
        return app
        
    except Exception as e:
        logger.error(f"Failed to create dashboard: {str(e)}")
        raise ConfigurationError(f"Dashboard creation failed: {str(e)}") from e

def run_dashboard(app=None, **kwargs):
    """Run dashboard application"""
    from ..utils.logger import setup_logger
    
    logger = setup_logger('dashboard')
    
    if app is None:
        app = create_dashboard()
    
    # Get configuration
    config = app.config.get('DASHBOARD_CONFIG', DEFAULT_CONFIG)
    
    # Override with kwargs
    run_config = {
        'host': config.get('host', '0.0.0.0'),
        'port': config.get('port', 5000),
        'debug': config.get('debug', False),
        'threaded': config.get('threaded', True)
    }
    run_config.update(kwargs)
    
    try:
        logger.info(f"Starting dashboard on {run_config['host']}:{run_config['port']}")
        app.run(**run_config)
    except Exception as e:
        logger.error(f"Dashboard startup failed: {str(e)}")
        raise DashboardError(f"Failed to start dashboard: {str(e)}") from e

def get_dashboard_config():
    """Get dashboard configuration"""
    from ..utils.config import Config
    config = Config()
    return config.get_dashboard_config()

# API response helpers
class APIResponse:
    """Standard API response helper"""
    
    @staticmethod
    def success(data=None, message="Success", meta=None):
        """Create success response"""
        response = {
            'success': True,
            'message': message,
            'data': data or {}
        }
        if meta:
            response['meta'] = meta
        return response
    
    @staticmethod
    def error(message="Error occurred", error_code=None, details=None):
        """Create error response"""
        response = {
            'success': False,
            'message': message,
            'error': {
                'code': error_code,
                'details': details
            }
        }
        return response
    
    @staticmethod
    def paginated(data, page=1, per_page=20, total=0):
        """Create paginated response"""
        total_pages = (total + per_page - 1) // per_page if total > 0 else 1
        
        return {
            'success': True,
            'data': data,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': total,
                'total_pages': total_pages,
                'has_next': page < total_pages,
                'has_prev': page > 1
            }
        }

# Template utilities
class TemplateHelper:
    """Helper functions for templates"""
    
    @staticmethod
    def format_currency(amount, currency='GBP'):
        """Format currency for display"""
        from ..utils.helpers import format_currency
        return format_currency(amount, currency)
    
    @staticmethod
    def format_percentage(value, decimals=1):
        """Format percentage for display"""
        if value is None:
            return 'N/A'
        return f"{value:.{decimals}f}%"
    
    @staticmethod
    def format_score(score):
        """Format score for display"""
        if score is None:
            return 'N/A'
        return f"{score:.0f}"
    
    @staticmethod
    def get_recommendation_class(category):
        """Get CSS class for recommendation category"""
        classes = {
            'Highly Recommended': 'success',
            'Recommended': 'info',
            'Consider': 'warning',
            'Caution': 'warning',
            'Not Recommended': 'danger'
        }
        return classes.get(category, 'secondary')
    
    @staticmethod
    def get_priority_class(priority):
        """Get CSS class for priority"""
        classes = {
            'High': 'danger',
            'Medium': 'warning',
            'Low': 'info',
            'None': 'secondary'
        }
        return classes.get(priority, 'secondary')

# Dashboard metrics
class DashboardMetrics:
    """Dashboard performance metrics"""
    
    def __init__(self):
        self.metrics = {
            'page_views': 0,
            'api_calls': 0,
            'unique_sessions': set(),
            'avg_response_time': 0,
            'error_count': 0,
            'last_reset': None
        }
    
    def record_page_view(self, session_id=None):
        """Record page view"""
        self.metrics['page_views'] += 1
        if session_id:
            self.metrics['unique_sessions'].add(session_id)
    
    def record_api_call(self, response_time=None):
        """Record API call"""
        self.metrics['api_calls'] += 1
        if response_time:
            total_time = self.metrics['avg_response_time'] * (self.metrics['api_calls'] - 1)
            self.metrics['avg_response_time'] = (total_time + response_time) / self.metrics['api_calls']
    
    def record_error(self):
        """Record error"""
        self.metrics['error_count'] += 1
    
    def get_metrics(self):
        """Get current metrics"""
        metrics = self.metrics.copy()
        metrics['unique_sessions'] = len(self.metrics['unique_sessions'])
        return metrics
    
    def reset(self):
        """Reset metrics"""
        from datetime import datetime
        self.metrics = {
            'page_views': 0,
            'api_calls': 0,
            'unique_sessions': set(),
            'avg_response_time': 0,
            'error_count': 0,
            'last_reset': datetime.now().isoformat()
        }

# Cache utilities
class DashboardCache:
    """Simple dashboard cache"""
    
    def __init__(self, default_timeout=300):
        self.cache = {}
        self.default_timeout = default_timeout
    
    def get(self, key):
        """Get cached value"""
        import time
        
        if key in self.cache:
            value, timestamp, timeout = self.cache[key]
            if time.time() - timestamp < timeout:
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key, value, timeout=None):
        """Set cached value"""
        import time
        
        if timeout is None:
            timeout = self.default_timeout
        
        self.cache[key] = (value, time.time(), timeout)
    
    def delete(self, key):
        """Delete cached value"""
        if key in self.cache:
            del self.cache[key]
    
    def clear(self):
        """Clear all cached values"""
        self.cache.clear()

# WebSocket support (placeholder for future enhancement)
class WebSocketManager:
    """WebSocket manager for real-time updates"""
    
    def __init__(self):
        self.connections = set()
    
    def add_connection(self, connection):
        """Add WebSocket connection"""
        self.connections.add(connection)
    
    def remove_connection(self, connection):
        """Remove WebSocket connection"""
        self.connections.discard(connection)
    
    def broadcast(self, message):
        """Broadcast message to all connections"""
        for connection in self.connections.copy():
            try:
                connection.send(message)
            except:
                self.connections.discard(connection)

# Dashboard middleware
class DashboardMiddleware:
    """Custom middleware for dashboard"""
    
    def __init__(self, app, metrics=None):
        self.app = app
        self.metrics = metrics or DashboardMetrics()
    
    def __call__(self, environ, start_response):
        """WSGI middleware"""
        import time
        
        start_time = time.time()
        
        def custom_start_response(status, headers, exc_info=None):
            # Record metrics
            response_time = time.time() - start_time
            self.metrics.record_api_call(response_time)
            
            if status.startswith('4') or status.startswith('5'):
                self.metrics.record_error()
            
            return start_response(status, headers, exc_info)
        
        return self.app(environ, custom_start_response)

# Global instances
_dashboard_metrics = DashboardMetrics()
_dashboard_cache = DashboardCache()
_websocket_manager = WebSocketManager()

def get_metrics():
    """Get global dashboard metrics"""
    return _dashboard_metrics

def get_cache():
    """Get global dashboard cache"""
    return _dashboard_cache

def get_websocket_manager():
    """Get global WebSocket manager"""
    return _websocket_manager

# Security utilities
class SecurityHelper:
    """Security utilities for dashboard"""
    
    @staticmethod
    def sanitize_input(input_string):
        """Sanitize user input"""
        import html
        return html.escape(str(input_string).strip())
    
    @staticmethod
    def validate_api_key(api_key):
        """Validate API key (placeholder)"""
        # In production, implement proper API key validation
        return api_key is not None and len(api_key) > 10
    
    @staticmethod
    def check_rate_limit(client_ip, limit=100):
        """Check rate limit for client IP"""
        # In production, implement proper rate limiting
        return True

# Development utilities
def enable_dev_mode(app):
    """Enable development mode features"""
    app.config['DEBUG'] = True
    app.config['TESTING'] = True
    
    # Add development-specific routes or middleware
    @app.route('/dev/metrics')
    def dev_metrics():
        """Development metrics endpoint"""
        return get_metrics().get_metrics()
    
    @app.route('/dev/cache/clear')
    def dev_clear_cache():
        """Clear cache endpoint"""
        get_cache().clear()
        return {'status': 'cache cleared'}

# Package initialization
def initialize_package():
    """Initialize dashboard package"""
    from ..utils.logger import setup_logger
    logger = setup_logger('dashboard')
    logger.info("Dashboard package initialized")

# Auto-initialize on import
initialize_package()