"""
Complete logging utility with file rotation and structured logging
"""
import logging
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any
import json
import traceback

class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured logging"""
    
    def format(self, record):
        """Format log record with structured data"""
        # Create base log data
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data['exception'] = {
                'type': record.exc_info[0].__name__,
                'message': str(record.exc_info[1]),
                'traceback': traceback.format_exception(*record.exc_info)
            }
        
        # Add extra fields if present
        if hasattr(record, 'extra_data'):
            log_data['extra'] = record.extra_data
        
        # Add process and thread info
        log_data['process_id'] = record.process
        log_data['thread_id'] = record.thread
        
        return json.dumps(log_data, ensure_ascii=False)

class ColoredConsoleFormatter(logging.Formatter):
    """Colored console formatter for better readability"""
    
    # Color codes
    COLORS = {
        'DEBUG': '\033[36m',      # Cyan
        'INFO': '\033[32m',       # Green
        'WARNING': '\033[33m',    # Yellow
        'ERROR': '\033[31m',      # Red
        'CRITICAL': '\033[35m',   # Magenta
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record):
        """Format with colors for console output"""
        # Get color for level
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        # Format timestamp
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
        
        # Create formatted message
        formatted = f"{color}[{timestamp}] {record.levelname:8s}{reset} "
        formatted += f"{record.name}:{record.funcName}:{record.lineno} - "
        formatted += f"{record.getMessage()}"
        
        # Add exception info if present
        if record.exc_info:
            formatted += f"\n{self.formatException(record.exc_info)}"
        
        return formatted

class VehicleAnalyzerLogger:
    """Main logger class for the Vehicle Analyzer application"""
    
    def __init__(self, name: str = 'vehicle_analyzer'):
        self.name = name
        self.logger = logging.getLogger(name)
        self.log_dir = 'logs'
        self.log_file = 'vehicle_analyzer.log'
        self.error_file = 'errors.log'
        self.max_file_size = 10 * 1024 * 1024  # 10MB
        self.backup_count = 5
        self._setup_logging()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        # Create logs directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Clear existing handlers
        self.logger.handlers.clear()
        
        # Set logging level
        log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
        self.logger.setLevel(getattr(logging, log_level, logging.INFO))
        
        # Console handler with colors
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = ColoredConsoleFormatter()
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # File handler for all logs
        file_path = os.path.join(self.log_dir, self.log_file)
        file_handler = logging.handlers.RotatingFileHandler(
            file_path,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = StructuredFormatter()
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)
        
        # Separate error file handler
        error_path = os.path.join(self.log_dir, self.error_file)
        error_handler = logging.handlers.RotatingFileHandler(
            error_path,
            maxBytes=self.max_file_size,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(file_formatter)
        self.logger.addHandler(error_handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
    
    def debug(self, message: str, extra_data: Optional[Dict] = None):
        """Log debug message"""
        self._log(logging.DEBUG, message, extra_data)
    
    def info(self, message: str, extra_data: Optional[Dict] = None):
        """Log info message"""
        self._log(logging.INFO, message, extra_data)
    
    def warning(self, message: str, extra_data: Optional[Dict] = None):
        """Log warning message"""
        self._log(logging.WARNING, message, extra_data)
    
    def error(self, message: str, extra_data: Optional[Dict] = None, exc_info: bool = False):
        """Log error message"""
        self._log(logging.ERROR, message, extra_data, exc_info)
    
    def critical(self, message: str, extra_data: Optional[Dict] = None, exc_info: bool = False):
        """Log critical message"""
        self._log(logging.CRITICAL, message, extra_data, exc_info)
    
    def exception(self, message: str, extra_data: Optional[Dict] = None):
        """Log exception with traceback"""
        self._log(logging.ERROR, message, extra_data, exc_info=True)
    
    def _log(self, level: int, message: str, extra_data: Optional[Dict] = None, exc_info: bool = False):
        """Internal logging method"""
        if extra_data:
            # Create log record with extra data
            record = self.logger.makeRecord(
                name=self.logger.name,
                level=level,
                fn='',
                lno=0,
                msg=message,
                args=(),
                exc_info=sys.exc_info() if exc_info else None
            )
            record.extra_data = extra_data
            self.logger.handle(record)
        else:
            self.logger.log(level, message, exc_info=exc_info)
    
    def log_api_call(self, api_name: str, endpoint: str, status_code: int, 
                    response_time: float, error: Optional[str] = None):
        """Log API call with structured data"""
        extra_data = {
            'api_name': api_name,
            'endpoint': endpoint,
            'status_code': status_code,
            'response_time_ms': round(response_time * 1000, 2),
            'success': status_code < 400
        }
        
        if error:
            extra_data['error'] = error
        
        if status_code >= 400:
            self.error(f"API call failed: {api_name} {endpoint}", extra_data)
        else:
            self.info(f"API call successful: {api_name} {endpoint}", extra_data)
    
    def log_data_processing(self, operation: str, records_processed: int, 
                          duration: float, errors: int = 0):
        """Log data processing operation"""
        extra_data = {
            'operation': operation,
            'records_processed': records_processed,
            'duration_seconds': round(duration, 2),
            'records_per_second': round(records_processed / duration, 2) if duration > 0 else 0,
            'error_count': errors,
            'success_rate': round((records_processed - errors) / records_processed * 100, 2) if records_processed > 0 else 0
        }
        
        if errors > 0:
            self.warning(f"Data processing completed with errors: {operation}", extra_data)
        else:
            self.info(f"Data processing completed: {operation}", extra_data)
    
    def log_analysis_result(self, make: str, model: str, year: int, 
                          profit_margin: float, final_score: float, 
                          recommendation: str):
        """Log vehicle analysis result"""
        extra_data = {
            'vehicle': {
                'make': make,
                'model': model,
                'year': year
            },
            'analysis': {
                'profit_margin_percent': profit_margin,
                'final_score': final_score,
                'recommendation': recommendation
            },
            'analysis_timestamp': datetime.now().isoformat()
        }
        
        self.info(f"Analysis completed: {make} {model} ({year})", extra_data)
    
    def log_database_operation(self, operation: str, table: str, affected_rows: int, 
                             duration: float, error: Optional[str] = None):
        """Log database operation"""
        extra_data = {
            'operation': operation,
            'table': table,
            'affected_rows': affected_rows,
            'duration_ms': round(duration * 1000, 2)
        }
        
        if error:
            extra_data['error'] = error
            self.error(f"Database operation failed: {operation} on {table}", extra_data)
        else:
            self.debug(f"Database operation: {operation} on {table}", extra_data)
    
    def log_system_metrics(self, cpu_percent: float, memory_percent: float, 
                          disk_usage_percent: float):
        """Log system performance metrics"""
        extra_data = {
            'system_metrics': {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'disk_usage_percent': disk_usage_percent,
                'timestamp': datetime.now().isoformat()
            }
        }
        
        # Log warnings for high resource usage
        if cpu_percent > 80:
            self.warning("High CPU usage detected", extra_data)
        elif memory_percent > 80:
            self.warning("High memory usage detected", extra_data)
        elif disk_usage_percent > 80:
            self.warning("High disk usage detected", extra_data)
        else:
            self.debug("System metrics collected", extra_data)
    
    def log_user_action(self, user_id: Optional[str], action: str, 
                       resource: str, success: bool, details: Optional[Dict] = None):
        """Log user action for audit trail"""
        extra_data = {
            'user_id': user_id or 'anonymous',
            'action': action,
            'resource': resource,
            'success': success,
            'timestamp': datetime.now().isoformat()
        }
        
        if details:
            extra_data['details'] = details
        
        if success:
            self.info(f"User action: {action} on {resource}", extra_data)
        else:
            self.warning(f"User action failed: {action} on {resource}", extra_data)

# Global logger instances
_loggers: Dict[str, VehicleAnalyzerLogger] = {}

def setup_logger(name: str = 'vehicle_analyzer') -> VehicleAnalyzerLogger:
    """Setup and return logger instance"""
    if name not in _loggers:
        _loggers[name] = VehicleAnalyzerLogger(name)
    return _loggers[name]

def get_logger(name: str = 'vehicle_analyzer') -> VehicleAnalyzerLogger:
    """Get existing logger instance"""
    if name not in _loggers:
        return setup_logger(name)
    return _loggers[name]

# Performance monitoring decorator
def log_performance(operation_name: str):
    """Decorator to log function performance"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = get_logger()
            start_time = datetime.now()
            
            try:
                result = func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds()
                
                logger.debug(f"Performance: {operation_name}", {
                    'function': func.__name__,
                    'duration_seconds': round(duration, 3),
                    'success': True
                })
                
                return result
                
            except Exception as e:
                duration = (datetime.now() - start_time).total_seconds()
                
                logger.error(f"Performance: {operation_name} failed", {
                    'function': func.__name__,
                    'duration_seconds': round(duration, 3),
                    'error': str(e),
                    'success': False
                }, exc_info=True)
                
                raise
        
        return wrapper
    return decorator

# Context manager for logging operations
class LoggedOperation:
    """Context manager for logging operations with timing"""
    
    def __init__(self, operation_name: str, logger_name: str = 'vehicle_analyzer'):
        self.operation_name = operation_name
        self.logger = get_logger(logger_name)
        self.start_time = None
        self.extra_data = {}
    
    def __enter__(self):
        self.start_time = datetime.now()
        self.logger.info(f"Starting operation: {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()
        self.extra_data['duration_seconds'] = round(duration, 3)
        
        if exc_type is None:
            self.logger.info(f"Completed operation: {self.operation_name}", self.extra_data)
        else:
            self.extra_data['error'] = str(exc_val)
            self.logger.error(f"Failed operation: {self.operation_name}", self.extra_data, exc_info=True)
    
    def add_data(self, key: str, value: Any):
        """Add extra data to be logged"""
        self.extra_data[key] = value
    
    def update_data(self, data: Dict):
        """Update extra data dictionary"""
        self.extra_data.update(data)

# Utility functions
def configure_logging_from_config(config: Dict):
    """Configure logging from configuration dictionary"""
    log_level = config.get('LOG_LEVEL', 'INFO')
    log_dir = config.get('LOG_DIRECTORY', 'logs')
    
    # Update environment variable
    os.environ['LOG_LEVEL'] = log_level
    
    # Setup main logger with new configuration
    logger = setup_logger()
    logger.log_dir = log_dir
    logger._setup_logging()
    
    logger.info("Logging configuration updated", {
        'log_level': log_level,
        'log_directory': log_dir
    })

def cleanup_old_logs(days_to_keep: int = 30):
    """Clean up old log files"""
    import glob
    from pathlib import Path
    
    logger = get_logger()
    log_dir = Path(logger.log_dir)
    
    if not log_dir.exists():
        return
    
    cutoff_time = datetime.now().timestamp() - (days_to_keep * 24 * 3600)
    deleted_count = 0
    
    for log_file in log_dir.glob('*.log*'):
        if log_file.stat().st_mtime < cutoff_time:
            try:
                log_file.unlink()
                deleted_count += 1
            except OSError as e:
                logger.warning(f"Could not delete old log file {log_file}: {e}")
    
    if deleted_count > 0:
        logger.info(f"Cleaned up {deleted_count} old log files")

# Example usage and testing
if __name__ == "__main__":
    # Setup logger
    logger = setup_logger()
    
    # Test different log levels
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    
    # Test structured logging
    logger.info("API call completed", {
        'api': 'autotrader',
        'endpoint': '/vehicles/search',
        'status_code': 200,
        'response_time': 0.245
    })
    
    # Test exception logging
    try:
        raise ValueError("This is a test exception")
    except ValueError:
        logger.exception("Caught test exception")
    
    # Test performance decorator
    @log_performance("test_function")
    def test_function():
        import time
        time.sleep(0.1)
        return "completed"
    
    result = test_function()
    
    # Test context manager
    with LoggedOperation("test_operation") as op:
        op.add_data("records_processed", 100)
        import time
        time.sleep(0.05)
    
    print("Logging test completed. Check the logs directory for output files.")