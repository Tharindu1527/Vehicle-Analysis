"""
Database package for Vehicle Import Analyzer
Provides database connection management, schema definitions, and data access layers
"""

from .connection import DatabaseConnection
from .models import DatabaseSchema

__version__ = "1.0.0"
__author__ = "Vehicle Import Analyzer Team"

# Package-level exports
__all__ = [
    'DatabaseConnection',
    'DatabaseSchema',
    'get_connection',
    'initialize_database',
    'create_all_tables',
    'get_database_info'
]

# Global connection instance
_connection = None

def get_connection():
    """Get global database connection instance"""
    global _connection
    if _connection is None:
        _connection = DatabaseConnection()
    return _connection

async def initialize_database(db_path=None):
    """Initialize database with schema"""
    from .models import DatabaseSchema
    from ..utils.config import Config
    
    config = Config()
    if db_path is None:
        db_path = config.get('DATABASE_PATH', 'vehicle_import_analyzer.db')
    
    # Create tables
    await DatabaseSchema.create_tables(db_path)
    
    # Initialize connection
    global _connection
    _connection = DatabaseConnection()
    await _connection.connect()
    
    return _connection

async def create_all_tables(db_path=None):
    """Create all database tables"""
    await DatabaseSchema.create_tables(db_path)

def get_database_info():
    """Get database configuration information"""
    from ..utils.config import Config
    config = Config()
    
    return {
        'database_path': config.get('DATABASE_PATH'),
        'timeout': config.get('DATABASE_TIMEOUT'),
        'backup_enabled': config.get('BACKUP_ENABLED'),
        'connection_active': _connection is not None
    }

# Database utilities
class DatabaseError(Exception):
    """Custom database exception"""
    pass

class ConnectionError(DatabaseError):
    """Database connection error"""
    pass

class SchemaError(DatabaseError):
    """Database schema error"""
    pass

# Context manager for database operations
class DatabaseTransaction:
    """Context manager for database transactions"""
    
    def __init__(self, connection=None):
        self.connection = connection or get_connection()
        self.committed = False
    
    async def __aenter__(self):
        if not self.connection._connection:
            await self.connection.connect()
        return self.connection
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None and not self.committed:
            await self.connection.commit()
            self.committed = True
        elif exc_type is not None:
            await self.connection.rollback()

# Data access helpers
async def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    """Execute database query with automatic connection management"""
    async with DatabaseTransaction() as db:
        if fetch_one:
            return await db.fetchone(query, params)
        elif fetch_all:
            return await db.fetchall(query, params)
        else:
            return await db.execute(query, params)

async def get_table_stats():
    """Get statistics for all tables"""
    stats = {}
    
    tables = [
        'uk_market_data',
        'japan_auction_data',
        'government_data',
        'exchange_rates',
        'profitability_analysis',
        'landed_cost_components',
        'market_intelligence'
    ]
    
    for table in tables:
        try:
            count_result = await execute_query(
                f"SELECT COUNT(*) as count FROM {table}",
                fetch_one=True
            )
            stats[table] = count_result['count'] if count_result else 0
        except Exception:
            stats[table] = 0
    
    return stats

async def cleanup_old_data(days_to_keep=90):
    """Clean up old data from database"""
    from datetime import datetime, timedelta
    
    cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).isoformat()
    
    cleanup_queries = [
        ("uk_market_data", "DELETE FROM uk_market_data WHERE created_at < ?"),
        ("japan_auction_data", "DELETE FROM japan_auction_data WHERE created_at < ?"),
        ("government_data", "DELETE FROM government_data WHERE created_at < ?"),
        ("exchange_rates", "DELETE FROM exchange_rates WHERE date_recorded < date(?)"),
    ]
    
    total_deleted = 0
    async with DatabaseTransaction() as db:
        for table_name, query in cleanup_queries:
            try:
                deleted = await db.execute(query, (cutoff_date,))
                total_deleted += deleted
            except Exception as e:
                print(f"Error cleaning {table_name}: {e}")
    
    return total_deleted

async def backup_database(backup_path=None):
    """Create database backup"""
    import shutil
    from ..utils.config import Config
    
    config = Config()
    source_path = config.get('DATABASE_PATH')
    
    if backup_path is None:
        from datetime import datetime
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = f"backup/vehicle_analyzer_{timestamp}.db"
    
    try:
        # Ensure backup directory exists
        import os
        backup_dir = os.path.dirname(backup_path)
        if backup_dir:
            os.makedirs(backup_dir, exist_ok=True)
        
        # Copy database file
        shutil.copy2(source_path, backup_path)
        return backup_path
    except Exception as e:
        raise DatabaseError(f"Backup failed: {e}")

async def restore_database(backup_path, target_path=None):
    """Restore database from backup"""
    import shutil
    from ..utils.config import Config
    
    if target_path is None:
        config = Config()
        target_path = config.get('DATABASE_PATH')
    
    try:
        # Close existing connection
        global _connection
        if _connection:
            await _connection.disconnect()
            _connection = None
        
        # Restore from backup
        shutil.copy2(backup_path, target_path)
        
        # Reinitialize connection
        _connection = DatabaseConnection()
        await _connection.connect()
        
        return True
    except Exception as e:
        raise DatabaseError(f"Restore failed: {e}")

# Database health check
async def health_check():
    """Perform database health check"""
    health = {
        'status': 'unknown',
        'connection': False,
        'tables': {},
        'last_data_update': None,
        'total_records': 0
    }
    
    try:
        # Test connection
        async with DatabaseTransaction() as db:
            health['connection'] = True
            
            # Check tables
            health['tables'] = await get_table_stats()
            health['total_records'] = sum(health['tables'].values())
            
            # Get last update time
            last_update = await db.fetchone(
                "SELECT MAX(created_at) as last_update FROM uk_market_data"
            )
            if last_update and last_update['last_update']:
                health['last_data_update'] = last_update['last_update']
            
            health['status'] = 'healthy'
            
    except Exception as e:
        health['status'] = f'error: {str(e)}'
    
    return health

# Migration utilities
class Migration:
    """Base class for database migrations"""
    
    def __init__(self, version, description):
        self.version = version
        self.description = description
    
    async def up(self, connection):
        """Apply migration"""
        raise NotImplementedError
    
    async def down(self, connection):
        """Rollback migration"""
        raise NotImplementedError

async def run_migrations(migrations=None):
    """Run database migrations"""
    if migrations is None:
        migrations = []
    
    async with DatabaseTransaction() as db:
        # Create migrations table if not exists
        await db.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                description TEXT,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Get applied migrations
        applied = await db.fetchall("SELECT version FROM schema_migrations")
        applied_versions = {row['version'] for row in applied}
        
        # Apply new migrations
        for migration in migrations:
            if migration.version not in applied_versions:
                try:
                    await migration.up(db)
                    await db.execute(
                        "INSERT INTO schema_migrations (version, description) VALUES (?, ?)",
                        (migration.version, migration.description)
                    )
                    print(f"Applied migration {migration.version}: {migration.description}")
                except Exception as e:
                    print(f"Migration {migration.version} failed: {e}")
                    raise

# Package initialization
def initialize_package():
    """Initialize database package"""
    from ..utils.logger import setup_logger
    logger = setup_logger('database')
    logger.info("Database package initialized")

# Auto-initialize on import
initialize_package()