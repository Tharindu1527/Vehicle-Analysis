# scripts/setup_database.py
#!/usr/bin/env python3
"""
Database setup and initialization script
"""
import asyncio
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database.models import DatabaseSchema
from src.utils.config import Config
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

async def setup_database():
    """Initialize database with all required tables"""
    try:
        config = Config()
        db_path = config.get('DATABASE_PATH', 'vehicle_import_analyzer.db')
        
        logger.info(f"Setting up database at: {db_path}")
        
        # Create database directory if it doesn't exist
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            logger.info(f"Created database directory: {db_dir}")
        
        # Create all tables
        await DatabaseSchema.create_tables(db_path)
        
        logger.info("Database setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(setup_database())



