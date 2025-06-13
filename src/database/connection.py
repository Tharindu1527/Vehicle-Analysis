"""
Database connection and management
"""
import aiosqlite
import asyncio
from typing import List, Dict, Any, Optional
import os
from contextlib import asynccontextmanager

from utils.logger import setup_logger
from utils.config import Config

logger = setup_logger(__name__)

class DatabaseConnection:
    def __init__(self):
        self.config = Config()
        self.db_path = self.config.get('DATABASE_PATH', 'vehicle_import_analyzer.db')
        self._connection = None
        
    async def connect(self):
        """Establish database connection"""
        try:
            self._connection = await aiosqlite.connect(self.db_path)
            self._connection.row_factory = aiosqlite.Row
            await self._connection.execute("PRAGMA foreign_keys = ON")
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    async def disconnect(self):
        """Close database connection"""
        if self._connection:
            await self._connection.close()
            self._connection = None
            logger.info("Database connection closed")

    async def execute(self, query: str, params: tuple = None) -> int:
        """Execute a query and return affected rows"""
        try:
            if not self._connection:
                await self.connect()
            
            cursor = await self._connection.execute(query, params or ())
            return cursor.rowcount
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise

    async def fetchone(self, query: str, params: tuple = None) -> Optional[Dict]:
        """Fetch one row"""
        try:
            if not self._connection:
                await self.connect()
                
            cursor = await self._connection.execute(query, params or ())
            row = await cursor.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(f"Error fetching one row: {str(e)}")
            return None

    async def fetchall(self, query: str, params: tuple = None) -> List[Dict]:
        """Fetch all rows"""
        try:
            if not self._connection:
                await self.connect()
                
            cursor = await self._connection.execute(query, params or ())
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error fetching all rows: {str(e)}")
            return []

    async def commit(self):
        """Commit transaction"""
        if self._connection:
            await self._connection.commit()

    async def rollback(self):
        """Rollback transaction"""
        if self._connection:
            await self._connection.rollback()

    @asynccontextmanager
    async def transaction(self):
        """Transaction context manager"""
        try:
            if not self._connection:
                await self.connect()
            yield self
            await self.commit()
        except Exception as e:
            await self.rollback()
            logger.error(f"Transaction rolled back: {str(e)}")
            raise