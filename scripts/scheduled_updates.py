# scripts/scheduled_updates.py
#!/usr/bin/env python3
"""
Scheduled update script for production deployment
"""
import asyncio
import sys
import os
import schedule
import time
from datetime import datetime, timedelta

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.main import VehicleImportAnalyzer
from src.utils.logger import setup_logger
from src.utils.config import Config

logger = setup_logger(__name__)

class ScheduledUpdater:
    """Handles scheduled data updates"""
    
    def __init__(self):
        self.analyzer = VehicleImportAnalyzer()
        self.config = Config()
        self.last_update_times = {}
        
    async def run_uk_market_update(self):
        """Update UK market data"""
        try:
            logger.info("Starting scheduled UK market data update")
            await self.analyzer.data_collector.collect_uk_market_data()
            self.last_update_times['uk_market'] = datetime.now()
            logger.info("UK market data update completed")
        except Exception as e:
            logger.error(f"Error in UK market update: {str(e)}")
    
    async def run_japan_auction_update(self):
        """Update Japan auction data"""
        try:
            logger.info("Starting scheduled Japan auction data update")
            await self.analyzer.data_collector.collect_japan_auction_data()
            self.last_update_times['japan_auction'] = datetime.now()
            logger.info("Japan auction data update completed")
        except Exception as e:
            logger.error(f"Error in Japan auction update: {str(e)}")
    
    async def run_government_data_update(self):
        """Update government data"""
        try:
            logger.info("Starting scheduled government data update")
            await self.analyzer.data_collector.collect_government_data()
            self.last_update_times['government'] = datetime.now()
            logger.info("Government data update completed")
        except Exception as e:
            logger.error(f"Error in government data update: {str(e)}")
    
    async def run_exchange_rate_update(self):
        """Update exchange rates"""
        try:
            logger.info("Starting scheduled exchange rate update")
            await self.analyzer.data_collector.collect_exchange_rates()
            self.last_update_times['exchange_rates'] = datetime.now()
            logger.info("Exchange rate update completed")
        except Exception as e:
            logger.error(f"Error in exchange rate update: {str(e)}")
    
    async def run_full_analysis(self):
        """Run complete analysis pipeline"""
        try:
            logger.info("Starting scheduled full analysis")
            await self.analyzer.run_analysis_pipeline()
            self.last_update_times['full_analysis'] = datetime.now()
            logger.info("Full analysis completed")
        except Exception as e:
            logger.error(f"Error in full analysis: {str(e)}")
    
    def setup_schedule(self):
        """Setup the update schedule"""
        # Get intervals from config
        uk_interval = self.config.get('data_collection.update_intervals.uk_market_hours', 6)
        japan_interval = self.config.get('data_collection.update_intervals.japan_auction_hours', 12)
        gov_interval = self.config.get('data_collection.update_intervals.government_data_hours', 24)
        rates_interval = self.config.get('data_collection.update_intervals.exchange_rates_hours', 1)
        
        # Schedule updates
        schedule.every(rates_interval).hours.do(self._async_job, self.run_exchange_rate_update)
        schedule.every(uk_interval).hours.do(self._async_job, self.run_uk_market_update)
        schedule.every(japan_interval).hours.do(self._async_job, self.run_japan_auction_update)
        schedule.every(gov_interval).hours.do(self._async_job, self.run_government_data_update)
        
        # Full analysis twice daily
        schedule.every().day.at("06:00").do(self._async_job, self.run_full_analysis)
        schedule.every().day.at("18:00").do(self._async_job, self.run_full_analysis)
        
        logger.info("Update schedule configured:")
        logger.info(f"  Exchange rates: every {rates_interval} hours")
        logger.info(f"  UK market data: every {uk_interval} hours")
        logger.info(f"  Japan auction data: every {japan_interval} hours")
        logger.info(f"  Government data: every {gov_interval} hours")
        logger.info("  Full analysis: daily at 06:00 and 18:00")
    
    def _async_job(self, coro):
        """Helper to run async jobs in scheduler"""
        try:
            asyncio.run(coro())
        except Exception as e:
            logger.error(f"Error in scheduled job: {str(e)}")
    
    def run_forever(self):
        """Run the scheduler forever"""
        logger.info("Starting scheduled updater...")
        self.setup_schedule()
        
        # Run initial updates
        logger.info("Running initial data collection...")
        self._async_job(self.run_exchange_rate_update)
        self._async_job(self.run_uk_market_update)
        self._async_job(self.run_japan_auction_update)
        self._async_job(self.run_government_data_update)
        self._async_job(self.run_full_analysis)
        
        # Start scheduler
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                logger.info("Scheduler stopped by user")
                break
            except Exception as e:
                logger.error(f"Scheduler error: {str(e)}")
                time.sleep(300)  # Wait 5 minutes before retrying

if __name__ == "__main__":
    updater = ScheduledUpdater()
    updater.run_forever()