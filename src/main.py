"""
Main application entry point for Vehicle Import Analysis System
"""
import asyncio
import logging
from datetime import datetime, timedelta
import schedule
import time
from threading import Thread

from src.utils.config import Config
from src.utils.logger import setup_logger
from src.data_processing.data_collector import DataCollector
from src.data_processing.scoring_engine import ScoringEngine
from src.dashboard.app import create_app

logger = setup_logger(__name__)

class VehicleImportAnalyzer:
    def __init__(self):
        self.config = Config()
        self.data_collector = DataCollector()
        self.scoring_engine = ScoringEngine()
        
    async def run_analysis_pipeline(self):
        """Main analysis pipeline"""
        try:
            logger.info("Starting vehicle import analysis pipeline")
            
            # Collect data from all sources
            uk_data = await self.data_collector.collect_uk_market_data()
            japan_data = await self.data_collector.collect_japan_auction_data()
            gov_data = await self.data_collector.collect_government_data()
            
            # Process and analyze data
            results = await self.scoring_engine.analyze_profitability(
                uk_data, japan_data, gov_data
            )
            
            logger.info(f"Analysis completed. Found {len(results)} profitable opportunities")
            return results
            
        except Exception as e:
            logger.error(f"Error in analysis pipeline: {str(e)}")
            raise

    def schedule_updates(self):
        """Schedule regular data updates"""
        schedule.every(6).hours.do(self.run_scheduled_update)
        schedule.every().day.at("00:00").do(self.run_daily_analysis)
        
        while True:
            schedule.run_pending()
            time.sleep(60)

    def run_scheduled_update(self):
        """Run scheduled data updates"""
        asyncio.run(self.run_analysis_pipeline())

    def run_daily_analysis(self):
        """Run comprehensive daily analysis"""
        asyncio.run(self.run_analysis_pipeline())

if __name__ == "__main__":
    analyzer = VehicleImportAnalyzer()
    
    # Start Flask dashboard in separate thread
    app = create_app()
    dashboard_thread = Thread(target=lambda: app.run(host='0.0.0.0', port=5000))
    dashboard_thread.daemon = True
    dashboard_thread.start()
    
    # Start scheduled updates
    analyzer.schedule_updates()