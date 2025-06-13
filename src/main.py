"""
Main application entry point for Vehicle Import Analysis System
"""
import sys
import os
import asyncio
import logging
from datetime import datetime, timedelta
import schedule
import time
from threading import Thread

# Add current directory to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    from src.utils.config import Config
    from src.utils.logger import setup_logger
    from src.data_processing.data_collector import DataCollector
    from src.data_processing.scoring_engine import ScoringEngine
    from src.dashboard.app import create_app
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure all modules are in the src directory")
    sys.exit(1)

logger = setup_logger(__name__)

class VehicleImportAnalyzer:
    def __init__(self):
        try:
            self.config = Config()
            self.data_collector = DataCollector()
            self.scoring_engine = ScoringEngine()
            logger.info("VehicleImportAnalyzer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize VehicleImportAnalyzer: {e}")
            raise
        
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
        try:
            asyncio.run(self.run_analysis_pipeline())
        except Exception as e:
            logger.error(f"Scheduled update failed: {e}")

    def run_daily_analysis(self):
        """Run comprehensive daily analysis"""
        try:
            asyncio.run(self.run_analysis_pipeline())
        except Exception as e:
            logger.error(f"Daily analysis failed: {e}")

def main():
    """Main function"""
    try:
        print("🚀 Vehicle Import Analyzer Starting...")
        
        analyzer = VehicleImportAnalyzer()
        print("✅ Analyzer initialized")
        
        # Start Flask dashboard in separate thread
        app = create_app()
        print("✅ Dashboard created")
        
        dashboard_thread = Thread(
            target=lambda: app.run(host='0.0.0.0', port=5000, debug=False),
            daemon=True
        )
        dashboard_thread.start()
        
        print("🌐 Dashboard started on http://localhost:5000")
        print("   Press Ctrl+C to stop")
        
        # Start scheduled updates
        analyzer.schedule_updates()
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
        print("\n👋 Application stopped")
    except Exception as e:
        logger.error(f"Application failed to start: {str(e)}")
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()