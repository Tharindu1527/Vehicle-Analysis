# scripts/data_pipeline.py
#!/usr/bin/env python3
"""
Manual data pipeline execution script
"""
import asyncio
import sys
import os
from datetime import datetime

# Add the project root to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data_processing.data_collector import DataCollector
from src.data_processing.scoring_engine import ScoringEngine
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

async def run_data_pipeline(make=None, model=None):
    """Run the complete data pipeline"""
    try:
        logger.info("Starting manual data pipeline execution")
        start_time = datetime.now()
        
        # Initialize components
        data_collector = DataCollector()
        scoring_engine = ScoringEngine()
        
        # Collect data from all sources
        logger.info("Collecting UK market data...")
        uk_data = await data_collector.collect_uk_market_data(make, model)
        
        logger.info("Collecting Japan auction data...")
        japan_data = await data_collector.collect_japan_auction_data(make, model)
        
        logger.info("Collecting government data...")
        gov_data = await data_collector.collect_government_data()
        
        logger.info("Collecting exchange rates...")
        exchange_rates = await data_collector.collect_exchange_rates()
        
        # Run analysis
        logger.info("Running profitability analysis...")
        results = await scoring_engine.analyze_profitability(uk_data, japan_data, gov_data)
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
        logger.info(f"Collected {len(uk_data)} UK market records")
        logger.info(f"Collected {len(japan_data)} Japan auction records")
        logger.info(f"Collected {len(gov_data)} government data records")
        logger.info(f"Generated {len(results)} profitability analyses")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in data pipeline: {str(e)}")
        raise

async def main():
    """Main function with command line argument parsing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Run Vehicle Import Analysis Pipeline')
    parser.add_argument('--make', help='Filter by vehicle make')
    parser.add_argument('--model', help='Filter by vehicle model')
    parser.add_argument('--setup-db', action='store_true', help='Setup database first')
    
    args = parser.parse_args()
    
    if args.setup_db:
        logger.info("Setting up database...")
        from scripts.setup_database import setup_database
        await setup_database()
    
    # Run pipeline
    results = await run_data_pipeline(args.make, args.model)
    
    # Display top results
    if results:
        logger.info("\nTop 10 Opportunities:")
        for i, result in enumerate(results[:10], 1):
            logger.info(
                f"{i}. {result.get('make')} {result.get('model')} ({result.get('year')}) - "
                f"Margin: {result.get('profit_margin_percent', 0):.1f}% - "
                f"Score: {result.get('final_recommendation_score', 0):.0f}"
            )

if __name__ == "__main__":
    asyncio.run(main())