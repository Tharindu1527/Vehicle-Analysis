"""
Data Processing package for Vehicle Import Analyzer
Provides data collection, cleaning, analysis, and scoring capabilities
"""

from .data_collector import DataCollector
from .data_cleaner import DataCleaner
from .profitability_calculator import ProfitabilityCalculator
from .scoring_engine import ScoringEngine

__version__ = "1.0.0"
__author__ = "Vehicle Import Analyzer Team"

# Package-level exports
__all__ = [
    'DataCollector',
    'DataCleaner', 
    'ProfitabilityCalculator',
    'ScoringEngine',
    'create_analysis_pipeline',
    'run_complete_analysis',
    'get_processing_stats',
    'ProcessingError'
]

# Custom exceptions
class ProcessingError(Exception):
    """Base exception for data processing errors"""
    pass

class DataCollectionError(ProcessingError):
    """Error during data collection"""
    pass

class DataCleaningError(ProcessingError):
    """Error during data cleaning"""
    pass

class AnalysisError(ProcessingError):
    """Error during analysis"""
    pass

# Processing pipeline
class AnalysisPipeline:
    """Complete data analysis pipeline"""
    
    def __init__(self):
        self.collector = DataCollector()
        self.cleaner = DataCleaner()
        self.calculator = ProfitabilityCalculator()
        self.scorer = ScoringEngine()
        self.stats = {
            'total_processed': 0,
            'successful_analyses': 0,
            'errors': 0,
            'last_run': None
        }
    
    async def run_full_analysis(self, make=None, model=None):
        """Run complete analysis pipeline"""
        from datetime import datetime
        from ..utils.logger import setup_logger
        
        logger = setup_logger('data_processing')
        start_time = datetime.now()
        
        try:
            logger.info("Starting complete data analysis pipeline")
            
            # Step 1: Data Collection
            logger.info("Step 1: Collecting data from all sources")
            uk_data = await self.collector.collect_uk_market_data(make, model)
            japan_data = await self.collector.collect_japan_auction_data(make, model)
            gov_data = await self.collector.collect_government_data()
            exchange_rates = await self.collector.collect_exchange_rates()
            
            # Step 2: Data Cleaning
            logger.info("Step 2: Cleaning and validating data")
            clean_uk_data = self.cleaner.clean_uk_market_data(uk_data)
            clean_japan_data = self.cleaner.clean_japan_auction_data(japan_data)
            clean_gov_data = self.cleaner.clean_government_data(gov_data)
            
            # Step 3: Analysis
            logger.info("Step 3: Running profitability analysis")
            results = await self.scorer.analyze_profitability(
                clean_uk_data, clean_japan_data, clean_gov_data
            )
            
            # Update statistics
            duration = (datetime.now() - start_time).total_seconds()
            self.stats.update({
                'total_processed': len(results),
                'successful_analyses': len([r for r in results if r.get('final_recommendation_score', 0) > 0]),
                'last_run': start_time.isoformat(),
                'duration_seconds': duration
            })
            
            logger.info(f"Analysis completed successfully in {duration:.2f} seconds")
            logger.info(f"Processed {len(results)} opportunities")
            
            return results
            
        except Exception as e:
            self.stats['errors'] += 1
            logger.error(f"Analysis pipeline failed: {str(e)}")
            raise AnalysisError(f"Pipeline execution failed: {str(e)}") from e
    
    async def run_incremental_update(self):
        """Run incremental data update"""
        from datetime import datetime, timedelta
        from ..utils.logger import setup_logger
        
        logger = setup_logger('data_processing')
        
        try:
            logger.info("Starting incremental data update")
            
            # Get only recent data (last 7 days)
            cutoff_date = datetime.now() - timedelta(days=7)
            
            # Update UK market data
            uk_data = await self.collector.collect_uk_market_data()
            clean_uk_data = self.cleaner.clean_uk_market_data(uk_data)
            
            # Update exchange rates
            exchange_rates = await self.collector.collect_exchange_rates()
            
            logger.info("Incremental update completed")
            return {
                'uk_records_updated': len(clean_uk_data),
                'exchange_rates_updated': len(exchange_rates),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Incremental update failed: {str(e)}")
            raise DataCollectionError(f"Incremental update failed: {str(e)}") from e
    
    def get_stats(self):
        """Get processing statistics"""
        return self.stats.copy()

# Factory functions
def create_analysis_pipeline():
    """Create new analysis pipeline instance"""
    return AnalysisPipeline()

async def run_complete_analysis(make=None, model=None):
    """Run complete analysis using default pipeline"""
    pipeline = create_analysis_pipeline()
    return await pipeline.run_full_analysis(make, model)

def get_processing_stats():
    """Get processing statistics from default pipeline"""
    pipeline = create_analysis_pipeline()
    return pipeline.get_stats()

# Data validation utilities
def validate_uk_market_data(data):
    """Validate UK market data structure"""
    required_fields = ['make', 'model', 'year', 'price']
    errors = []
    
    for i, record in enumerate(data):
        for field in required_fields:
            if field not in record or record[field] is None:
                errors.append(f"Record {i}: Missing {field}")
    
    return errors

def validate_japan_auction_data(data):
    """Validate Japan auction data structure"""
    required_fields = ['make', 'model', 'year', 'hammer_price']
    errors = []
    
    for i, record in enumerate(data):
        for field in required_fields:
            if field not in record or record[field] is None:
                errors.append(f"Record {i}: Missing {field}")
    
    return errors

# Data processing utilities
class DataProcessor:
    """Utility class for common data processing operations"""
    
    @staticmethod
    def calculate_basic_stats(data, numeric_field):
        """Calculate basic statistics for numeric field"""
        if not data or numeric_field not in data[0]:
            return {}
        
        values = [record[numeric_field] for record in data if record.get(numeric_field) is not None]
        
        if not values:
            return {}
        
        values.sort()
        n = len(values)
        
        return {
            'count': n,
            'min': min(values),
            'max': max(values),
            'mean': sum(values) / n,
            'median': values[n // 2] if n % 2 == 1 else (values[n // 2 - 1] + values[n // 2]) / 2,
            'std_dev': (sum((x - sum(values) / n) ** 2 for x in values) / n) ** 0.5 if n > 1 else 0
        }
    
    @staticmethod
    def group_by_field(data, field):
        """Group data by specified field"""
        groups = {}
        for record in data:
            key = record.get(field, 'Unknown')
            if key not in groups:
                groups[key] = []
            groups[key].append(record)
        return groups
    
    @staticmethod
    def filter_by_criteria(data, criteria):
        """Filter data by criteria dictionary"""
        filtered = []
        for record in data:
            match = True
            for field, value in criteria.items():
                if record.get(field) != value:
                    match = False
                    break
            if match:
                filtered.append(record)
        return filtered

# Batch processing utilities
class BatchProcessor:
    """Utility for processing data in batches"""
    
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
    
    async def process_batches(self, data, processor_func):
        """Process data in batches"""
        results = []
        total_batches = (len(data) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            batch_number = (i // self.batch_size) + 1
            
            try:
                batch_results = await processor_func(batch)
                results.extend(batch_results)
            except Exception as e:
                print(f"Error processing batch {batch_number}/{total_batches}: {e}")
                continue
        
        return results

# Analysis result utilities
class AnalysisResultProcessor:
    """Utility for processing analysis results"""
    
    @staticmethod
    def filter_by_score(results, min_score=60):
        """Filter results by minimum score"""
        return [r for r in results if r.get('final_recommendation_score', 0) >= min_score]
    
    @staticmethod
    def filter_by_profit_margin(results, min_margin=10):
        """Filter results by minimum profit margin"""
        return [r for r in results if r.get('profit_margin_percent', 0) >= min_margin]
    
    @staticmethod
    def sort_by_recommendation(results):
        """Sort results by recommendation score (descending)"""
        return sorted(results, key=lambda x: x.get('final_recommendation_score', 0), reverse=True)
    
    @staticmethod
    def get_top_opportunities(results, limit=20):
        """Get top opportunities"""
        sorted_results = AnalysisResultProcessor.sort_by_recommendation(results)
        return sorted_results[:limit]
    
    @staticmethod
    def group_by_make(results):
        """Group results by vehicle make"""
        groups = {}
        for result in results:
            make = result.get('make', 'Unknown')
            if make not in groups:
                groups[make] = []
            groups[make].append(result)
        return groups
    
    @staticmethod
    def calculate_summary_stats(results):
        """Calculate summary statistics for results"""
        if not results:
            return {}
        
        profit_margins = [r.get('profit_margin_percent', 0) for r in results]
        roi_values = [r.get('roi_percent', 0) for r in results]
        scores = [r.get('final_recommendation_score', 0) for r in results]
        
        return {
            'total_opportunities': len(results),
            'avg_profit_margin': sum(profit_margins) / len(profit_margins),
            'avg_roi': sum(roi_values) / len(roi_values),
            'avg_score': sum(scores) / len(scores),
            'high_score_count': len([s for s in scores if s >= 80]),
            'profitable_count': len([m for m in profit_margins if m >= 15])
        }

# Export utilities
class DataExporter:
    """Utility for exporting processed data"""
    
    @staticmethod
    def to_csv(data, filename):
        """Export data to CSV"""
        import csv
        
        if not data:
            return False
        
        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            return True
        except Exception as e:
            print(f"Error exporting to CSV: {e}")
            return False
    
    @staticmethod
    def to_json(data, filename):
        """Export data to JSON"""
        import json
        
        try:
            with open(filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(data, jsonfile, indent=2, default=str, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error exporting to JSON: {e}")
            return False

# Performance monitoring
class ProcessingMonitor:
    """Monitor processing performance"""
    
    def __init__(self):
        self.metrics = {
            'total_runs': 0,
            'successful_runs': 0,
            'average_duration': 0,
            'total_records_processed': 0,
            'error_count': 0
        }
    
    def record_run(self, duration, records_processed, success=True):
        """Record processing run metrics"""
        self.metrics['total_runs'] += 1
        self.metrics['total_records_processed'] += records_processed
        
        if success:
            self.metrics['successful_runs'] += 1
            
            # Update average duration
            total_duration = self.metrics['average_duration'] * (self.metrics['successful_runs'] - 1)
            self.metrics['average_duration'] = (total_duration + duration) / self.metrics['successful_runs']
        else:
            self.metrics['error_count'] += 1
    
    def get_metrics(self):
        """Get performance metrics"""
        metrics = self.metrics.copy()
        if metrics['total_runs'] > 0:
            metrics['success_rate'] = metrics['successful_runs'] / metrics['total_runs'] * 100
        else:
            metrics['success_rate'] = 0
        return metrics

# Global instances
_pipeline = None
_monitor = ProcessingMonitor()

def get_pipeline():
    """Get global pipeline instance"""
    global _pipeline
    if _pipeline is None:
        _pipeline = AnalysisPipeline()
    return _pipeline

def get_monitor():
    """Get global monitor instance"""
    return _monitor

# Package initialization
def initialize_package():
    """Initialize data processing package"""
    from ..utils.logger import setup_logger
    logger = setup_logger('data_processing')
    logger.info("Data processing package initialized")

# Auto-initialize on import
initialize_package()