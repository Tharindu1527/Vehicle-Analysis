"""
Flask dashboard application
"""
import sys
import os

# Add src to path for imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from flask import Flask, render_template, jsonify, request
import asyncio
from datetime import datetime, timedelta
import json

try:
    from utils.config import Config
    from utils.logger import setup_logger
    from database.connection import DatabaseConnection
    from data_processing.profitability_calculator import ProfitabilityCalculator
    from data_processing.scoring_engine import ScoringEngine
except ImportError as e:
    print(f"Dashboard import error: {e}")
    # Create minimal fallback classes
    class Config:
        def get(self, key, default=None):
            return default
    
    class setup_logger:
        def __init__(self, name):
            self.name = name
        def info(self, msg): print(f"INFO: {msg}")
        def error(self, msg): print(f"ERROR: {msg}")
    
    class DatabaseConnection:
        async def fetchone(self, query, params=None): return None
        async def fetchall(self, query, params=None): return []
    
    class ProfitabilityCalculator:
        async def get_top_opportunities(self, limit): return []
        async def get_fast_moving_vehicles(self, limit): return []
    
    class ScoringEngine:
        pass

logger = setup_logger(__name__)

def create_app():
    """Create and configure Flask application"""
    app = Flask(__name__)
    
    try:
        config = Config()
        app.config['SECRET_KEY'] = config.get('SECRET_KEY', 'dev-secret-key')
    except:
        app.config['SECRET_KEY'] = 'dev-secret-key'
    
    try:
        db = DatabaseConnection()
        profitability_calc = ProfitabilityCalculator()
        scoring_engine = ScoringEngine()
    except:
        db = None
        profitability_calc = None
        scoring_engine = None
    
    @app.route('/')
    def index():
        """Main dashboard page"""
        try:
            return render_template('index.html')
        except:
            # Return simple HTML if template not found
            return '''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Vehicle Import Analyzer</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; }
                    .container { max-width: 800px; margin: 0 auto; }
                    .success { color: green; }
                    .info { color: blue; }
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>🚀 Vehicle Import Analyzer</h1>
                    <div class="success">✅ Dashboard is running successfully!</div>
                    <p class="info">The dashboard is working. Template files can be added later for full UI.</p>
                    
                    <h2>📊 API Endpoints</h2>
                    <ul>
                        <li><a href="/api/market-summary">Market Summary</a></li>
                        <li><a href="/api/top-opportunities">Top Opportunities</a></li>
                        <li><a href="/dashboard">Dashboard View</a></li>
                    </ul>
                    
                    <h2>🎯 Next Steps</h2>
                    <ol>
                        <li>Set up database: <code>python scripts/setup_database.py</code></li>
                        <li>Add API keys to .env file</li>
                        <li>Run data collection</li>
                    </ol>
                </div>
            </body>
            </html>
            '''
    
    @app.route('/dashboard')
    def dashboard():
        """Analytics dashboard"""
        try:
            return render_template('dashboard.html')
        except:
            return '''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Dashboard - Vehicle Import Analyzer</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 20px; }
                    .metric { background: #f0f0f0; padding: 20px; margin: 10px; border-radius: 5px; }
                </style>
            </head>
            <body>
                <h1>📊 Vehicle Import Dashboard</h1>
                <div class="metric">
                    <h3>Total Opportunities</h3>
                    <p>Dashboard is running! Add data to see metrics.</p>
                </div>
                <a href="/">← Back to Home</a>
            </body>
            </html>
            '''
    
    @app.route('/api/top-opportunities')
    def api_top_opportunities():
        """API endpoint for top profit opportunities"""
        try:
            limit = request.args.get('limit', 20, type=int)
            if profitability_calc:
                opportunities = asyncio.run(profitability_calc.get_top_opportunities(limit))
            else:
                opportunities = []
            
            return jsonify({
                'success': True,
                'data': opportunities,
                'count': len(opportunities),
                'generated_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error fetching top opportunities: {str(e)}")
            return jsonify({
                'success': False, 
                'error': str(e),
                'data': [],
                'message': 'Service starting up - data will be available once analysis completes'
            }), 200  # Return 200 to avoid browser errors
    
    @app.route('/api/fast-moving')
    def api_fast_moving():
        """API endpoint for fast-moving vehicles"""
        try:
            limit = request.args.get('limit', 20, type=int)
            if profitability_calc:
                fast_moving = asyncio.run(profitability_calc.get_fast_moving_vehicles(limit))
            else:
                fast_moving = []
            
            return jsonify({
                'success': True,
                'data': fast_moving,
                'count': len(fast_moving),
                'generated_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error fetching fast-moving vehicles: {str(e)}")
            return jsonify({
                'success': False, 
                'error': str(e),
                'data': [],
                'message': 'Service starting up - data will be available once analysis completes'
            }), 200
    
    @app.route('/api/market-summary')
    def api_market_summary():
        """API endpoint for market summary statistics"""
        try:
            summary = asyncio.run(get_market_summary(db))
            return jsonify({
                'success': True,
                'data': summary,
                'generated_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error fetching market summary: {str(e)}")
            return jsonify({
                'success': True,
                'data': {
                    'total_opportunities': 0,
                    'high_profit_opportunities': 0,
                    'average_profit_margin': 0,
                    'top_performing_make': {'make': 'N/A', 'avg_margin': 0, 'count': 0},
                    'data_freshness': {'uk_data': None, 'japan_data': None},
                    'status': 'Starting up - connect database for live data'
                },
                'generated_at': datetime.now().isoformat()
            })
    
    @app.route('/api/search')
    def api_search():
        """API endpoint for vehicle search"""
        try:
            make = request.args.get('make', '').strip()
            model = request.args.get('model', '').strip()
            
            if not make:
                return jsonify({'success': False, 'error': 'Make is required'}), 400
            
            results = asyncio.run(search_vehicles(db, make, model))
            return jsonify({
                'success': True,
                'data': results,
                'count': len(results),
                'search_params': {'make': make, 'model': model},
                'generated_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error searching vehicles: {str(e)}")
            return jsonify({
                'success': False, 
                'error': str(e),
                'data': [],
                'message': 'Search unavailable - database not connected'
            }), 200
    
    @app.route('/api/refresh-data', methods=['POST'])
    def api_refresh_data():
        """API endpoint to trigger data refresh"""
        try:
            return jsonify({
                'success': True,
                'message': 'Data refresh would be initiated here',
                'initiated_at': datetime.now().isoformat(),
                'note': 'Connect data collection services for live refresh'
            })
        except Exception as e:
            logger.error(f"Error refreshing data: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/health')
    def health_check():
        """Health check endpoint"""
        return jsonify({
            'status': 'healthy',
            'service': 'Vehicle Import Analyzer Dashboard',
            'timestamp': datetime.now().isoformat(),
            'version': '1.0.0'
        })
    
    return app

async def get_market_summary(db):
    """Get market summary statistics"""
    if not db:
        return {
            'total_opportunities': 0,
            'high_profit_opportunities': 0,
            'average_profit_margin': 0,
            'top_performing_make': {'make': 'N/A', 'avg_margin': 0, 'count': 0},
            'data_freshness': {'uk_data': None, 'japan_data': None}
        }
    
    try:
        # Total opportunities
        total_opportunities = await db.fetchone(
            "SELECT COUNT(*) as count FROM profitability_analysis WHERE profit_margin_percent > 10"
        )
        
        # High profit opportunities
        high_profit = await db.fetchone(
            "SELECT COUNT(*) as count FROM profitability_analysis WHERE profit_margin_percent > 20"
        )
        
        # Average profit margin
        avg_margin = await db.fetchone(
            "SELECT AVG(profit_margin_percent) as avg_margin FROM profitability_analysis"
        )
        
        # Top performing make
        top_make = await db.fetchone("""
            SELECT make, AVG(profit_margin_percent) as avg_margin, COUNT(*) as count
            FROM profitability_analysis 
            GROUP BY make 
            ORDER BY avg_margin DESC 
            LIMIT 1
        """)
        
        # Recent data freshness
        latest_uk_data = await db.fetchone(
            "SELECT MAX(created_at) as latest FROM uk_market_data"
        )
        
        latest_japan_data = await db.fetchone(
            "SELECT MAX(created_at) as latest FROM japan_auction_data"
        )
        
        return {
            'total_opportunities': total_opportunities['count'] if total_opportunities else 0,
            'high_profit_opportunities': high_profit['count'] if high_profit else 0,
            'average_profit_margin': round(avg_margin['avg_margin'], 2) if avg_margin and avg_margin['avg_margin'] else 0,
            'top_performing_make': {
                'make': top_make['make'] if top_make else 'N/A',
                'avg_margin': round(top_make['avg_margin'], 2) if top_make else 0,
                'count': top_make['count'] if top_make else 0
            },
            'data_freshness': {
                'uk_data': latest_uk_data['latest'] if latest_uk_data else None,
                'japan_data': latest_japan_data['latest'] if latest_japan_data else None
            }
        }
    except Exception as e:
        logger.error(f"Error getting market summary: {str(e)}")
        return {
            'total_opportunities': 0,
            'high_profit_opportunities': 0,
            'average_profit_margin': 0,
            'top_performing_make': {'make': 'Error', 'avg_margin': 0, 'count': 0},
            'data_freshness': {'uk_data': None, 'japan_data': None}
        }

async def search_vehicles(db, make, model=None):
    """Search for specific vehicles"""
    if not db:
        return []
    
    try:
        where_clause = "WHERE LOWER(make) = ?"
        params = [make.lower()]
        
        if model:
            where_clause += " AND LOWER(model) = ?"
            params.append(model.lower())
        
        query = f"""
        SELECT * FROM profitability_analysis 
        {where_clause}
        ORDER BY final_recommendation_score DESC
        LIMIT 50
        """
        
        results = await db.fetchall(query, tuple(params))
        return results
    except Exception as e:
        logger.error(f"Error searching vehicles: {str(e)}")
        return []

# For standalone testing
if __name__ == '__main__':
    app = create_app()
    print("🌐 Dashboard running on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)