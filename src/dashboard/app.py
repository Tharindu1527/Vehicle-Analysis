"""
Flask dashboard application
"""
from flask import Flask, render_template, jsonify, request
import asyncio
from datetime import datetime, timedelta
import json

from src.utils.config import Config
from src.utils.logger import setup_logger
from src.database.connection import DatabaseConnection
from src.data_processing.profitability_calculator import ProfitabilityCalculator
from src.data_processing.scoring_engine import ScoringEngine

logger = setup_logger(__name__)

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = Config().get('SECRET_KEY', 'dev-secret-key')
    
    db = DatabaseConnection()
    profitability_calc = ProfitabilityCalculator()
    scoring_engine = ScoringEngine()
    
    @app.route('/')
    def index():
        """Main dashboard page"""
        return render_template('index.html')
    
    @app.route('/dashboard')
    def dashboard():
        """Analytics dashboard"""
        return render_template('dashboard.html')
    
    @app.route('/api/top-opportunities')
    def api_top_opportunities():
        """API endpoint for top profit opportunities"""
        try:
            limit = request.args.get('limit', 20, type=int)
            opportunities = asyncio.run(profitability_calc.get_top_opportunities(limit))
            return jsonify({
                'success': True,
                'data': opportunities,
                'count': len(opportunities),
                'generated_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error fetching top opportunities: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/fast-moving')
    def api_fast_moving():
        """API endpoint for fast-moving vehicles"""
        try:
            limit = request.args.get('limit', 20, type=int)
            fast_moving = asyncio.run(profitability_calc.get_fast_moving_vehicles(limit))
            return jsonify({
                'success': True,
                'data': fast_moving,
                'count': len(fast_moving),
                'generated_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error fetching fast-moving vehicles: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
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
            return jsonify({'success': False, 'error': str(e)}), 500
    
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
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/trends')
    def api_trends():
        """API endpoint for market trends"""
        try:
            period = request.args.get('period', '30', type=int)
            trends = asyncio.run(get_market_trends(db, period))
            return jsonify({
                'success': True,
                'data': trends,
                'period_days': period,
                'generated_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error fetching trends: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/refresh-data', methods=['POST'])
    def api_refresh_data():
        """API endpoint to trigger data refresh"""
        try:
            # This would trigger the main analysis pipeline
            # For now, return success
            return jsonify({
                'success': True,
                'message': 'Data refresh initiated',
                'initiated_at': datetime.now().isoformat()
            })
        except Exception as e:
            logger.error(f"Error refreshing data: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    return app

async def get_market_summary(db: DatabaseConnection) -> dict:
    """Get market summary statistics"""
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
        return {}

async def search_vehicles(db: DatabaseConnection, make: str, model: str = None) -> list:
    """Search for specific vehicles"""
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

async def get_market_trends(db: DatabaseConnection, period_days: int) -> dict:
    """Get market trends over specified period"""
    try:
        cutoff_date = (datetime.now() - timedelta(days=period_days)).isoformat()
        
        # Price trends
        price_trends = await db.fetchall("""
            SELECT 
                DATE(created_at) as date,
                AVG(price) as avg_price,
                COUNT(*) as listing_count
            FROM uk_market_data 
            WHERE created_at >= ?
            GROUP BY DATE(created_at)
            ORDER BY date
        """, (cutoff_date,))
        
        # Popular makes
        popular_makes = await db.fetchall("""
            SELECT 
                make,
                COUNT(*) as count,
                AVG(profit_margin_percent) as avg_margin
            FROM profitability_analysis
            GROUP BY make
            ORDER BY count DESC
            LIMIT 10
        """)
        
        # Auction volume trends
        auction_trends = await db.fetchall("""
            SELECT 
                DATE(created_at) as date,
                COUNT(*) as auction_count,
                AVG(hammer_price) as avg_hammer_price
            FROM japan_auction_data 
            WHERE created_at >= ?
            GROUP BY DATE(created_at)
            ORDER BY date
        """, (cutoff_date,))
        
        return {
            'price_trends': [dict(row) for row in price_trends],
            'popular_makes': [dict(row) for row in popular_makes],
            'auction_trends': [dict(row) for row in auction_trends]
        }
    except Exception as e:
        logger.error(f"Error getting market trends: {str(e)}")
        return {}