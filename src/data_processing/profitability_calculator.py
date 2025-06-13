"""
Profitability calculation engine
Calculates profit margins and ROI for vehicle imports
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
import statistics

from src.utils.logger import setup_logger
from src.database.connection import DatabaseConnection

logger = setup_logger(__name__)

class ProfitabilityCalculator:
    def __init__(self):
        self.db = DatabaseConnection()
        
    async def calculate_profitability_matrix(self, make: str = None, model: str = None) -> List[Dict]:
        """Calculate comprehensive profitability matrix"""
        logger.info(f"Calculating profitability for {make} {model}" if make else "Calculating overall profitability")
        
        try:
            # Get matched vehicle data
            matched_data = await self._match_uk_japan_data(make, model)
            
            profitability_results = []
            
            for match in matched_data:
                profit_analysis = await self._calculate_individual_profit(match)
                if profit_analysis:
                    profitability_results.append(profit_analysis)
            
            # Sort by profit margin descending
            profitability_results.sort(key=lambda x: x.get('profit_margin_percent', 0), reverse=True)
            
            logger.info(f"Calculated profitability for {len(profitability_results)} vehicle matches")
            return profitability_results
            
        except Exception as e:
            logger.error(f"Error calculating profitability: {str(e)}")
            return []

    async def _match_uk_japan_data(self, make: str = None, model: str = None) -> List[Dict]:
        """Match UK and Japan data for same vehicles"""
        try:
            # Build dynamic query based on parameters
            where_clause = "WHERE 1=1"
            params = []
            
            if make:
                where_clause += " AND uk.make = ?"
                params.append(make)
            if model:
                where_clause += " AND uk.model = ?"
                params.append(model)
                
            query = f"""
            SELECT 
                uk.make, uk.model, uk.year, uk.fuel_type,
                AVG(uk.price) as avg_uk_price,
                MIN(uk.price) as min_uk_price,
                MAX(uk.price) as max_uk_price,
                COUNT(uk.id) as uk_listing_count,
                AVG(uk.days_listed) as avg_days_listed,
                AVG(uk.mileage) as avg_uk_mileage,
                
                AVG(jp.hammer_price) as avg_hammer_price,
                AVG(jp.total_landed_cost_gbp) as avg_landed_cost,
                MIN(jp.total_landed_cost_gbp) as min_landed_cost,
                MAX(jp.total_landed_cost_gbp) as max_landed_cost,
                COUNT(jp.id) as japan_auction_count,
                AVG(jp.mileage) as avg_japan_mileage,
                AVG(CASE WHEN jp.condition_grade ~ '^[0-9.]+$' 
                    THEN CAST(jp.condition_grade AS FLOAT) ELSE NULL END) as avg_condition_grade
                    
            FROM uk_market_data uk
            INNER JOIN japan_auction_data jp ON (
                LOWER(uk.make) = LOWER(jp.make) AND 
                LOWER(uk.model) = LOWER(jp.model) AND
                uk.year = jp.year AND
                uk.fuel_type = jp.fuel_type AND
                jp.total_landed_cost_gbp > 0
            )
            {where_clause}
            GROUP BY uk.make, uk.model, uk.year, uk.fuel_type
            HAVING COUNT(uk.id) >= 3 AND COUNT(jp.id) >= 3
            """
            
            results = await self.db.fetchall(query, params)
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error matching UK and Japan data: {str(e)}")
            return []

    async def _calculate_individual_profit(self, match_data: Dict) -> Optional[Dict]:
        """Calculate profit metrics for individual vehicle match"""
        try:
            avg_uk_price = match_data.get('avg_uk_price', 0)
            avg_landed_cost = match_data.get('avg_landed_cost', 0)
            min_landed_cost = match_data.get('min_landed_cost', 0)
            avg_days_listed = match_data.get('avg_days_listed', 30)
            
            if avg_uk_price <= 0 or avg_landed_cost <= 0:
                return None
                
            # Calculate profit metrics
            gross_profit = avg_uk_price - avg_landed_cost
            profit_margin_percent = (gross_profit / avg_uk_price) * 100
            roi_percent = (gross_profit / avg_landed_cost) * 100
            
            # Calculate best case scenario (using minimum costs)
            best_case_profit = avg_uk_price - min_landed_cost
            best_case_roi = (best_case_profit / min_landed_cost) * 100 if min_landed_cost > 0 else 0
            
            # Calculate annualized return
            days_to_sell = max(avg_days_listed, 1)
            annualized_roi = (roi_percent * 365) / days_to_sell
            
            # Risk assessment
            risk_score = await self._calculate_risk_score(match_data)
            
            # Market demand score
            demand_score = await self._calculate_demand_score(match_data)
            
            result = {
                'make': match_data.get('make'),
                'model': match_data.get('model'),
                'year': match_data.get('year'),
                'fuel_type': match_data.get('fuel_type'),
                
                # Price data
                'avg_uk_selling_price': round(avg_uk_price, 2),
                'avg_landed_cost': round(avg_landed_cost, 2),
                'min_landed_cost': round(min_landed_cost, 2),
                
                # Profit metrics
                'gross_profit': round(gross_profit, 2),
                'profit_margin_percent': round(profit_margin_percent, 2),
                'roi_percent': round(roi_percent, 2),
                'best_case_profit': round(best_case_profit, 2),
                'best_case_roi_percent': round(best_case_roi, 2),
                'annualized_roi_percent': round(annualized_roi, 2),
                
                # Market metrics
                'avg_days_to_sell': round(avg_days_listed, 1),
                'uk_listing_count': match_data.get('uk_listing_count', 0),
                'japan_auction_count': match_data.get('japan_auction_count', 0),
                'avg_condition_grade': round(match_data.get('avg_condition_grade', 0), 1),
                
                # Scores
                'risk_score': risk_score,
                'demand_score': demand_score,
                'overall_score': self._calculate_overall_score(
                    profit_margin_percent, roi_percent, risk_score, demand_score, avg_days_listed
                ),
                
                'last_calculated': datetime.now().isoformat()
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Error calculating individual profit: {str(e)}")
            return None

    async def _calculate_risk_score(self, match_data: Dict) -> float:
        """Calculate risk score (0-100, lower is better)"""
        risk_factors = []
        
        # Price volatility risk
        uk_price_range = match_data.get('max_uk_price', 0) - match_data.get('min_uk_price', 0)
        avg_price = match_data.get('avg_uk_price', 1)
        price_volatility = (uk_price_range / avg_price) * 100 if avg_price > 0 else 50
        risk_factors.append(min(price_volatility, 50))  # Cap at 50
        
        # Liquidity risk (based on listing count)
        uk_count = match_data.get('uk_listing_count', 0)
        if uk_count < 5:
            risk_factors.append(40)
        elif uk_count < 10:
            risk_factors.append(25)
        elif uk_count < 20:
            risk_factors.append(15)
        else:
            risk_factors.append(5)
            
        # Age risk
        current_year = datetime.now().year
        vehicle_age = current_year - match_data.get('year', current_year)
        if vehicle_age > 15:
            risk_factors.append(30)
        elif vehicle_age > 10:
            risk_factors.append(20)
        elif vehicle_age > 5:
            risk_factors.append(10)
        else:
            risk_factors.append(5)
            
        # Condition risk
        avg_condition = match_data.get('avg_condition_grade', 3.5)
        if avg_condition < 3.0:
            risk_factors.append(25)
        elif avg_condition < 3.5:
            risk_factors.append(15)
        elif avg_condition < 4.0:
            risk_factors.append(10)
        else:
            risk_factors.append(5)
            
        return round(sum(risk_factors) / len(risk_factors), 1)

    async def _calculate_demand_score(self, match_data: Dict) -> float:
        """Calculate demand score (0-100, higher is better)"""
        demand_factors = []
        
        # Listing frequency
        uk_count = match_data.get('uk_listing_count', 0)
        if uk_count > 50:
            demand_factors.append(90)
        elif uk_count > 30:
            demand_factors.append(80)
        elif uk_count > 20:
            demand_factors.append(70)
        elif uk_count > 10:
            demand_factors.append(60)
        else:
            demand_factors.append(40)
            
        # Days to sell (inverse relationship)
        avg_days = match_data.get('avg_days_listed', 30)
        if avg_days < 14:
            demand_factors.append(90)
        elif avg_days < 30:
            demand_factors.append(75)
        elif avg_days < 60:
            demand_factors.append(60)
        elif avg_days < 90:
            demand_factors.append(45)
        else:
            demand_factors.append(30)
            
        # Fuel type popularity
        fuel_type = match_data.get('fuel_type', '').lower()
        if fuel_type in ['hybrid', 'electric']:
            demand_factors.append(85)
        elif fuel_type == 'petrol':
            demand_factors.append(70)
        elif fuel_type == 'diesel':
            demand_factors.append(60)
        else:
            demand_factors.append(50)
            
        return round(sum(demand_factors) / len(demand_factors), 1)

    def _calculate_overall_score(self, profit_margin: float, roi: float, 
                               risk_score: float, demand_score: float, days_listed: float) -> float:
        """Calculate overall investment score (0-100)"""
        try:
            # Weighted scoring
            weights = {
                'profit_margin': 0.25,  # 25%
                'roi': 0.25,           # 25%
                'risk': 0.20,          # 20% (inverted)
                'demand': 0.20,        # 20%
                'speed': 0.10          # 10% (selling speed)
            }
            
            # Normalize profit margin (cap at 50%)
            profit_score = min(profit_margin * 2, 100)  # Scale 0-50% to 0-100
            
            # Normalize ROI (cap at 100%)
            roi_score = min(roi, 100)
            
            # Risk is inverted (lower risk = higher score)
            risk_score_inverted = 100 - risk_score
            
            # Speed score (faster selling = higher score)
            speed_score = max(0, 100 - (days_listed * 2))  # 50 days = 0 score
            
            overall = (
                profit_score * weights['profit_margin'] +
                roi_score * weights['roi'] +
                risk_score_inverted * weights['risk'] +
                demand_score * weights['demand'] +
                speed_score * weights['speed']
            )
            
            return round(overall, 1)
            
        except Exception as e:
            logger.error(f"Error calculating overall score: {str(e)}")
            return 0.0

    async def get_top_opportunities(self, limit: int = 20) -> List[Dict]:
        """Get top profit opportunities"""
        try:
            query = """
            SELECT * FROM profitability_analysis 
            WHERE profit_margin_percent > 10 
            AND overall_score > 60
            ORDER BY overall_score DESC, profit_margin_percent DESC
            LIMIT ?
            """
            
            results = await self.db.fetchall(query, (limit,))
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting top opportunities: {str(e)}")
            return []

    async def get_fast_moving_vehicles(self, limit: int = 20) -> List[Dict]:
        """Get fastest moving vehicles"""
        try:
            query = """
            SELECT * FROM profitability_analysis 
            WHERE avg_days_to_sell < 30 
            AND profit_margin_percent > 5
            ORDER BY avg_days_to_sell ASC, overall_score DESC
            LIMIT ?
            """
            
            results = await self.db.fetchall(query, (limit,))
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting fast moving vehicles: {str(e)}")
            return []