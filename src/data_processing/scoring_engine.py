"""
Advanced scoring engine with ML capabilities
Provides comprehensive analysis and recommendations
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple, Any
from datetime import datetime, timedelta
import json
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import asyncio

from utils.logger import setup_logger
from database.connection import DatabaseConnection
from data_processing.profitability_calculator import ProfitabilityCalculator

logger = setup_logger(__name__)

class ScoringEngine:
    """Advanced scoring engine with machine learning capabilities"""
    
    def __init__(self):
        self.db = DatabaseConnection()
        self.profitability_calc = ProfitabilityCalculator()
        self.ml_model = None
        self.scaler = StandardScaler()
        self._initialize_ml_model()
    
    def _initialize_ml_model(self):
        """Initialize the ML model for scoring"""
        try:
            self.ml_model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            logger.info("ML model initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing ML model: {str(e)}")
    
    async def analyze_profitability(self, uk_data: List[Dict], japan_data: List[Dict], 
                                  gov_data: List[Dict]) -> List[Dict]:
        """Complete profitability analysis with enhanced scoring"""
        logger.info("Starting comprehensive profitability analysis")
        
        try:
            # Get base profitability calculations
            base_results = await self.profitability_calc.calculate_profitability_matrix()
            
            enhanced_results = []
            
            for result in base_results:
                # Enhance with market intelligence
                enhanced_result = await self._enhance_with_market_intelligence(result, gov_data)
                
                # Calculate ML score
                enhanced_result['ml_score'] = self._calculate_ml_score_for_result(enhanced_result)
                
                # Calculate final recommendation score
                enhanced_result['final_recommendation_score'] = self._calculate_final_score(enhanced_result)
                
                # Generate recommendations
                enhanced_result.update(self._generate_recommendations(enhanced_result))
                
                # Add confidence metrics
                enhanced_result.update(self._calculate_confidence_interval(enhanced_result))
                
                # Generate actionable insights
                enhanced_result['action_items'] = self._generate_action_items(enhanced_result)
                enhanced_result['risk_warnings'] = self._generate_risk_warnings(enhanced_result)
                enhanced_result['timing_recommendations'] = self._generate_timing_recommendations(enhanced_result)
                
                enhanced_results.append(enhanced_result)
            
            # Sort by final recommendation score
            enhanced_results.sort(key=lambda x: x.get('final_recommendation_score', 0), reverse=True)
            
            # Store results in database
            await self._store_analysis_results(enhanced_results)
            
            logger.info(f"Completed analysis for {len(enhanced_results)} opportunities")
            return enhanced_results
            
        except Exception as e:
            logger.error(f"Error in profitability analysis: {str(e)}")
            return []
    
    async def _enhance_with_market_intelligence(self, result: Dict, gov_data: List[Dict]) -> Dict:
        """Enhance result with market intelligence data"""
        try:
            make = result.get('make', '')
            model = result.get('model', '')
            year = result.get('year', 0)
            
            # Add government registration data
            registration_data = self._get_registration_trends(make, model, year, gov_data)
            result['registration_trend'] = registration_data
            
            # Add ULEZ compliance check
            result['ulez_compliant'] = await self._check_ulez_compliance(make, model, year)
            
            # Add seasonal patterns
            result['seasonal_factors'] = await self._analyze_seasonal_patterns(make, model)
            
            # Add competition analysis
            result['competition_analysis'] = await self._analyze_competition(make, model, year)
            
            # Add market volatility assessment
            result['market_volatility'] = await self._assess_market_volatility(make, model)
            
            # Add supply chain risk assessment
            result['supply_chain_risk'] = await self._assess_supply_chain_risk(make, model)
            
            return result
            
        except Exception as e:
            logger.error(f"Error enhancing with market intelligence: {str(e)}")
            return result
    
    def _get_registration_trends(self, make: str, model: str, year: int, gov_data: List[Dict]) -> Dict:
        """Analyze registration trends from government data"""
        try:
            relevant_data = [
                d for d in gov_data 
                if (d.get('make', '').lower() == make.lower() and 
                    d.get('model', '').lower() == model.lower())
            ]
            
            if not relevant_data:
                return {'trend': 'unknown', 'confidence': 'low', 'data_points': 0}
            
            # Calculate trend
            monthly_counts = {}
            for item in relevant_data:
                month = item.get('month', 0)
                count = item.get('registration_count', 0)
                if month and count:
                    monthly_counts[month] = count
            
            if len(monthly_counts) < 3:
                return {'trend': 'insufficient_data', 'confidence': 'low', 'data_points': len(monthly_counts)}
            
            # Simple trend calculation
            months = sorted(monthly_counts.keys())
            counts = [monthly_counts[m] for m in months]
            
            # Linear trend
            if len(counts) >= 2:
                slope = (counts[-1] - counts[0]) / len(counts)
                if slope > 10:
                    trend = 'growing'
                elif slope < -10:
                    trend = 'declining'
                else:
                    trend = 'stable'
            else:
                trend = 'stable'
            
            return {
                'trend': trend,
                'confidence': 'high' if len(monthly_counts) > 6 else 'medium',
                'data_points': len(monthly_counts),
                'total_registrations': sum(counts),
                'monthly_average': sum(counts) / len(counts)
            }
            
        except Exception as e:
            logger.error(f"Error calculating registration trends: {str(e)}")
            return {'trend': 'unknown', 'confidence': 'low', 'data_points': 0}
    
    async def _check_ulez_compliance(self, make: str, model: str, year: int) -> Dict:
        """Check ULEZ compliance (simplified logic for demo)"""
        try:
            # Simplified ULEZ compliance logic
            # Real implementation would use TfL API or emissions database
            
            compliance = {
                'ulez_compliant': False,
                'charge_amount': 12.50,
                'compliance_date': None,
                'exemption_code': None
            }
            
            # Basic rules (simplified)
            if year >= 2016:  # Most cars from 2016+ are compliant
                compliance['ulez_compliant'] = True
                compliance['charge_amount'] = 0
            elif year >= 2006:  # Euro 4+ petrol cars
                compliance['ulez_compliant'] = True
                compliance['charge_amount'] = 0
            
            # Electric and hybrid vehicles are always compliant
            # This would be determined by fuel_type in real data
            
            return compliance
            
        except Exception as e:
            logger.error(f"Error checking ULEZ compliance: {str(e)}")
            return {'ulez_compliant': False, 'charge_amount': 12.50}
    
    async def _analyze_seasonal_patterns(self, make: str, model: str) -> Dict:
        """Analyze seasonal buying patterns"""
        try:
            # This would analyze historical data to find seasonal patterns
            # For demo, using simplified logic
            
            seasonal_data = {
                'seasonal_pattern': 'unknown',
                'peak_months': [],
                'low_months': [],
                'seasonal_factor': 1.0
            }
            
            # Some basic seasonal assumptions
            luxury_brands = ['bmw', 'mercedes', 'audi', 'lexus', 'porsche']
            economy_brands = ['toyota', 'honda', 'nissan', 'ford', 'vauxhall']
            
            if make.lower() in luxury_brands:
                seasonal_data.update({
                    'seasonal_pattern': 'winter_peak',
                    'peak_months': ['November', 'December', 'January'],
                    'low_months': ['July', 'August'],
                    'seasonal_factor': 1.15
                })
            elif make.lower() in economy_brands:
                seasonal_data.update({
                    'seasonal_pattern': 'spring_peak',
                    'peak_months': ['March', 'April', 'May'],
                    'low_months': ['December', 'January'],
                    'seasonal_factor': 1.05
                })
            
            return seasonal_data
            
        except Exception as e:
            logger.error(f"Error analyzing seasonal patterns: {str(e)}")
            return {'seasonal_pattern': 'unknown', 'seasonal_factor': 1.0}
    
    async def _analyze_competition(self, make: str, model: str, year: int) -> Dict:
        """Analyze competition levels"""
        try:
            # Get competitor count from UK market data
            query = """
            SELECT COUNT(DISTINCT make || ' ' || model) as competitor_count,
                   COUNT(*) as total_listings,
                   AVG(price) as avg_competitor_price
            FROM uk_market_data 
            WHERE year = ? AND fuel_type = (
                SELECT fuel_type FROM uk_market_data 
                WHERE make = ? AND model = ? AND year = ? 
                LIMIT 1
            )
            AND NOT (make = ? AND model = ?)
            """
            
            result = await self.db.fetchone(query, (year, make, model, year, make, model))
            
            if result:
                competitor_count = result.get('competitor_count', 0)
                total_listings = result.get('total_listings', 0)
                avg_competitor_price = result.get('avg_competitor_price', 0)
                
                # Determine competition level
                if competitor_count < 5:
                    competition_level = 'low'
                elif competitor_count < 15:
                    competition_level = 'medium'
                else:
                    competition_level = 'high'
                
                return {
                    'competition_level': competition_level,
                    'competitor_count': competitor_count,
                    'total_competitor_listings': total_listings,
                    'avg_competitor_price': avg_competitor_price
                }
            
            return {'competition_level': 'unknown', 'competitor_count': 0}
            
        except Exception as e:
            logger.error(f"Error analyzing competition: {str(e)}")
            return {'competition_level': 'medium', 'competitor_count': 0}
    
    async def _assess_market_volatility(self, make: str, model: str) -> Dict:
        """Assess market price volatility"""
        try:
            # Calculate price volatility from UK market data
            query = """
            SELECT AVG(price) as avg_price,
                   MIN(price) as min_price,
                   MAX(price) as max_price,
                   COUNT(*) as price_points
            FROM uk_market_data 
            WHERE make = ? AND model = ?
            AND created_at >= date('now', '-90 days')
            """
            
            result = await self.db.fetchone(query, (make, model))
            
            if result and result.get('price_points', 0) > 5:
                avg_price = result.get('avg_price', 0)
                min_price = result.get('min_price', 0)
                max_price = result.get('max_price', 0)
                
                if avg_price > 0:
                    volatility_ratio = (max_price - min_price) / avg_price
                    
                    if volatility_ratio < 0.15:
                        volatility = 'low'
                    elif volatility_ratio < 0.30:
                        volatility = 'medium'
                    else:
                        volatility = 'high'
                    
                    return {
                        'volatility': volatility,
                        'volatility_ratio': round(volatility_ratio, 3),
                        'price_range': max_price - min_price,
                        'data_points': result.get('price_points')
                    }
            
            return {'volatility': 'unknown', 'data_points': 0}
            
        except Exception as e:
            logger.error(f"Error assessing market volatility: {str(e)}")
            return {'volatility': 'medium'}
    
    async def _assess_supply_chain_risk(self, make: str, model: str) -> Dict:
        """Assess supply chain risks"""
        try:
            # Assess based on Japan auction availability
            query = """
            SELECT COUNT(*) as auction_count,
                   AVG(hammer_price) as avg_hammer_price,
                   COUNT(DISTINCT auction_house) as auction_house_count
            FROM japan_auction_data 
            WHERE make = ? AND model = ?
            AND created_at >= date('now', '-90 days')
            """
            
            result = await self.db.fetchone(query, (make, model))
            
            if result:
                auction_count = result.get('auction_count', 0)
                auction_house_count = result.get('auction_house_count', 0)
                
                # Determine supply risk
                if auction_count >= 15 and auction_house_count >= 3:
                    supply_risk = 'low'
                elif auction_count >= 8 and auction_house_count >= 2:
                    supply_risk = 'medium'
                else:
                    supply_risk = 'high'
                
                return {
                    'supply_risk': supply_risk,
                    'recent_auction_count': auction_count,
                    'auction_house_diversity': auction_house_count,
                    'avg_hammer_price': result.get('avg_hammer_price', 0)
                }
            
            return {'supply_risk': 'high', 'recent_auction_count': 0}
            
        except Exception as e:
            logger.error(f"Error assessing supply chain risk: {str(e)}")
            return {'supply_risk': 'medium'}
    
    def _calculate_ml_score_for_result(self, result: Dict) -> float:
        """Calculate ML-based score for a result"""
        try:
            # Extract features for ML model
            features = self._extract_features_for_ml(result)
            
            if self.ml_model and len(features) > 0:
                # Use trained model if available
                features_array = np.array(features).reshape(1, -1)
                scaled_features = self.scaler.fit_transform(features_array)
                ml_score = self.ml_model.predict(scaled_features)[0]
                return max(0, min(100, ml_score))  # Ensure 0-100 range
            else:
                # Fallback to rule-based scoring
                return self._calculate_ml_score(features)
                
        except Exception as e:
            logger.error(f"Error calculating ML score: {str(e)}")
            return result.get('overall_score', 50.0)
    
    def _extract_features_for_ml(self, result: Dict) -> List[float]:
        """Extract numerical features for ML model"""
        try:
            features = [
                result.get('profit_margin_percent', 0),
                result.get('roi_percent', 0),
                result.get('avg_days_to_sell', 30),
                result.get('uk_listing_count', 0),
                result.get('japan_auction_count', 0),
                result.get('risk_score', 50),
                result.get('demand_score', 50),
                1 if result.get('ulez_compliant', {}).get('ulez_compliant') else 0,
                result.get('registration_trend', {}).get('total_registrations', 0) / 100,  # Normalized
                self._encode_trend(result.get('registration_trend', {}).get('trend', 'unknown')),
                self._encode_competition(result.get('competition_analysis', {}).get('competition_level', 'medium')),
                datetime.now().year - result.get('year', datetime.now().year),  # vehicle age
                self._encode_volatility(result.get('market_volatility', {}).get('volatility', 'medium')),
                self._encode_supply_risk(result.get('supply_chain_risk', {}).get('supply_risk', 'medium'))
            ]
            
            return features
            
        except Exception as e:
            logger.error(f"Error extracting ML features: {str(e)}")
            return []
    
    def _encode_trend(self, trend: str) -> float:
        """Encode trend as numerical value"""
        encoding = {
            'growing': 1.0,
            'stable': 0.5,
            'declining': 0.0,
            'unknown': 0.5,
            'insufficient_data': 0.3
        }
        return encoding.get(trend, 0.5)
    
    def _encode_competition(self, competition: str) -> float:
        """Encode competition level as numerical value"""
        encoding = {
            'low': 1.0,
            'medium': 0.5,
            'high': 0.0,
            'unknown': 0.5
        }
        return encoding.get(competition, 0.5)
    
    def _encode_volatility(self, volatility: str) -> float:
        """Encode volatility as numerical value"""
        encoding = {
            'low': 1.0,
            'medium': 0.5,
            'high': 0.0,
            'unknown': 0.5
        }
        return encoding.get(volatility, 0.5)
    
    def _encode_supply_risk(self, supply_risk: str) -> float:
        """Encode supply risk as numerical value"""
        encoding = {
            'low': 1.0,
            'medium': 0.5,
            'high': 0.0
        }
        return encoding.get(supply_risk, 0.5)
    
    def _calculate_ml_score(self, features: List[float]) -> float:
        """Rule-based ML score calculation"""
        if not features or len(features) < 14:
            return 50.0
        
        try:
            # Weighted scoring based on features
            weights = [
                0.20,  # profit_margin_percent
                0.20,  # roi_percent
                0.10,  # avg_days_to_sell (inverted)
                0.05,  # uk_listing_count
                0.05,  # japan_auction_count
                0.10,  # risk_score (inverted)
                0.10,  # demand_score
                0.05,  # ulez_compliant
                0.03,  # regional_concentration
                0.05,  # registration_trend
                0.03,  # competition_level
                0.02,  # vehicle_age (inverted)
                0.01,  # market_volatility (inverted)
                0.01   # supply_chain_risk (inverted)
            ]
            
            # Normalize and score features
            normalized_features = []
            
            # Profit margin (0-50% -> 0-100 score)
            normalized_features.append(min(100, features[0] * 2))
            
            # ROI (0-100% -> 0-100 score)
            normalized_features.append(min(100, features[1]))
            
            # Days to sell (inverted: less days = higher score)
            normalized_features.append(max(0, 100 - features[2] * 2))
            
            # Listing counts (log scaled)
            normalized_features.append(min(100, np.log1p(features[3]) * 20))
            normalized_features.append(min(100, np.log1p(features[4]) * 25))
            
            # Risk score (inverted)
            normalized_features.append(100 - features[5])
            
            # Demand score (direct)
            normalized_features.append(features[6])
            
            # Binary features (0-1 -> 0-100)
            for i in range(7, len(features)):
                normalized_features.append(features[i] * 100)
            
            # Vehicle age (inverted: newer = better)
            if len(normalized_features) > 11:
                normalized_features[11] = max(0, 100 - normalized_features[11] * 5)
            
            # Calculate weighted score
            score = sum(nf * w for nf, w in zip(normalized_features, weights[:len(normalized_features)]))
            
            return max(0, min(100, score))
            
        except Exception as e:
            logger.error(f"Error in ML score calculation: {str(e)}")
            return 50.0
    
    def _calculate_final_score(self, result: Dict) -> float:
        """Calculate final recommendation score"""
        try:
            # Base scores
            overall_score = result.get('overall_score', 50)
            ml_score = result.get('ml_score', 50)
            
            # Weighted combination
            base_score = (overall_score * 0.6) + (ml_score * 0.4)
            
            # Bonus/penalty factors
            bonus_factors = 0
            penalty_factors = 0
            
            # ULEZ compliance bonus
            if result.get('ulez_compliant', {}).get('ulez_compliant'):
                bonus_factors += 5
            else:
                penalty_factors += 3
            
            # Registration trend factor
            trend = result.get('registration_trend', {}).get('trend', 'unknown')
            if trend == 'growing':
                bonus_factors += 3
            elif trend == 'declining':
                penalty_factors += 5
            
            # Competition factor
            competition = result.get('competition_analysis', {}).get('competition_level', 'medium')
            if competition == 'low':
                bonus_factors += 4
            elif competition == 'high':
                penalty_factors += 3
            
            # Volatility factor
            volatility = result.get('market_volatility', {}).get('volatility', 'medium')
            if volatility == 'low':
                bonus_factors += 2
            elif volatility == 'high':
                penalty_factors += 4
            
            # Supply chain risk factor
            supply_risk = result.get('supply_chain_risk', {}).get('supply_risk', 'medium')
            if supply_risk == 'low':
                bonus_factors += 3
            elif supply_risk == 'high':
                penalty_factors += 5
            
            # Calculate final score
            final_score = base_score + bonus_factors - penalty_factors
            
            return max(0, min(100, final_score))
            
        except Exception as e:
            logger.error(f"Error calculating final score: {str(e)}")
            return result.get('overall_score', 50)
    
    def _generate_recommendations(self, result: Dict) -> Dict:
        """Generate recommendation category and priority"""
        try:
            final_score = result.get('final_recommendation_score', 50)
            
            # Recommendation category
            if final_score >= 80:
                category = 'Highly Recommended'
                priority = 'High'
            elif final_score >= 70:
                category = 'Recommended'
                priority = 'High'
            elif final_score >= 60:
                category = 'Consider'
                priority = 'Medium'
            elif final_score >= 50:
                category = 'Caution'
                priority = 'Low'
            else:
                category = 'Not Recommended'
                priority = 'None'
            
            return {
                'recommendation_category': category,
                'priority': priority
            }
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {str(e)}")
            return {'recommendation_category': 'Consider', 'priority': 'Medium'}
    
    def _calculate_confidence_interval(self, result: Dict) -> Dict:
        """Calculate confidence metrics"""
        try:
            confidence_factors = []
            
            # Data quantity factors
            uk_count = result.get('uk_listing_count', 0)
            japan_count = result.get('japan_auction_count', 0)
            
            if uk_count >= 10 and japan_count >= 5:
                confidence_factors.append(0.9)
            elif uk_count >= 5 and japan_count >= 3:
                confidence_factors.append(0.7)
            else:
                confidence_factors.append(0.4)
            
            # Market intelligence confidence
            reg_confidence = result.get('registration_trend', {}).get('confidence', 'low')
            if reg_confidence == 'high':
                confidence_factors.append(0.8)
            elif reg_confidence == 'medium':
                confidence_factors.append(0.6)
            else:
                confidence_factors.append(0.3)
            
            # Volatility factor
            volatility = result.get('market_volatility', {}).get('volatility', 'unknown')
            if volatility == 'low':
                confidence_factors.append(0.8)
            elif volatility == 'medium':
                confidence_factors.append(0.6)
            else:
                confidence_factors.append(0.3)
            
            # Calculate overall confidence
            avg_confidence = sum(confidence_factors) / len(confidence_factors)
            
            # Confidence level
            if avg_confidence >= 0.75:
                confidence_level = 'High'
                margin_of_error = 5.0
            elif avg_confidence >= 0.55:
                confidence_level = 'Medium'
                margin_of_error = 10.0
            else:
                confidence_level = 'Low'
                margin_of_error = 15.0
            
            final_score = result.get('final_recommendation_score', 50)
            
            return {
                'confidence_level': confidence_level,
                'confidence_score': round(avg_confidence * 100, 1),
                'margin_of_error': margin_of_error,
                'lower_bound': max(0, final_score - margin_of_error),
                'upper_bound': min(100, final_score + margin_of_error)
            }
            
        except Exception as e:
            logger.error(f"Error calculating confidence interval: {str(e)}")
            return {'confidence_level': 'Medium', 'margin_of_error': 10.0}
    
    def _generate_action_items(self, result: Dict) -> List[str]:
        """Generate actionable recommendations"""
        actions = []
        
        try:
            # High profit opportunities
            if result.get('profit_margin_percent', 0) > 25:
                actions.append("Prioritize this vehicle - exceptional profit potential identified")
            
            # Fast-moving vehicles
            if result.get('avg_days_to_sell', 60) < 15:
                actions.append("Fast-moving vehicle - consider immediate action to secure inventory")
            
            # Low competition
            if result.get('competition_analysis', {}).get('competition_level') == 'low':
                actions.append("Low competition detected - opportunity for market leadership")
            
            # ULEZ compliance
            if result.get('ulez_compliant', {}).get('ulez_compliant'):
                actions.append("ULEZ compliant - market this as a key selling point")
            
            # Seasonal timing
            seasonal_pattern = result.get('seasonal_factors', {}).get('seasonal_pattern')
            if seasonal_pattern == 'winter_peak':
                actions.append("Consider timing imports for October-December peak season")
            elif seasonal_pattern == 'spring_peak':
                actions.append("Plan imports for February-April optimal selling period")
            
            # Supply chain
            if result.get('supply_chain_risk', {}).get('supply_risk') == 'low':
                actions.append("Stable supply chain - consider volume purchasing strategy")
            
            return actions
            
        except Exception as e:
            logger.error(f"Error generating action items: {str(e)}")
            return ["Review detailed analysis for specific recommendations"]
    
    def _generate_risk_warnings(self, result: Dict) -> List[str]:
        """Generate risk warnings"""
        warnings = []
        
        try:
            # Market volatility
            if result.get('market_volatility', {}).get('volatility') == 'high':
                warnings.append("High price volatility detected - monitor market closely")
            
            # Supply chain risks
            if result.get('supply_chain_risk', {}).get('supply_risk') == 'high':
                warnings.append("Limited auction supply - secure inventory quickly when available")
            
            # High competition
            if result.get('competition_analysis', {}).get('competition_level') == 'high':
                warnings.append("High competition - ensure competitive pricing strategy")
            
            # Declining trend
            if result.get('registration_trend', {}).get('trend') == 'declining':
                warnings.append("Declining registration trend - monitor demand carefully")
            
            # High risk score
            if result.get('risk_score', 50) > 70:
                warnings.append("High overall risk score - consider additional due diligence")
            
            # Slow selling
            if result.get('avg_days_to_sell', 30) > 60:
                warnings.append("Slow-selling vehicle - ensure adequate cash flow planning")
            
            return warnings
            
        except Exception as e:
            logger.error(f"Error generating risk warnings: {str(e)}")
            return ["Review risk metrics carefully before proceeding"]
    
    def _generate_timing_recommendations(self, result: Dict) -> Dict:
        """Generate timing recommendations"""
        try:
            recommendations = {}
            
            # Seasonal timing
            seasonal_pattern = result.get('seasonal_factors', {}).get('seasonal_pattern')
            if seasonal_pattern == 'winter_peak':
                recommendations['import_timing'] = 'Import in September-October for winter sales'
                recommendations['selling_season'] = 'November-January peak selling period'
            elif seasonal_pattern == 'spring_peak':
                recommendations['import_timing'] = 'Import in January-February for spring sales'
                recommendations['selling_season'] = 'March-May optimal selling period'
            else:
                recommendations['import_timing'] = 'Year-round importing suitable'
                recommendations['selling_season'] = 'No strong seasonal pattern identified'
            
            # Market entry timing
            competition = result.get('competition_analysis', {}).get('competition_level', 'medium')
            if competition == 'low':
                recommendations['market_entry'] = 'Enter immediately - low competition window'
            elif competition == 'high':
                recommendations['market_entry'] = 'Wait for market opportunity or ensure differentiation'
            else:
                recommendations['market_entry'] = 'Standard market entry timing acceptable'
            
            # Inventory timing
            supply_risk = result.get('supply_chain_risk', {}).get('supply_risk', 'medium')
            if supply_risk == 'high':
                recommendations['inventory_strategy'] = 'Secure inventory immediately when available'
            elif supply_risk == 'low':
                recommendations['inventory_strategy'] = 'Flexible timing - adequate supply available'
            else:
                recommendations['inventory_strategy'] = 'Monitor supply levels - moderate availability'
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating timing recommendations: {str(e)}")
            return {'import_timing': 'Review market conditions', 'selling_season': 'Monitor trends'}
    
    async def _store_analysis_results(self, results: List[Dict]):
        """Store analysis results in database"""
        try:
            # Clear existing results for fresh analysis
            await self.db.execute("DELETE FROM profitability_analysis")
            
            query = """
            INSERT INTO profitability_analysis (
                make, model, year, fuel_type, avg_uk_selling_price, avg_landed_cost,
                gross_profit, profit_margin_percent, roi_percent, avg_days_to_sell,
                risk_score, demand_score, overall_score, ml_score, final_recommendation_score,
                recommendation_category, priority, confidence_level, analysis_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            for result in results:
                # Store core metrics and additional data as JSON
                analysis_data = {
                    'registration_trend': result.get('registration_trend', {}),
                    'ulez_compliant': result.get('ulez_compliant', {}),
                    'seasonal_factors': result.get('seasonal_factors', {}),
                    'competition_analysis': result.get('competition_analysis', {}),
                    'market_volatility': result.get('market_volatility', {}),
                    'supply_chain_risk': result.get('supply_chain_risk', {}),
                    'action_items': result.get('action_items', []),
                    'risk_warnings': result.get('risk_warnings', []),
                    'timing_recommendations': result.get('timing_recommendations', {}),
                    'confidence_metrics': {
                        'confidence_level': result.get('confidence_level'),
                        'confidence_score': result.get('confidence_score'),
                        'margin_of_error': result.get('margin_of_error')
                    }
                }
                
                await self.db.execute(query, (
                    result.get('make'),
                    result.get('model'),
                    result.get('year'),
                    result.get('fuel_type'),
                    result.get('avg_uk_selling_price'),
                    result.get('avg_landed_cost'),
                    result.get('gross_profit'),
                    result.get('profit_margin_percent'),
                    result.get('roi_percent'),
                    result.get('avg_days_to_sell'),
                    result.get('risk_score'),
                    result.get('demand_score'),
                    result.get('overall_score'),
                    result.get('ml_score'),
                    result.get('final_recommendation_score'),
                    result.get('recommendation_category'),
                    result.get('priority'),
                    result.get('confidence_level'),
                    json.dumps(analysis_data)
                ))
            
            await self.db.commit()
            logger.info(f"Stored {len(results)} analysis results in database")
            
        except Exception as e:
            logger.error(f"Error storing analysis results: {str(e)}")
    
    async def get_market_insights(self) -> Dict:
        """Get comprehensive market insights"""
        try:
            # Top opportunities
            top_opportunities = await self.db.fetchall("""
                SELECT make, model, year, profit_margin_percent, final_recommendation_score
                FROM profitability_analysis 
                ORDER BY final_recommendation_score DESC 
                LIMIT 10
            """)
            
            # Market summary statistics
            market_stats = await self.db.fetchone("""
                SELECT 
                    COUNT(*) as total_opportunities,
                    AVG(profit_margin_percent) as avg_profit_margin,
                    AVG(final_recommendation_score) as avg_score,
                    COUNT(CASE WHEN recommendation_category = 'Highly Recommended' THEN 1 END) as highly_recommended,
                    COUNT(CASE WHEN priority = 'High' THEN 1 END) as high_priority
                FROM profitability_analysis
            """)
            
            # Brand performance
            brand_performance = await self.db.fetchall("""
                SELECT 
                    make,
                    COUNT(*) as opportunity_count,
                    AVG(profit_margin_percent) as avg_margin,
                    AVG(final_recommendation_score) as avg_score
                FROM profitability_analysis
                GROUP BY make
                ORDER BY avg_score DESC
                LIMIT 10
            """)
            
            return {
                'market_summary': dict(market_stats) if market_stats else {},
                'top_opportunities': [dict(row) for row in top_opportunities],
                'brand_performance': [dict(row) for row in brand_performance],
                'generated_at': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting market insights: {str(e)}")
            return {}
    
    async def get_vehicle_analysis(self, make: str, model: str, year: int = None) -> Optional[Dict]:
        """Get detailed analysis for specific vehicle"""
        try:
            where_clause = "WHERE make = ? AND model = ?"
            params = [make, model]
            
            if year:
                where_clause += " AND year = ?"
                params.append(year)
            
            query = f"""
                SELECT * FROM profitability_analysis 
                {where_clause}
                ORDER BY final_recommendation_score DESC
                LIMIT 1
            """
            
            result = await self.db.fetchone(query, tuple(params))
            
            if result:
                # Parse analysis data JSON
                analysis_dict = dict(result)
                if analysis_dict.get('analysis_data'):
                    try:
                        additional_data = json.loads(analysis_dict['analysis_data'])
                        analysis_dict.update(additional_data)
                    except json.JSONDecodeError:
                        pass
                
                return analysis_dict
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting vehicle analysis: {str(e)}")
            return None