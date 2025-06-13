from dataclasses import dataclass, field
from typing import Optional, Dict, List
from datetime import datetime
import statistics

from models.vehicle import Vehicle

@dataclass
class MarketData:
    """Aggregated market data for a vehicle"""
    make: str
    model: str
    year: int
    fuel_type: str
    
    # UK Market Data
    uk_listings_count: int = 0
    uk_avg_price: Optional[float] = None
    uk_min_price: Optional[float] = None
    uk_max_price: Optional[float] = None
    uk_median_price: Optional[float] = None
    uk_avg_mileage: Optional[float] = None
    uk_avg_days_listed: Optional[float] = None
    uk_price_trend: Optional[str] = None  # 'increasing', 'decreasing', 'stable'
    
    # Japan Auction Data
    japan_auctions_count: int = 0
    japan_avg_hammer_price: Optional[float] = None
    japan_min_hammer_price: Optional[float] = None
    japan_max_hammer_price: Optional[float] = None
    japan_avg_condition_grade: Optional[float] = None
    japan_avg_mileage: Optional[float] = None
    
    # Regional Data
    regional_distribution: Dict[str, int] = field(default_factory=dict)
    most_popular_region: Optional[str] = None
    
    # Temporal Data
    seasonal_patterns: Dict[str, float] = field(default_factory=dict)
    monthly_volume: Dict[int, int] = field(default_factory=dict)
    
    # Metadata
    last_updated: datetime = field(default_factory=datetime.now)
    data_quality_score: float = 0.0
    
    def calculate_market_health(self) -> Dict:
        """Calculate market health indicators"""
        health_indicators = {
            'liquidity': 'low',
            'price_stability': 'unknown',
            'supply_level': 'low',
            'demand_level': 'low'
        }
        
        # Liquidity assessment
        if self.uk_listings_count >= 20:
            health_indicators['liquidity'] = 'high'
        elif self.uk_listings_count >= 10:
            health_indicators['liquidity'] = 'medium'
        
        # Price stability
        if self.uk_min_price and self.uk_max_price and self.uk_avg_price:
            price_range = (self.uk_max_price - self.uk_min_price) / self.uk_avg_price
            if price_range < 0.2:
                health_indicators['price_stability'] = 'stable'
            elif price_range < 0.4:
                health_indicators['price_stability'] = 'moderate'
            else:
                health_indicators['price_stability'] = 'volatile'
        
        # Supply level
        if self.japan_auctions_count >= 15:
            health_indicators['supply_level'] = 'high'
        elif self.japan_auctions_count >= 8:
            health_indicators['supply_level'] = 'medium'
        
        # Demand level (based on days listed)
        if self.uk_avg_days_listed:
            if self.uk_avg_days_listed < 20:
                health_indicators['demand_level'] = 'high'
            elif self.uk_avg_days_listed < 40:
                health_indicators['demand_level'] = 'medium'
        
        return health_indicators
    
    def get_price_statistics(self) -> Dict:
        """Get comprehensive price statistics"""
        return {
            'uk_market': {
                'average': self.uk_avg_price,
                'median': self.uk_median_price,
                'min': self.uk_min_price,
                'max': self.uk_max_price,
                'range': (self.uk_max_price - self.uk_min_price) if self.uk_min_price and self.uk_max_price else None,
                'coefficient_of_variation': self._calculate_cv('uk') if self.uk_avg_price else None
            },
            'japan_market': {
                'average': self.japan_avg_hammer_price,
                'min': self.japan_min_hammer_price,
                'max': self.japan_max_hammer_price,
                'range': (self.japan_max_hammer_price - self.japan_min_hammer_price) if self.japan_min_hammer_price and self.japan_max_hammer_price else None
            }
        }
    
    def _calculate_cv(self, market: str) -> Optional[float]:
        """Calculate coefficient of variation (placeholder - would need actual data)"""
        # This would require access to individual price points, not just averages
        # Implementation would depend on having variance/std dev data
        return None

@dataclass
class ProfitabilityAnalysis:
    """Profitability analysis results"""
    vehicle: Vehicle
    market_data: MarketData
    
    # Financial Metrics
    avg_uk_selling_price: float
    avg_landed_cost_gbp: float
    gross_profit: float
    profit_margin_percent: float
    roi_percent: float
    best_case_profit: float
    worst_case_profit: float
    
    # Risk Metrics
    risk_score: float  # 0-100, lower is better
    volatility_score: float
    liquidity_risk: float
    competition_risk: float
    
    # Market Metrics
    demand_score: float  # 0-100, higher is better
    avg_days_to_sell: float
    market_share_estimate: float
    seasonal_factor: float
    
    # Scores
    overall_score: float
    ml_score: float
    final_recommendation_score: float
    
    # Recommendations
    recommendation_category: str  # 'Highly Recommended', 'Recommended', etc.
    priority: str  # 'High', 'Medium', 'Low', 'None'
    confidence_level: str  # 'High', 'Medium', 'Low'
    
    # Additional Analysis
    action_items: List[str] = field(default_factory=list)
    risk_warnings: List[str] = field(default_factory=list)
    timing_advice: Dict = field(default_factory=dict)
    
    # Metadata
    analysis_date: datetime = field(default_factory=datetime.now)
    analyst_notes: Optional[str] = None
    
    @property
    def investment_grade(self) -> str:
        """Determine investment grade"""
        if self.final_recommendation_score >= 85:
            return 'A+'
        elif self.final_recommendation_score >= 80:
            return 'A'
        elif self.final_recommendation_score >= 75:
            return 'A-'
        elif self.final_recommendation_score >= 70:
            return 'B+'
        elif self.final_recommendation_score >= 65:
            return 'B'
        elif self.final_recommendation_score >= 60:
            return 'B-'
        elif self.final_recommendation_score >= 55:
            return 'C+'
        elif self.final_recommendation_score >= 50:
            return 'C'
        else:
            return 'D'
    
    @property
    def annualized_roi(self) -> float:
        """Calculate annualized ROI"""
        if self.avg_days_to_sell > 0:
            return (self.roi_percent * 365) / self.avg_days_to_sell
        return 0
    
    @property
    def risk_adjusted_return(self) -> float:
        """Calculate risk-adjusted return (Sharpe-like ratio)"""
        if self.risk_score > 0:
            return self.roi_percent / (self.risk_score / 100)
        return self.roi_percent
    
    def get_summary(self) -> Dict:
        """Get analysis summary"""
        return {
            'vehicle': f"{self.vehicle.make} {self.vehicle.model} ({self.vehicle.year})",
            'investment_grade': self.investment_grade,
            'profit_potential': f"£{self.gross_profit:,.0f} ({self.profit_margin_percent:.1f}%)",
            'roi': f"{self.roi_percent:.1f}%",
            'time_to_sell': f"{self.avg_days_to_sell:.0f} days",
            'risk_level': 'Low' if self.risk_score < 30 else 'Medium' if self.risk_score < 60 else 'High',
            'recommendation': self.recommendation_category,
            'confidence': self.confidence_level
        }