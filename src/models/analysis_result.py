from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

@dataclass
class AnalysisResult:
    """Complete analysis result for a vehicle import opportunity"""
    
    # Vehicle Information
    make: str
    model: str
    year: int
    fuel_type: str
    
    # Core Financial Metrics
    avg_uk_selling_price: float
    avg_landed_cost: float
    gross_profit: float
    profit_margin_percent: float
    roi_percent: float
    
    # Market Data
    uk_listing_count: int
    japan_auction_count: int
    avg_days_to_sell: float
    avg_uk_mileage: Optional[float] = None
    avg_japan_mileage: Optional[float] = None
    
    # Scoring
    risk_score: float
    demand_score: float
    overall_score: float
    ml_score: float
    final_recommendation_score: float
    
    # Recommendations
    recommendation_category: str
    priority: str
    confidence_level: str
    
    # Extended Analysis
    market_intelligence: Dict[str, Any] = field(default_factory=dict)
    cost_breakdown: Dict[str, float] = field(default_factory=dict)
    competitive_analysis: Dict[str, Any] = field(default_factory=dict)
    regulatory_compliance: Dict[str, Any] = field(default_factory=dict)
    
    # Actionable Insights
    action_items: List[str] = field(default_factory=list)
    risk_warnings: List[str] = field(default_factory=list)
    timing_recommendations: Dict[str, str] = field(default_factory=dict)
    
    # Quality Metrics
    data_quality_score: float = 0.0
    analysis_completeness: float = 0.0
    confidence_interval: Dict[str, float] = field(default_factory=dict)
    
    # Metadata
    analysis_id: Optional[str] = None
    analyst_version: str = "1.0"
    analysis_date: datetime = field(default_factory=datetime.now)
    last_updated: datetime = field(default_factory=datetime.now)
    source_data_date: Optional[datetime] = None
    
    def __post_init__(self):
        """Validate and enhance data after initialization"""
        if not self.analysis_id:
            self.analysis_id = self._generate_analysis_id()
        
        # Validate core metrics
        if self.avg_uk_selling_price <= 0:
            raise ValueError("UK selling price must be positive")
        if self.avg_landed_cost <= 0:
            raise ValueError("Landed cost must be positive")
        
        # Calculate derived metrics if not provided
        if not hasattr(self, '_calculated'):
            self._calculate_derived_metrics()
            self._calculated = True
    
    def _generate_analysis_id(self) -> str:
        """Generate unique analysis ID"""
        timestamp = int(datetime.now().timestamp())
        vehicle_id = f"{self.make}_{self.model}_{self.year}_{self.fuel_type}".lower().replace(" ", "_")
        return f"{vehicle_id}_{timestamp}"
    
    def _calculate_derived_metrics(self):
        """Calculate derived metrics"""
        # Ensure profit calculations are consistent
        self.gross_profit = self.avg_uk_selling_price - self.avg_landed_cost
        self.profit_margin_percent = (self.gross_profit / self.avg_uk_selling_price) * 100
        self.roi_percent = (self.gross_profit / self.avg_landed_cost) * 100
    
    @property
    def vehicle_identifier(self) -> str:
        """Unique vehicle identifier"""
        return f"{self.make} {self.model} ({self.year}) - {self.fuel_type}"
    
    @property
    def investment_grade(self) -> str:
        """Investment grade based on final score"""
        score = self.final_recommendation_score
        if score >= 90: return 'A+'
        elif score >= 85: return 'A'
        elif score >= 80: return 'A-'
        elif score >= 75: return 'B+'
        elif score >= 70: return 'B'
        elif score >= 65: return 'B-'
        elif score >= 60: return 'C+'
        elif score >= 55: return 'C'
        elif score >= 50: return 'C-'
        else: return 'D'
    
    @property
    def risk_category(self) -> str:
        """Risk category based on risk score"""
        if self.risk_score < 25: return 'Low Risk'
        elif self.risk_score < 50: return 'Medium Risk'
        elif self.risk_score < 75: return 'High Risk'
        else: return 'Very High Risk'
    
    @property
    def profitability_tier(self) -> str:
        """Profitability tier"""
        margin = self.profit_margin_percent
        if margin >= 30: return 'Exceptional'
        elif margin >= 20: return 'High'
        elif margin >= 15: return 'Good'
        elif margin >= 10: return 'Moderate'
        elif margin >= 5: return 'Low'
        else: return 'Marginal'
    
    @property
    def market_demand_level(self) -> str:
        """Market demand level"""
        if self.avg_days_to_sell <= 14: return 'Very High'
        elif self.avg_days_to_sell <= 30: return 'High'
        elif self.avg_days_to_sell <= 45: return 'Moderate'
        elif self.avg_days_to_sell <= 60: return 'Low'
        else: return 'Very Low'
    
    def get_executive_summary(self) -> Dict[str, Any]:
        """Get executive summary for dashboard"""
        return {
            'vehicle': self.vehicle_identifier,
            'investment_grade': self.investment_grade,
            'profit_potential': {
                'amount': self.gross_profit,
                'margin': self.profit_margin_percent,
                'tier': self.profitability_tier
            },
            'roi': {
                'percentage': self.roi_percent,
                'annualized': self._calculate_annualized_roi()
            },
            'risk': {
                'score': self.risk_score,
                'category': self.risk_category
            },
            'market_dynamics': {
                'demand': self.market_demand_level,
                'time_to_sell': self.avg_days_to_sell,
                'supply': self._assess_supply_level()
            },
            'recommendation': {
                'category': self.recommendation_category,
                'priority': self.priority,
                'confidence': self.confidence_level
            }
        }
    
    def get_detailed_report(self) -> Dict[str, Any]:
        """Get detailed analysis report"""
        return {
            'analysis_metadata': {
                'id': self.analysis_id,
                'date': self.analysis_date.isoformat(),
                'version': self.analyst_version,
                'quality_score': self.data_quality_score
            },
            'vehicle_details': {
                'make': self.make,
                'model': self.model,
                'year': self.year,
                'fuel_type': self.fuel_type,
                'avg_mileage_uk': self.avg_uk_mileage,
                'avg_mileage_japan': self.avg_japan_mileage
            },
            'financial_analysis': {
                'uk_market_price': self.avg_uk_selling_price,
                'total_landed_cost': self.avg_landed_cost,
                'gross_profit': self.gross_profit,
                'profit_margin': self.profit_margin_percent,
                'roi': self.roi_percent,
                'cost_breakdown': self.cost_breakdown
            },
            'market_analysis': {
                'uk_listings': self.uk_listing_count,
                'japan_auctions': self.japan_auction_count,
                'avg_days_to_sell': self.avg_days_to_sell,
                'market_intelligence': self.market_intelligence,
                'competitive_analysis': self.competitive_analysis
            },
            'risk_assessment': {
                'overall_risk_score': self.risk_score,
                'risk_category': self.risk_category,
                'demand_score': self.demand_score,
                'risk_warnings': self.risk_warnings
            },
            'recommendations': {
                'category': self.recommendation_category,
                'priority': self.priority,
                'confidence': self.confidence_level,
                'investment_grade': self.investment_grade,
                'action_items': self.action_items,
                'timing': self.timing_recommendations
            },
            'compliance': self.regulatory_compliance
        }
    
    def _calculate_annualized_roi(self) -> float:
        """Calculate annualized ROI"""
        if self.avg_days_to_sell > 0:
            return (self.roi_percent * 365) / self.avg_days_to_sell
        return 0
    
    def _assess_supply_level(self) -> str:
        """Assess supply level based on auction count"""
        if self.japan_auction_count >= 20: return 'High'
        elif self.japan_auction_count >= 10: return 'Moderate'
        elif self.japan_auction_count >= 5: return 'Low'
        else: return 'Very Low'
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage/API"""
        return {
            'analysis_id': self.analysis_id,
            'vehicle': {
                'make': self.make,
                'model': self.model,
                'year': self.year,
                'fuel_type': self.fuel_type
            },
            'financial_metrics': {
                'avg_uk_selling_price': self.avg_uk_selling_price,
                'avg_landed_cost': self.avg_landed_cost,
                'gross_profit': self.gross_profit,
                'profit_margin_percent': self.profit_margin_percent,
                'roi_percent': self.roi_percent
            },
            'market_metrics': {
                'uk_listing_count': self.uk_listing_count,
                'japan_auction_count': self.japan_auction_count,
                'avg_days_to_sell': self.avg_days_to_sell
            },
            'scores': {
                'risk_score': self.risk_score,
                'demand_score': self.demand_score,
                'overall_score': self.overall_score,
                'ml_score': self.ml_score,
                'final_recommendation_score': self.final_recommendation_score
            },
            'recommendations': {
                'category': self.recommendation_category,
                'priority': self.priority,
                'confidence_level': self.confidence_level,
                'investment_grade': self.investment_grade
            },
            'insights': {
                'action_items': self.action_items,
                'risk_warnings': self.risk_warnings,
                'timing_recommendations': self.timing_recommendations
            },
            'metadata': {
                'analysis_date': self.analysis_date.isoformat(),
                'last_updated': self.last_updated.isoformat(),
                'data_quality_score': self.data_quality_score,
                'analyst_version': self.analyst_version
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AnalysisResult':
        """Create from dictionary"""
        # Flatten nested structure for dataclass initialization
        flat_data = {}
        
        # Vehicle data
        if 'vehicle' in data:
            flat_data.update(data['vehicle'])
        
        # Financial metrics
        if 'financial_metrics' in data:
            flat_data.update(data['financial_metrics'])
        
        # Market metrics
        if 'market_metrics' in data:
            flat_data.update(data['market_metrics'])
        
        # Scores
        if 'scores' in data:
            flat_data.update(data['scores'])
        
        # Recommendations
        if 'recommendations' in data:
            rec_data = data['recommendations']
            flat_data['recommendation_category'] = rec_data.get('category')
            flat_data['priority'] = rec_data.get('priority')
            flat_data['confidence_level'] = rec_data.get('confidence_level')
        
        # Insights
        if 'insights' in data:
            insights = data['insights']
            flat_data['action_items'] = insights.get('action_items', [])
            flat_data['risk_warnings'] = insights.get('risk_warnings', [])
            flat_data['timing_recommendations'] = insights.get('timing_recommendations', {})
        
        # Metadata
        if 'metadata' in data:
            meta = data['metadata']
            flat_data['analysis_id'] = data.get('analysis_id')
            flat_data['data_quality_score'] = meta.get('data_quality_score', 0.0)
            flat_data['analyst_version'] = meta.get('analyst_version', '1.0')
            
            # Handle datetime fields
            if 'analysis_date' in meta:
                flat_data['analysis_date'] = datetime.fromisoformat(meta['analysis_date'])
            if 'last_updated' in meta:
                flat_data['last_updated'] = datetime.fromisoformat(meta['last_updated'])
        
        return cls(**flat_data)
    
    def update_recommendation(self, category: str, priority: str, confidence: str, notes: str = None):
        """Update recommendation details"""
        self.recommendation_category = category
        self.priority = priority
        self.confidence_level = confidence
        if notes:
            self.analyst_notes = notes
        self.last_updated = datetime.now()
    
    def add_action_item(self, action: str):
        """Add action item"""
        if action not in self.action_items:
            self.action_items.append(action)
            self.last_updated = datetime.now()
    
    def add_risk_warning(self, warning: str):
        """Add risk warning"""
        if warning not in self.risk_warnings:
            self.risk_warnings.append(warning)
            self.last_updated = datetime.now()