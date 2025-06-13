from dataclasses import dataclass, field
from typing import Optional, Dict, List
from datetime import datetime
import json

@dataclass
class Vehicle:
    """Base vehicle data model"""
    make: str
    model: str
    year: int
    fuel_type: str
    mileage: Optional[int] = None
    color: Optional[str] = None
    transmission: Optional[str] = None
    engine_size: Optional[int] = None
    drive_type: Optional[str] = None
    body_type: Optional[str] = None
    doors: Optional[int] = None
    seats: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Validate data after initialization"""
        if not self.make or not self.model:
            raise ValueError("Make and model are required")
        
        if self.year < 1980 or self.year > datetime.now().year + 1:
            raise ValueError(f"Invalid year: {self.year}")
        
        if self.mileage is not None and (self.mileage < 0 or self.mileage > 500000):
            raise ValueError(f"Invalid mileage: {self.mileage}")
    
    @property
    def age(self) -> int:
        """Calculate vehicle age"""
        return datetime.now().year - self.year
    
    @property
    def unique_id(self) -> str:
        """Generate unique identifier"""
        return f"{self.make}_{self.model}_{self.year}_{self.fuel_type}".lower().replace(" ", "_")
    
    def to_dict(self) -> Dict:
        """Convert to dictionary"""
        return {
            'make': self.make,
            'model': self.model,
            'year': self.year,
            'fuel_type': self.fuel_type,
            'mileage': self.mileage,
            'color': self.color,
            'transmission': self.transmission,
            'engine_size': self.engine_size,
            'drive_type': self.drive_type,
            'body_type': self.body_type,
            'doors': self.doors,
            'seats': self.seats,
            'age': self.age,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Vehicle':
        """Create from dictionary"""
        # Handle datetime fields
        if 'created_at' in data and isinstance(data['created_at'], str):
            data['created_at'] = datetime.fromisoformat(data['created_at'])
        if 'updated_at' in data and isinstance(data['updated_at'], str):
            data['updated_at'] = datetime.fromisoformat(data['updated_at'])
        
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

@dataclass
class UKMarketListing(Vehicle):
    """UK market listing model"""
    price: float
    location: str
    listing_date: datetime
    days_listed: int
    seller_type: str
    url: str
    source: str
    listing_id: Optional[str] = None
    dealer_name: Optional[str] = None
    condition: Optional[str] = None
    mot_expiry: Optional[datetime] = None
    tax_due: Optional[datetime] = None
    previous_owners: Optional[int] = None
    service_history: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        
        if self.price <= 0:
            raise ValueError("Price must be positive")
        
        if self.days_listed < 0:
            raise ValueError("Days listed cannot be negative")
    
    @property
    def price_per_mile(self) -> Optional[float]:
        """Calculate price per mile"""
        if self.mileage and self.mileage > 0:
            return self.price / self.mileage
        return None
    
    @property
    def is_recent_listing(self) -> bool:
        """Check if listing is recent (within 7 days)"""
        return self.days_listed <= 7
    
    @property
    def is_stale_listing(self) -> bool:
        """Check if listing is stale (over 60 days)"""
        return self.days_listed > 60

@dataclass
class JapanAuctionResult(Vehicle):
    """Japan auction result model"""
    hammer_price: float  # in JPY
    condition_grade: str
    exterior_grade: str
    interior_grade: str
    auction_date: datetime
    auction_house: str
    lot_number: str
    source: str
    seller_fees: float = 0.0
    buyer_fees: float = 0.0
    auction_id: Optional[str] = None
    repair_needed: Optional[str] = None
    accident_history: Optional[str] = None
    modification_details: Optional[str] = None
    
    def __post_init__(self):
        super().__post_init__()
        
        if self.hammer_price <= 0:
            raise ValueError("Hammer price must be positive")
    
    @property
    def total_auction_cost_jpy(self) -> float:
        """Calculate total auction cost in JPY"""
        return self.hammer_price + self.seller_fees + self.buyer_fees
    
    @property
    def condition_score(self) -> float:
        """Convert condition grade to numeric score (0-10)"""
        grade_mapping = {
            'S': 10, 'A': 8, 'B': 6, 'C': 4, 'D': 2, 'R': 0,
            '5': 10, '4.5': 9, '4': 8, '3.5': 7, '3': 6, '2': 4, '1': 2
        }
        return grade_mapping.get(self.condition_grade, 5)