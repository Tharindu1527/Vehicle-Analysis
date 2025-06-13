"""
Database schema and models
"""
import aiosqlite
from typing import List
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class DatabaseSchema:
    """Database schema definition"""
    
    @staticmethod
    async def create_tables(db_path: str):
        """Create all database tables"""
        try:
            async with aiosqlite.connect(db_path) as db:
                # UK Market Data Table
                await db.execute("""
                CREATE TABLE IF NOT EXISTS uk_market_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    make TEXT NOT NULL,
                    model TEXT NOT NULL,
                    year INTEGER,
                    mileage INTEGER,
                    price DECIMAL(10,2),
                    fuel_type TEXT,
                    location TEXT,
                    listing_date TEXT,
                    days_listed INTEGER,
                    seller_type TEXT,
                    url TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """)
                
                # Japan Auction Data Table
                await db.execute("""
                CREATE TABLE IF NOT EXISTS japan_auction_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    make TEXT NOT NULL,
                    model TEXT NOT NULL,
                    year INTEGER,
                    mileage INTEGER,
                    hammer_price DECIMAL(10,2),
                    condition_grade TEXT,
                    exterior_grade TEXT,
                    interior_grade TEXT,
                    auction_date TEXT,
                    auction_house TEXT,
                    lot_number TEXT,
                    engine_size INTEGER,
                    fuel_type TEXT,
                    transmission TEXT,
                    drive_type TEXT,
                    color TEXT,
                    seller_fees DECIMAL(8,2),
                    buyer_fees DECIMAL(8,2),
                    total_landed_cost_gbp DECIMAL(10,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source, auction_house, lot_number, auction_date)
                )
                """)
                
                # Government Data Table
                await db.execute("""
                CREATE TABLE IF NOT EXISTS government_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_type TEXT NOT NULL,
                    make TEXT,
                    model TEXT,
                    year INTEGER,
                    fuel_type TEXT,
                    registration_count INTEGER,
                    region TEXT,
                    month INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(data_type, make, model, year, region, month)
                )
                """)
                
                # Exchange Rates Table
                await db.execute("""
                CREATE TABLE IF NOT EXISTS exchange_rates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    base_currency TEXT NOT NULL,
                    target_currency TEXT NOT NULL,
                    rate DECIMAL(10,6) NOT NULL,
                    date_recorded DATE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(base_currency, target_currency, date_recorded)
                )
                """)
                
                # Profitability Analysis Table
                await db.execute("""
                CREATE TABLE IF NOT EXISTS profitability_analysis (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    make TEXT NOT NULL,
                    model TEXT NOT NULL,
                    year INTEGER,
                    fuel_type TEXT,
                    avg_uk_selling_price DECIMAL(10,2),
                    avg_landed_cost DECIMAL(10,2),
                    gross_profit DECIMAL(10,2),
                    profit_margin_percent DECIMAL(5,2),
                    roi_percent DECIMAL(5,2),
                    avg_days_to_sell DECIMAL(5,1),
                    risk_score DECIMAL(5,1),
                    demand_score DECIMAL(5,1),
                    overall_score DECIMAL(5,1),
                    ml_score DECIMAL(5,1),
                    final_recommendation_score DECIMAL(5,1),
                    recommendation_category TEXT,
                    priority TEXT,
                    confidence_level TEXT,
                    analysis_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(make, model, year, fuel_type)
                )
                """)
                
                # Landed Cost Components Table
                await db.execute("""
                CREATE TABLE IF NOT EXISTS landed_cost_components (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    auction_id INTEGER,
                    hammer_price_jpy DECIMAL(12,2),
                    auction_fees_jpy DECIMAL(10,2),
                    transport_to_port_jpy DECIMAL(8,2),
                    export_fees_jpy DECIMAL(8,2),
                    freight_cost_gbp DECIMAL(8,2),
                    import_duty_gbp DECIMAL(8,2),
                    vat_gbp DECIMAL(8,2),
                    port_handling_gbp DECIMAL(6,2),
                    transport_from_port_gbp DECIMAL(6,2),
                    compliance_costs_gbp DECIMAL(6,2),
                    total_landed_cost_gbp DECIMAL(10,2),
                    exchange_rate_used DECIMAL(8,6),
                    calculation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (auction_id) REFERENCES japan_auction_data (id)
                )
                """)
                
                # Market Intelligence Table
                await db.execute("""
                CREATE TABLE IF NOT EXISTS market_intelligence (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    make TEXT NOT NULL,
                    model TEXT NOT NULL,
                    year INTEGER,
                    fuel_type TEXT,
                    market_trend TEXT,
                    seasonal_pattern TEXT,
                    competition_level TEXT,
                    regional_popularity TEXT,
                    ulez_compliance BOOLEAN,
                    data_confidence TEXT,
                    intelligence_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(make, model, year, fuel_type)
                )
                """)
                
                # Create indexes for better performance
                await db.execute("CREATE INDEX IF NOT EXISTS idx_uk_make_model ON uk_market_data(make, model)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_uk_year ON uk_market_data(year)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_uk_price ON uk_market_data(price)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_uk_created_at ON uk_market_data(created_at)")
                
                await db.execute("CREATE INDEX IF NOT EXISTS idx_japan_make_model ON japan_auction_data(make, model)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_japan_year ON japan_auction_data(year)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_japan_hammer_price ON japan_auction_data(hammer_price)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_japan_auction_date ON japan_auction_data(auction_date)")
                
                await db.execute("CREATE INDEX IF NOT EXISTS idx_prof_score ON profitability_analysis(final_recommendation_score)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_prof_margin ON profitability_analysis(profit_margin_percent)")
                await db.execute("CREATE INDEX IF NOT EXISTS idx_prof_priority ON profitability_analysis(priority)")
                
                await db.commit()
                logger.info("Database tables created successfully")
                
        except Exception as e:
            logger.error(f"Error creating database tables: {str(e)}")
            raise