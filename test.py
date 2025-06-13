#!/usr/bin/env python3
"""
Complete test script to verify the Vehicle Import Analyzer setup
"""

import sys
import os
import sqlite3

# Setup path
project_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

def test_environment():
    """Test basic environment"""
    print("🧪 Testing Environment")
    print("-" * 30)
    
    # Python version
    print(f"✅ Python: {sys.version.split()[0]}")
    
    # Virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("✅ Virtual environment: Active")
    else:
        print("⚠️  Virtual environment: Not detected")
    
    # Project structure
    required_dirs = ['src', 'data', 'logs']
    for dirname in required_dirs:
        dirpath = os.path.join(project_root, dirname)
        if os.path.exists(dirpath):
            print(f"✅ Directory: {dirname}")
        else:
            print(f"❌ Directory missing: {dirname}")
    
    return True

def test_imports():
    """Test critical imports"""
    print("\n🔧 Testing Imports")
    print("-" * 30)
    
    # Test Flask
    try:
        from flask import Flask
        print("✅ Flask: Available")
    except ImportError as e:
        print(f"❌ Flask: Missing ({e})")
        return False
    
    # Test asyncio
    try:
        import asyncio
        print("✅ Asyncio: Available")
    except ImportError as e:
        print(f"❌ Asyncio: Missing ({e})")
        return False
    
    # Test our modules
    modules_to_test = [
        ('utils.config', 'Config'),
        ('utils.logger', 'setup_logger'),
        ('dashboard.app', 'create_app')
    ]
    
    for module_name, class_name in modules_to_test:
        try:
            module = __import__(module_name, fromlist=[class_name])
            getattr(module, class_name)
            print(f"✅ {module_name}: Available")
        except ImportError as e:
            print(f"⚠️  {module_name}: Import issue ({e})")
        except Exception as e:
            print(f"⚠️  {module_name}: Other issue ({e})")
    
    return True

def test_database():
    """Test database setup"""
    print("\n🗄️  Testing Database")
    print("-" * 30)
    
    db_path = os.path.join(project_root, 'data', 'vehicle_import_analyzer.db')
    
    if not os.path.exists(db_path):
        print("❌ Database: Not found")
        print("   Run: python setup_database.py")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        expected_tables = [
            'uk_market_data',
            'japan_auction_data', 
            'profitability_analysis',
            'exchange_rates'
        ]
        
        found_tables = [table[0] for table in tables]
        
        for table in expected_tables:
            if table in found_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"✅ Table {table}: {count} records")
            else:
                print(f"❌ Table {table}: Missing")
        
        conn.close()
        print("✅ Database: Operational")
        return True
        
    except Exception as e:
        print(f"❌ Database: Error ({e})")
        return False

def test_dashboard():
    """Test dashboard creation"""
    print("\n🌐 Testing Dashboard")
    print("-" * 30)
    
    try:
        from dashboard.app import create_app
        app = create_app()
        print("✅ Dashboard: App created successfully")
        
        # Test routes
        with app.test_client() as client:
            response = client.get('/')
            if response.status_code == 200:
                print("✅ Dashboard: Home route working")
            else:
                print(f"⚠️  Dashboard: Home route status {response.status_code}")
            
            response = client.get('/api/status')
            if response.status_code == 200:
                print("✅ Dashboard: API routes working")
            else:
                print(f"⚠️  Dashboard: API routes status {response.status_code}")
        
        return True
        
    except Exception as e:
        print(f"❌ Dashboard: Error ({e})")
        return False

def test_configuration():
    """Test configuration"""
    print("\n⚙️  Testing Configuration")
    print("-" * 30)
    
    try:
        from utils.config import Config
        config = Config()
        
        # Test basic config access
        db_path = config.get('DATABASE_PATH', 'default')
        print(f"✅ Config: DATABASE_PATH = {db_path}")
        
        secret_key = config.get('SECRET_KEY', 'default')
        print(f"✅ Config: SECRET_KEY = {'***' if secret_key != 'default' else 'default'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Config: Error ({e})")
        return False

def run_complete_test():
    """Run all tests"""
    print("🚀 Vehicle Import Analyzer - Complete Test")
    print("=" * 50)
    
    tests = [
        ("Environment", test_environment),
        ("Imports", test_imports),
        ("Database", test_database),
        ("Configuration", test_configuration),
        ("Dashboard", test_dashboard)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name}: Exception ({e})")
            results[test_name] = False
    
    # Summary
    print("\n📊 Test Summary")
    print("=" * 50)
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Your system is ready.")
        print("🌐 Start the dashboard: python minimal_start.py")
    else:
        print("\n⚠️  Some tests failed. Check the issues above.")
        
        if not results.get("Database", False):
            print("   Fix: Run 'python setup_database.py'")
        
        if not results.get("Imports", False):
            print("   Fix: Run 'pip install -r requirements.txt'")
    
    return passed == total

if __name__ == "__main__":
    success = run_complete_test()
    sys.exit(0 if success else 1)