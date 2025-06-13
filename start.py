#!/usr/bin/env python3
"""
Minimal startup script that definitely works
"""
import sys
import os

# Setup path
project_root = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

print("🚀 Vehicle Import Analyzer - Minimal Version")
print("=" * 50)

# Create directories
for dirname in ['data', 'logs']:
    dirpath = os.path.join(project_root, dirname)
    os.makedirs(dirpath, exist_ok=True)
    print(f"✅ Directory: {dirname}")

try:
    # Import Flask directly and create minimal app
    from flask import Flask, jsonify
    
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'dev-secret-key'
    
    @app.route('/')
    def home():
        return '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Vehicle Import Analyzer</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body { 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                    max-width: 800px; 
                    margin: 40px auto; 
                    padding: 20px;
                    line-height: 1.6;
                }
                .success { 
                    background: #d4edda; 
                    color: #155724; 
                    padding: 15px; 
                    border-radius: 5px; 
                    margin: 20px 0;
                }
                .info { 
                    background: #d1ecf1; 
                    color: #0c5460; 
                    padding: 15px; 
                    border-radius: 5px; 
                    margin: 20px 0;
                }
                .card {
                    background: #f8f9fa;
                    padding: 20px;
                    border-radius: 8px;
                    margin: 20px 0;
                    border: 1px solid #dee2e6;
                }
                ul { padding-left: 20px; }
                li { margin: 8px 0; }
                a { color: #007bff; text-decoration: none; }
                a:hover { text-decoration: underline; }
                code { 
                    background: #f1f3f4; 
                    padding: 2px 6px; 
                    border-radius: 3px; 
                    font-family: monospace;
                }
            </style>
        </head>
        <body>
            <h1>🚀 Vehicle Import Analyzer</h1>
            
            <div class="success">
                <strong>✅ Success!</strong> The dashboard is running successfully!
            </div>
            
            <div class="info">
                <strong>📊 Status:</strong> Core application is working. Database and API integrations can be added progressively.
            </div>
            
            <div class="card">
                <h2>🔗 Available Endpoints</h2>
                <ul>
                    <li><a href="/api/status">System Status</a></li>
                    <li><a href="/api/health">Health Check</a></li>
                    <li><a href="/test">Test Page</a></li>
                </ul>
            </div>
            
            <div class="card">
                <h2>🛠️ Next Steps</h2>
                <ol>
                    <li>✅ Dashboard is running</li>
                    <li>📦 Install requirements: <code>pip install -r requirements.txt</code></li>
                    <li>🗄️ Setup database: <code>python scripts/setup_database.py</code></li>
                    <li>🔑 Add API keys to <code>.env</code> file</li>
                    <li>📊 Run data collection pipeline</li>
                </ol>
            </div>
            
            <div class="card">
                <h2>📁 Project Structure</h2>
                <ul>
                    <li><code>src/</code> - Source code</li>
                    <li><code>data/</code> - Database and data files ✅</li>
                    <li><code>logs/</code> - Application logs ✅</li>
                    <li><code>config/</code> - Configuration files</li>
                </ul>
            </div>
            
            <footer style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #dee2e6; color: #6c757d;">
                <p>Vehicle Import Analyzer v1.0.0 | Port: 5000</p>
            </footer>
        </body>
        </html>
        '''
    
    @app.route('/api/status')
    def api_status():
        return jsonify({
            'status': 'running',
            'service': 'Vehicle Import Analyzer',
            'version': '1.0.0',
            'timestamp': '2024-01-01T00:00:00Z',
            'components': {
                'dashboard': 'operational',
                'database': 'not_connected',
                'apis': 'not_configured',
                'data_pipeline': 'not_running'
            },
            'message': 'Core dashboard is operational'
        })
    
    @app.route('/api/health')
    def health():
        return jsonify({
            'healthy': True,
            'service': 'dashboard',
            'uptime': 'unknown'
        })
    
    @app.route('/test')
    def test():
        return '''
        <html>
        <head><title>Test Page</title></head>
        <body style="font-family: Arial; max-width: 600px; margin: 40px auto; padding: 20px;">
            <h1>🧪 Test Page</h1>
            <p>✅ Flask is working correctly!</p>
            <p>✅ Routing is working!</p>
            <p>✅ Templates are rendering!</p>
            <a href="/">&larr; Back to Home</a>
        </body>
        </html>
        '''
    
    print("✅ Flask app created")
    print("✅ Routes configured")
    print("🌐 Starting server on http://localhost:5000")
    print("   Press Ctrl+C to stop")
    print("-" * 50)
    
    app.run(host='0.0.0.0', port=5000, debug=False)
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()