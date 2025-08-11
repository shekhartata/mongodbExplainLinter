#!/usr/bin/env python3
"""
Simple Atlas Setup
"""

import re

def extract_credentials(connection_string):
    """Extract username and password from connection string"""
    username = ""
    password = ""
    
    # Look for username:password@ pattern
    auth_pattern = r'://([^:]+):([^@]+)@'
    match = re.search(auth_pattern, connection_string)
    
    if match:
        username = match.group(1)
        password = match.group(2)
        # Remove credentials from connection string
        connection_string = re.sub(auth_pattern, '://', connection_string)
    
    return connection_string, username, password

def setup():
    """Setup Atlas connection"""
    print("🔗 MongoDB Atlas Setup")
    print("=" * 30)
    
    # Get connection details
    connection_string = input("\n📡 Enter your Atlas connection string: ").strip()
    if not connection_string:
        print("❌ Connection string is required!")
        return
    
    database_name = input("🗄️  Enter database name (default: test): ").strip() or "test"
    
    # Extract credentials from connection string
    clean_connection_string, username, password = extract_credentials(connection_string)
    
    # Create .env file
    env_content = f"""# MongoDB Atlas Configuration
MONGO_CONNECTION_STRING={clean_connection_string}
MONGO_DATABASE={database_name}
MONGO_USERNAME={username}
MONGO_PASSWORD={password}
MONGO_AUTH_SOURCE=admin

# Linting Configuration
MAX_COLLECTION_SCAN_THRESHOLD=1000
MAX_EXECUTION_TIME_MS=100
INCLUDE_SYSTEM_COLLECTIONS=false
SAMPLE_SIZE=1000
CI_MODE=false
"""
    
    try:
        with open('.env', 'w') as f:
            f.write(env_content)
        
        print(f"\n✅ Created .env file!")
        print(f"📡 Connection: {clean_connection_string}")
        print(f"👤 Username: {username}")
        print(f"🔑 Password: {'*' * len(password) if password else 'None'}")
        print(f"🗄️  Database: {database_name}")
        
        print("\n🧪 Now you can test with:")
        print("python atlas_cli.py test")
        print("python atlas_cli.py collections")
        
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")

if __name__ == "__main__":
    setup()
