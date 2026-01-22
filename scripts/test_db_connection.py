"""
Robust database connection script with multiple fallback options
"""

import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv
import time

def test_connection(host, port, database, user, password, description):
    """Test a database connection"""
    print(f"\n🔍 Testing: {description}")
    print(f"   Host: {host}:{port}, DB: {database}, User: {user}")
    
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=5
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()[0]
        
        cursor.execute("SELECT inet_server_addr();")
        server_addr = cursor.fetchone()[0]
        
        print(f"   ✅ SUCCESS!")
        print(f"   PostgreSQL Version: {version}")
        print(f"   Database: {db_name}")
        print(f"   Server Address: {server_addr}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        return False

def main():
    print("="*60)
    print("DATABASE CONNECTION TROUBLESHOOTING")
    print("="*60)
    
    # Load environment
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        print(f"📁 Loaded .env from: {env_path}")
    else:
        print("⚠️  No .env file found")
    
    # Test multiple connection options
    test_cases = [
        # From host to Docker container
        {
            'host': 'localhost',
            'port': 5432,
            'database': 'telegram_warehouse',
            'user': 'postgres',
            'password': 'postgres123',
            'description': 'Host → Docker Container (localhost)'
        },
        {
            'host': '127.0.0.1',
            'port': 5432,
            'database': 'telegram_warehouse',
            'user': 'postgres', 
            'password': 'postgres123',
            'description': 'Host → Docker Container (127.0.0.1)'
        },
        # Docker internal (only works from inside container)
        {
            'host': 'postgres',  # Docker service name
            'port': 5432,
            'database': 'telegram_warehouse',
            'user': 'postgres',
            'password': 'postgres123',
            'description': 'Docker Internal (service name)'
        },
        # Try without password
        {
            'host': 'localhost',
            'port': 5432,
            'database': 'telegram_warehouse',
            'user': 'postgres',
            'password': 'postgres',  # Old password
            'description': 'With old password (postgres)'
        },
        {
            'host': 'localhost',
            'port': 5432,
            'database': 'postgres',  # Default database
            'user': 'postgres',
            'password': 'postgres123',
            'description': 'Default postgres database'
        }
    ]
    
    # Add environment variable test cases
    env_host = os.getenv('POSTGRES_HOST', 'localhost')
    env_port = int(os.getenv('POSTGRES_PORT', 5432))
    env_db = os.getenv('POSTGRES_DB', 'telegram_warehouse')
    env_user = os.getenv('POSTGRES_USER', 'postgres')
    env_password = os.getenv('POSTGRES_PASSWORD', 'postgres123')
    
    test_cases.append({
        'host': env_host,
        'port': env_port,
        'database': env_db,
        'user': env_user,
        'password': env_password,
        'description': 'From Environment Variables'
    })
    
    # Run all tests
    results = []
    for test in test_cases:
        success = test_connection(**test)
        results.append((test['description'], success))
    
    # Print summary
    print("\n" + "="*60)
    print("RESULTS SUMMARY")
    print("="*60)
    
    successful = [desc for desc, success in results if success]
    failed = [desc for desc, success in results if not success]
    
    if successful:
        print("\n✅ SUCCESSFUL CONNECTIONS:")
        for desc in successful:
            print(f"  • {desc}")
    
    if failed:
        print("\n❌ FAILED CONNECTIONS:")
        for desc in failed:
            print(f"  • {desc}")
    
    # Recommendations
    print("\n" + "="*60)
    print("RECOMMENDATIONS")
    print("="*60)
    
    if successful:
        print("\n🎉 Use one of the successful connection methods above.")
        print("Update your .env file with working credentials:")
        print(f"POSTGRES_HOST={env_host}")
        print(f"POSTGRES_PORT={env_port}")
        print(f"POSTGRES_DB={env_db}")
        print(f"POSTGRES_USER={env_user}")
        print(f"POSTGRES_PASSWORD=[working_password]")
    else:
        print("\n🚨 All connections failed. Try these fixes:")
        print("1. Make sure PostgreSQL container is running:")
        print("   docker-compose ps")
        print("\n2. Check PostgreSQL logs:")
        print("   docker-compose logs postgres")
        print("\n3. Reset PostgreSQL completely:")
        print("   docker-compose down -v")
        print("   docker-compose up -d postgres")
        print("\n4. Check if port 5432 is already in use:")
        print("   netstat -ano | findstr :5432")

if __name__ == "__main__":
    main() 