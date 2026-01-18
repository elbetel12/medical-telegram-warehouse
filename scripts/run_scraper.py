"""
Run the Telegram scraper with proper configuration
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

def check_telegram_credentials():
    """Check if Telegram API credentials are set"""
    required_vars = ['TELEGRAM_API_ID', 'TELEGRAM_API_HASH', 'TELEGRAM_PHONE']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Missing Telegram API credentials in .env file:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n📝 To get Telegram API credentials:")
        print("   1. Go to https://my.telegram.org")
        print("   2. Login with your phone number")
        print("   3. Create a new application")
        print("   4. Copy API ID and API Hash")
        print("\n🔧 Update your .env file with:")
        print(f"   TELEGRAM_API_ID=your_api_id")
        print(f"   TELEGRAM_API_HASH=your_api_hash")
        print(f"   TELEGRAM_PHONE=+251XXXXXXXXX")
        return False
    
    return True

def main():
    """Main function to run scraper"""
    print("=" * 60)
    print("TELEGRAM MEDICAL CHANNEL SCRAPER")
    print("=" * 60)
    
    # Check credentials
    if not check_telegram_credentials():
        sys.exit(1)
    
    print("✅ Telegram credentials found")
    
    # Ask for scraping parameters
    print("\n📊 Scraping Parameters:")
    print("   Default: Last 7 days, 500 messages per channel")
    
    try:
        # Import and run scraper
        from src.scraper import main as run_scraper
        import asyncio
        
        print("\n🚀 Starting scraper...")
        asyncio.run(run_scraper())
        
    except ImportError as e:
        print(f"❌ Error importing scraper: {e}")
        print("   Make sure you have installed requirements:")
        print("   pip install -r requirements.txt")
    except Exception as e:
        print(f"❌ Error running scraper: {e}")

if __name__ == "__main__":
    main()