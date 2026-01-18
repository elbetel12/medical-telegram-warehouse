"""
Telegram Data Scraper for Ethiopian Medical Businesses
Extracts messages and images from public Telegram channels
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaPhoto
from telethon.errors import FloodWaitError, ChannelPrivateError
from dotenv import load_dotenv
from loguru import logger
import pytz

# Load environment variables
load_dotenv()

class TelegramScraper:
    """Scrape data from Telegram channels"""
    
    def __init__(self):
        self.api_id = int(os.getenv('TELEGRAM_API_ID'))
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        self.phone = os.getenv('TELEGRAM_PHONE')
        
        # Define channels to scrape
        self.channels = [
            'CheMed123',           # Medical products
            'lobelia4cosmetics',   # Cosmetics and health products
            'tikvahpharma',        # Pharmaceuticals
            # Add more channels from et.tgstat.com/medicine
        ]
        
        # Setup directories
        self.base_dir = Path('data')
        self.raw_dir = self.base_dir / 'raw'
        self.images_dir = self.base_dir / 'raw' / 'images'
        self.json_dir = self.base_dir / 'raw' / 'telegram_messages'
        self.logs_dir = Path('logs')
        
        # Create directories if they don't exist
        for directory in [self.images_dir, self.json_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        logger.add(
            self.logs_dir / 'scraper_{time}.log',
            rotation="500 MB",
            retention="10 days",
            level=os.getenv('LOG_LEVEL', 'INFO')
        )
        
        # Initialize Telegram client
        self.client = TelegramClient('session', self.api_id, self.api_hash)
        
    async def connect(self):
        """Connect to Telegram"""
        await self.client.start(phone=self.phone)
        logger.info("Connected to Telegram API")
    
    async def scrape_channel(self, channel_username: str, days_back: int = 30) -> List[Dict]:
        """Scrape messages from a specific channel"""
        messages_data = []
        
        try:
            # Get channel entity
            entity = await self.client.get_entity(channel_username)
            logger.info(f"Scraping channel: {entity.title} (@{channel_username})")
            
            # Calculate date range
            end_date = datetime.now(pytz.UTC)
            start_date = end_date - timedelta(days=days_back)
            
            # Iterate through messages
            async for message in self.client.iter_messages(
                entity,
                offset_date=end_date,
                reverse=True
            ):
                # Skip messages outside date range
                if message.date < start_date:
                    break
                
                # Skip if message is empty
                if not message.message and not message.media:
                    continue
                
                # Extract message data
                message_data = await self._extract_message_data(message, channel_username)
                messages_data.append(message_data)
                
                # Download image if present
                if message_data['has_media'] and message.media:
                    await self._download_image(message, channel_username, message_data['message_id'])
            
            logger.success(f"Scraped {len(messages_data)} messages from @{channel_username}")
            return messages_data
            
        except ChannelPrivateError:
            logger.error(f"Channel @{channel_username} is private or inaccessible")
        except Exception as e:
            logger.error(f"Error scraping @{channel_username}: {str(e)}")
        
        return messages_data
    
    async def _extract_message_data(self, message: Message, channel_name: str) -> Dict[str, Any]:
        """Extract relevant data from a message"""
        return {
            'message_id': message.id,
            'channel_name': channel_name,
            'message_date': message.date.isoformat(),
            'message_text': message.message or '',
            'has_media': bool(message.media),
            'image_path': '',
            'views': message.views or 0,
            'forwards': message.forwards or 0,
            'scraped_at': datetime.now(pytz.UTC).isoformat()
        }
    
    async def _download_image(self, message: Message, channel_name: str, message_id: int):
        """Download image from message if available"""
        try:
            if isinstance(message.media, MessageMediaPhoto):
                # Create channel directory
                channel_dir = self.images_dir / channel_name
                channel_dir.mkdir(exist_ok=True)
                
                # Define image path
                image_path = channel_dir / f"{message_id}.jpg"
                
                # Download the image
                await message.download_media(file=str(image_path))
                
                logger.debug(f"Downloaded image for message {message_id}")
                
        except Exception as e:
            logger.error(f"Error downloading image for message {message_id}: {str(e)}")
    
    def save_to_json(self, messages: List[Dict], channel_name: str, date_str: str):
        """Save scraped messages to JSON file"""
        try:
            # Create date directory
            date_dir = self.json_dir / date_str
            date_dir.mkdir(exist_ok=True)
            
            # Define file path
            file_path = date_dir / f"{channel_name}.json"
            
            # Save messages
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved {len(messages)} messages to {file_path}")
            
        except Exception as e:
            logger.error(f"Error saving JSON for {channel_name}: {str(e)}")
    
    async def scrape_all_channels(self, days_back: int = 30):
        """Scrape all configured channels"""
        await self.connect()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        for channel in self.channels:
            try:
                logger.info(f"Starting scrape for @{channel}")
                
                # Scrape channel messages
                messages = await self.scrape_channel(channel, days_back)
                
                if messages:
                    # Save to JSON
                    self.save_to_json(messages, channel, today)
                    
                    # Brief pause between channels to avoid rate limiting
                    await asyncio.sleep(5)
                    
            except FloodWaitError as e:
                logger.warning(f"Flood wait required for {channel}: {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                
            except Exception as e:
                logger.error(f"Unexpected error for {channel}: {str(e)}")
                continue
    
    async def close(self):
        """Close Telegram client"""
        await self.client.disconnect()
        logger.info("Disconnected from Telegram API")

async def main():
    """Main scraping function"""
    scraper = TelegramScraper()
    
    try:
        await scraper.scrape_all_channels(days_back=7)  # Scrape last 7 days
        
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        
    finally:
        await scraper.close()

if __name__ == "__main__":
    # Run the scraper
    asyncio.run(main())