"""
Telegram Data Scraper for Ethiopian Medical Businesses
Updated with real Ethiopian channels
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaPhoto, Channel
from telethon.errors import FloodWaitError, ChannelPrivateError, UsernameNotOccupiedError
from telethon.tl.functions.channels import GetFullChannelRequest
from dotenv import load_dotenv
from loguru import logger
import pytz

# Load environment variables
load_dotenv()

class TelegramScraper:
    """Scrape data from Ethiopian medical Telegram channels"""
    
    def __init__(self):
        self.api_id = int(os.getenv('TELEGRAM_API_ID'))
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        self.phone = os.getenv('TELEGRAM_PHONE')
        
        # REAL Ethiopian Medical/Health Channels
        # Found from et.tgstat.com/medicine and manual search
        self.channels = [
            # Actual channels (verify these work)
            'lobelia4cosmetics',      # Cosmetics & health - CONFIRMED REAL
            'tikvahpharma',           # Pharmaceuticals - CONFIRMED REAL
            
            # Ethiopian medical channels (need to verify)
            'ethiopharm',             # Ethiopian pharmaceuticals
            'medicine_ethiopia',      # Medicine in Ethiopia
            'ethiomedical',           # Ethiopian medical
            'addispharmacy',          # Addis pharmacy
            
            # Additional medical channels
            'pharmacyaddis',          # Pharmacy in Addis
            'health_eth',             # Health Ethiopia
            'ethiodrugs',             # Ethiopian drugs
            
            # Generic medical channels that might have Ethiopian content
            'medicalproducts',        # Medical products
            'pharmachannel',          # Pharmacy channel
        ]
        
        # Alternative: Use channel IDs if usernames don't work
        self.channel_ids = {
            # You can find these by visiting channels in Telegram
            'lobelia4cosmetics': 123456789,  # Example ID, needs real value
            'tikvahpharma': 987654321,       # Example ID, needs real value
        }
        
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
        
        # Statistics
        self.stats = {
            'channels_scraped': 0,
            'messages_scraped': 0,
            'images_downloaded': 0,
            'failed_channels': []
        }
    
    async def connect(self):
        """Connect to Telegram"""
        await self.client.start(phone=self.phone)
        logger.info("Connected to Telegram API")
    
    async def validate_channel(self, channel_username: str) -> bool:
        """Check if a channel exists and is accessible"""
        try:
            entity = await self.client.get_entity(channel_username)
            
            if isinstance(entity, Channel):
                # Get channel info
                full_channel = await self.client(GetFullChannelRequest(entity))
                
                logger.info(f"Channel @{channel_username} validated:")
                logger.info(f"  Title: {entity.title}")
                logger.info(f"  Members: {full_channel.full_chat.participants_count if hasattr(full_channel.full_chat, 'participants_count') else 'N/A'}")
                logger.info(f"  Description: {getattr(entity, 'about', 'No description')[:100]}...")
                
                return True
            else:
                logger.warning(f"@{channel_username} is not a channel (might be a user or group)")
                return False
                
        except UsernameNotOccupiedError:
            logger.error(f"Channel @{channel_username} does not exist")
            return False
        except ChannelPrivateError:
            logger.error(f"Channel @{channel_username} is private")
            return False
        except ValueError as e:
            logger.error(f"Could not find @{channel_username}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error validating @{channel_username}: {str(e)}")
            return False
    
    async def discover_channels(self, search_terms: List[str] = None):
        """Discover Ethiopian medical channels"""
        if search_terms is None:
            search_terms = [
                'ethiopia medical',
                'ethiopia pharmacy',
                'addis ababa medicine',
                'ethiopian drugs',
                'የህክምና እቃዎች',  # Medical equipment in Amharic
                'ፋርማሲ',  # Pharmacy in Amharic
                'መድሃኒት',  # Medicine in Amharic
            ]
        
        discovered_channels = []
        
        for term in search_terms:
            try:
                logger.info(f"Searching for channels with term: {term}")
                
                # Search for channels
                results = await self.client.get_dialogs()
                
                # In real implementation, you'd use search functionality
                # This is simplified - in production, you'd implement proper search
                
            except Exception as e:
                logger.error(f"Error searching for '{term}': {str(e)}")
        
        return discovered_channels
    
    async def scrape_channel(self, channel_identifier: str, days_back: int = 30, limit: int = 1000) -> List[Dict]:
        """Scrape messages from a specific channel"""
        messages_data = []
        
        try:
            # Try to get entity by username or ID
            try:
                if channel_identifier.isdigit():
                    entity = await self.client.get_entity(int(channel_identifier))
                else:
                    entity = await self.client.get_entity(channel_identifier)
            except ValueError:
                # Try with @ prefix
                try:
                    entity = await self.client.get_entity(f'@{channel_identifier}')
                except Exception as e:
                    logger.error(f"Cannot find channel {channel_identifier}: {str(e)}")
                    return messages_data
            
            channel_name = getattr(entity, 'username', str(entity.id))
            channel_title = getattr(entity, 'title', f'Channel_{channel_name}')
            
            logger.info(f"Scraping channel: {channel_title} (@{channel_name})")
            
            # Calculate date range
            end_date = datetime.now(pytz.UTC)
            start_date = end_date - timedelta(days=days_back)
            
            message_count = 0
            image_count = 0
            
            # Iterate through messages
            async for message in self.client.iter_messages(
                entity,
                offset_date=end_date,
                reverse=True,
                limit=limit
            ):
                # Skip messages outside date range
                if message.date < start_date:
                    break
                
                # Skip service messages or empty messages
                if message.message is None and message.media is None:
                    continue
                
                # Extract message data
                message_data = await self._extract_message_data(message, channel_name, channel_title)
                messages_data.append(message_data)
                message_count += 1
                
                # Download image if present
                if message_data['has_media'] and message.media:
                    downloaded = await self._download_image(message, channel_name, message_data['message_id'])
                    if downloaded:
                        image_count += 1
                
                # Progress update every 50 messages
                if message_count % 50 == 0:
                    logger.info(f"  Scraped {message_count} messages from @{channel_name}")
            
            self.stats['messages_scraped'] += message_count
            self.stats['images_downloaded'] += image_count
            
            logger.success(f"Scraped {message_count} messages and {image_count} images from @{channel_name}")
            return messages_data
            
        except ChannelPrivateError:
            logger.error(f"Channel {channel_identifier} is private or inaccessible")
            self.stats['failed_channels'].append(channel_identifier)
        except Exception as e:
            logger.error(f"Error scraping {channel_identifier}: {str(e)}")
            self.stats['failed_channels'].append(channel_identifier)
        
        return messages_data
    
    async def _extract_message_data(self, message: Message, channel_name: str, channel_title: str) -> Dict[str, Any]:
        """Extract relevant data from a message"""
        # Extract text
        message_text = ''
        if message.message:
            message_text = message.message
        elif hasattr(message, 'raw_text'):
            message_text = message.raw_text
        
        # Check for media
        has_media = bool(message.media)
        image_path = ''
        
        if has_media and isinstance(message.media, MessageMediaPhoto):
            image_filename = f"{message.id}.jpg"
            image_path = f"data/raw/images/{channel_name}/{image_filename}"
        
        return {
            'message_id': message.id,
            'channel_name': channel_name,
            'channel_title': channel_title,
            'message_date': message.date.isoformat(),
            'message_text': message_text,
            'has_media': has_media,
            'image_path': image_path,
            'views': message.views or 0,
            'forwards': message.forwards or 0,
            'replies': getattr(message, 'replies', {}).get('replies', 0) if hasattr(message, 'replies') else 0,
            'scraped_at': datetime.now(pytz.UTC).isoformat(),
            'message_type': self._get_message_type(message)
        }
    
    def _get_message_type(self, message: Message) -> str:
        """Determine the type of message"""
        if message.media:
            if hasattr(message.media, 'photo'):
                return 'photo'
            elif hasattr(message.media, 'document'):
                return 'document'
            elif hasattr(message.media, 'video'):
                return 'video'
        
        # Check for common patterns in Ethiopian medical messages
        text = message.message or ''
        text_lower = text.lower()
        
        if any(word in text_lower for word in ['price', 'cost', 'birr', 'etb', '$']):
            return 'price_announcement'
        elif any(word in text_lower for word in ['available', 'in stock', 'arrived']):
            return 'availability'
        elif any(word in text_lower for word in ['discount', 'sale', 'offer']):
            return 'promotion'
        elif any(word in text_lower for word in ['ዋጋ', 'በቅርብ', 'አለ']):  # Amharic keywords
            return 'amharic_ad'
        
        return 'text'
    
    async def _download_image(self, message: Message, channel_name: str, message_id: int) -> bool:
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
                return True
                
        except Exception as e:
            logger.error(f"Error downloading image for message {message_id}: {str(e)}")
        
        return False
    
    def save_to_json(self, messages: List[Dict], channel_name: str, date_str: str):
        """Save scraped messages to JSON file"""
        try:
            if not messages:
                logger.warning(f"No messages to save for {channel_name}")
                return
            
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
    
    def print_statistics(self):
        """Print scraping statistics"""
        logger.info("=" * 50)
        logger.info("SCRAPING STATISTICS")
        logger.info("=" * 50)
        logger.info(f"Channels scraped successfully: {self.stats['channels_scraped']}")
        logger.info(f"Total messages scraped: {self.stats['messages_scraped']}")
        logger.info(f"Total images downloaded: {self.stats['images_downloaded']}")
        
        if self.stats['failed_channels']:
            logger.warning(f"Failed channels ({len(self.stats['failed_channels'])}):")
            for channel in self.stats['failed_channels']:
                logger.warning(f"  - {channel}")
        logger.info("=" * 50)
    
    async def scrape_all_channels(self, days_back: int = 7, limit_per_channel: int = 500):
        """Scrape all configured channels"""
        await self.connect()
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        for channel in self.channels:
            try:
                logger.info(f"Starting scrape for @{channel}")
                
                # Validate channel first
                is_valid = await self.validate_channel(channel)
                
                if not is_valid:
                    logger.warning(f"Skipping invalid channel: @{channel}")
                    self.stats['failed_channels'].append(channel)
                    continue
                
                # Scrape channel messages
                messages = await self.scrape_channel(channel, days_back, limit_per_channel)
                
                if messages:
                    # Save to JSON
                    self.save_to_json(messages, channel, today)
                    self.stats['channels_scraped'] += 1
                    
                    # Brief pause between channels to avoid rate limiting
                    await asyncio.sleep(10)
                    
            except FloodWaitError as e:
                logger.warning(f"Flood wait required for {channel}: {e.seconds} seconds")
                await asyncio.sleep(e.seconds)
                self.stats['failed_channels'].append(channel)
                
            except Exception as e:
                logger.error(f"Unexpected error for {channel}: {str(e)}")
                self.stats['failed_channels'].append(channel)
                continue
        
        # Print statistics
        self.print_statistics()
    
    async def close(self):
        """Close Telegram client"""
        await self.client.disconnect()
        logger.info("Disconnected from Telegram API")

async def main():
    """Main scraping function"""
    scraper = TelegramScraper()
    
    try:
        # Scrape last 7 days, up to 500 messages per channel
        await scraper.scrape_all_channels(days_back=7, limit_per_channel=500)
        
    except Exception as e:
        logger.error(f"Scraping failed: {str(e)}")
        
    finally:
        await scraper.close()

if __name__ == "__main__":
    # Run the scraper
    asyncio.run(main())