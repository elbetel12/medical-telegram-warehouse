"""
Create realistic sample Telegram data for Ethiopian medical businesses
Generates authentic-looking Ethiopian medical product messages
"""

import json
import random
import os
from pathlib import Path
from datetime import datetime, timedelta
from faker import Faker
import pytz

# Initialize Faker for realistic data
fake = Faker()
Faker.seed(42)  # For reproducible results
random.seed(42)

class EthiopianMedicalDataGenerator:
    """Generate realistic Ethiopian medical Telegram data"""
    
    def __init__(self):
        # Ethiopian timezone
        self.eth_timezone = pytz.timezone('Africa/Addis_Ababa')
        
        # Define realistic Ethiopian medical channels
        self.channels = [
            {
                'username': 'lobelia4cosmetics',
                'title': 'Lobelia Cosmetics ኮስሜቲክስ',
                'type': 'Cosmetics',
                'description': 'Premium cosmetics and skincare products 🇪🇹',
                'members': 12500,
                'language': 'English/Amharic'
            },
            {
                'username': 'tikvahpharma',
                'title': 'Tikvah Pharmaceuticals ፋርማሲ',
                'type': 'Pharmaceutical',
                'description': 'Quality medicines at affordable prices',
                'members': 8300,
                'language': 'English'
            },
            {
                'username': 'ethiopharm',
                'title': 'EthioPharm ኢትዮፋርም',
                'type': 'Pharmaceutical',
                'description': 'የኢትዮጵያ ፋርማሲ እቃዎች | Ethiopian pharmaceutical supplies',
                'members': 15600,
                'language': 'Amharic/English'
            },
            {
                'username': 'addispharmacy',
                'title': 'Addis Pharmacy አዲስ ፋርማሲ',
                'type': 'Pharmacy',
                'description': 'Your trusted pharmacy in Addis Ababa',
                'members': 7200,
                'language': 'Amharic'
            },
            {
                'username': 'healthcare_eth',
                'title': 'Healthcare Ethiopia የጤና እንክብካቤ',
                'type': 'Medical Equipment',
                'description': 'Medical equipment and supplies delivery',
                'members': 5400,
                'language': 'English'
            }
        ]
        
        # Ethiopian medical products with Amharic translations
        self.medical_products = [
            {
                'english': 'Paracetamol 500mg',
                'amharic': 'ፓራሲታሞል 500ሚሊግራም',
                'category': 'Pain Relief',
                'price_range': (15, 35)  # Birr
            },
            {
                'english': 'Amoxicillin 250mg',
                'amharic': 'አሞክሲሲሊን 250ሚሊግራም',
                'category': 'Antibiotic',
                'price_range': (45, 80)
            },
            {
                'english': 'Vitamin C 1000mg',
                'amharic': 'ቫይታሚን ሲ 1000ሚሊግራም',
                'category': 'Vitamin',
                'price_range': (25, 50)
            },
            {
                'english': 'Blood Pressure Monitor',
                'amharic': 'የደም ግፊት መለኪያ',
                'category': 'Medical Equipment',
                'price_range': (800, 1500)
            },
            {
                'english': 'Glucometer',
                'amharic': 'ስኳር መለኪያ',
                'category': 'Medical Equipment',
                'price_range': (600, 1200)
            },
            {
                'english': 'Insulin Syringes',
                'amharic': 'ኢንሱሊን መርፌ',
                'category': 'Diabetes Care',
                'price_range': (10, 25)
            },
            {
                'english': 'Cetirizine 10mg',
                'amharic': 'ሴቲሪዚን 10ሚሊግራም',
                'category': 'Allergy',
                'price_range': (20, 40)
            },
            {
                'english': 'Omeprazole 20mg',
                'amharic': 'ኦሜፕራዞል 20ሚሊግራም',
                'category': 'Acid Reducer',
                'price_range': (30, 60)
            }
        ]
        
        # Ethiopian cosmetics products
        self.cosmetics_products = [
            {
                'english': 'Vitamin C Serum',
                'amharic': 'ቫይታሚን ሲ ሴረም',
                'category': 'Skincare',
                'price_range': (250, 500)
            },
            {
                'english': 'Sunscreen SPF 50',
                'amharic': 'ፀሐይ መከላከያ SPF 50',
                'category': 'Sun Protection',
                'price_range': (180, 350)
            },
            {
                'english': 'Anti-Aging Cream',
                'amharic': 'እድሜን የሚቆጣጠር ክሬም',
                'category': 'Skincare',
                'price_range': (300, 600)
            },
            {
                'english': 'Aloe Vera Gel',
                'amharic': 'አልዎ ቬራ ጄል',
                'category': 'Natural Care',
                'price_range': (120, 250)
            },
            {
                'english': 'Tea Tree Oil',
                'amharic': 'ሻይ ዛፍ ዘይት',
                'category': 'Essential Oil',
                'price_range': (150, 300)
            }
        ]
        
        # Ethiopian locations
        self.ethiopian_locations = [
            'Addis Ababa', 'Bole', 'Megenagna', 'Merkato', 'Piazza',
            'Mekelle', 'Bahir Dar', 'Hawassa', 'Dire Dawa', 'Adama',
            'ቦሌ', 'መገናኛ', 'መርካቶ', 'ፒያሳ', 'አዲስ አበባ'
        ]
        
        # Amharic phrases for authentic messages
        self.amharic_phrases = [
            "ወደ አድራሻችን ይምጡ",  # Come to our address
            "በቅርብ ጊዜ ይደርሰናል",  # Will arrive soon
            "ለጅምላ ትዕዛዝ ቅናሽ አለ",  # Discount for bulk orders
            "ጥራቱ የተጠበቀ ነው",  # Quality is guaranteed
            "ፈጣን አቅራቢያ",  # Fast delivery
            "ከሱቅ ይግዙ",  # Buy from store
            "የውጭ ሀገር ምርት",  # Imported product
            "አዲስ የደረሰ",  # New arrival
            "ማሳሰቢያ: የዶክተር መመሪያ ያስፈልጋል",  # Warning: Doctor's prescription needed
        ]
        
        # English phrases for mixed language messages
        self.english_phrases = [
            "Available now", "Limited stock", "Best quality",
            "Original price", "Special offer", "Wholesale price",
            "Fast delivery", "Cash on delivery", "Bulk discount",
            "Contact us", "WhatsApp order", "Call now"
        ]
        
        # Hashtags used in Ethiopian medical channels
        self.hashtags = [
            '#Ethiopia', '#AddisAbaba', '#Pharmacy', '#Medicine',
            '#Healthcare', '#Medical', '#Cosmetics', '#SkinCare',
            '#የጤና', '#ፋርማሲ', '#መድሃኒት', '#ኮስሜቲክስ',
            '#HealthEthiopia', '#BuyEthiopian', '#SupportLocal'
        ]
        
        # Setup directories
        self.base_dir = Path('data')
        self.raw_dir = self.base_dir / 'raw'
        self.images_dir = self.raw_dir / 'images'
        self.json_dir = self.raw_dir / 'telegram_messages'
        self.logs_dir = Path('logs')
        
        # Create directories
        for directory in [self.images_dir, self.json_dir, self.logs_dir]:
            directory.mkdir(parents=True, exist_ok=True)
    
    def generate_message_text(self, product, channel_type):
        """Generate realistic message text for Ethiopian medical products"""
        
        # Choose language based on channel
        languages = []
        if channel_type in ['Pharmaceutical', 'Medical Equipment']:
            languages = ['English', 'Amharic', 'Mixed']
        elif channel_type == 'Cosmetics':
            languages = ['English', 'Mixed']
        else:
            languages = ['Amharic', 'Mixed']
        
        language = random.choice(languages)
        
        # Generate message components
        price = random.randint(*product['price_range'])
        location = random.choice(self.ethiopian_locations)
        
        # Create message based on language
        if language == 'English':
            message = self._generate_english_message(product, price, location)
        elif language == 'Amharic':
            message = self._generate_amharic_message(product, price, location)
        else:  # Mixed
            message = self._generate_mixed_message(product, price, location)
        
        # Add hashtags
        if random.random() > 0.3:  # 70% chance of hashtags
            num_hashtags = random.randint(1, 3)
            selected_hashtags = random.sample(self.hashtags, num_hashtags)
            message += " " + " ".join(selected_hashtags)
        
        return message
    
    def _generate_english_message(self, product, price, location):
        """Generate English message"""
        templates = [
            f"✅ AVAILABLE: {product['english']} - {price} Birr. Best quality guaranteed. Delivery in {location}.",
            f"🆕 NEW ARRIVAL: {product['english']}. Special price: {price} ETB. Limited stock!",
            f"📦 {product['english']} now in stock. Price: {price} Birr. Wholesale discounts available.",
            f"🔥 HOT DEAL: {product['english']} for only {price} Birr! Fast delivery to {location}.",
            f"💊 {product['english']} - {price} ETB. Original product with warranty. Contact for bulk orders.",
        ]
        return random.choice(templates)
    
    def _generate_amharic_message(self, product, price, location):
        """Generate Amharic message"""
        templates = [
            f"✅ የሚገኝ: {product['amharic']} - {price} ብር. ጥራቱ የተጠበቀ ነው. አቅራቢያ ወደ {location}.",
            f"🆕 አዲስ የደረሰ: {product['amharic']}. ልዩ ዋጋ: {price} ብር. ከተወሰነ ብዛት በላይ የለም!",
            f"📦 {product['amharic']} አሁን ይገኛል. ዋጋ: {price} ብር. ለጅምላ ትዕዛዝ ቅናሽ አለ.",
            f"🔥 ልዩ ቅናሽ: {product['amharic']} ለ {price} ብር ብቻ! ፈጣን አቅራቢያ ወደ {location}.",
            f"💊 {product['amharic']} - {price} ብር. ኦሪጅናል ምርት ከዋስትና ጋር. ለጅምላ ትዕዛዝ ያግኙን.",
        ]
        return random.choice(templates)
    
    def _generate_mixed_message(self, product, price, location):
        """Generate mixed English-Amharic message"""
        english_phrase = random.choice(self.english_phrases)
        amharic_phrase = random.choice(self.amharic_phrases)
        
        templates = [
            f"{product['english']} / {product['amharic']} - {price} Birr. {english_phrase}. {amharic_phrase}",
            f"💰 Price: {price} ETB for {product['english']}. {amharic_phrase}. Delivery in {location}.",
            f"{english_phrase}: {product['amharic']} ({product['english']}). Only {price} Birr!",
            f"{product['amharic']} available. {english_phrase} {price} Birr. ወደ {location} አቅራቢያ.",
        ]
        return random.choice(templates)
    
    def generate_message_date(self, base_date, hour_range=(8, 22)):
        """Generate realistic message timestamp"""
        # Ethiopian business hours: 8 AM to 10 PM
        hour = random.randint(*hour_range)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        message_date = base_date.replace(
            hour=hour, 
            minute=minute, 
            second=second,
            microsecond=random.randint(0, 999999)
        )
        
        # Convert to Ethiopia timezone
        return self.eth_timezone.localize(message_date)
    
    def generate_sample_images(self, num_images=20):
        """Create placeholder image files (or you can add real images later)"""
        image_dir = self.images_dir / 'sample_images'
        image_dir.mkdir(exist_ok=True)
        
        # Create empty placeholder files
        for i in range(num_images):
            image_path = image_dir / f'medical_product_{i+1}.txt'
            with open(image_path, 'w') as f:
                f.write(f"Placeholder for medical product image {i+1}\n")
                f.write(f"This would be a photo of medical/cosmetic product\n")
        
        print(f"Created {num_images} placeholder image files")
        return [str(image_dir / f'medical_product_{i+1}.txt') for i in range(num_images)]
    
    def generate_channel_data(self, days_back=30, messages_per_day_range=(5, 25)):
        """Generate data for all channels over specified days"""
        
        all_messages = []
        
        # Generate sample image paths
        sample_images = self.generate_sample_images()
        
        for channel in self.channels:
            print(f"\nGenerating data for: {channel['title']} (@{channel['username']})")
            
            channel_messages = []
            message_id_start = random.randint(100000, 999999)
            
            # Generate messages for each day
            for day_offset in range(days_back):
                current_date = datetime.now() - timedelta(days=day_offset)
                current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
                
                # Number of messages for this day
                num_messages = random.randint(*messages_per_day_range)
                
                for msg_idx in range(num_messages):
                    # Choose product based on channel type
                    if channel['type'] == 'Cosmetics':
                        product = random.choice(self.cosmetics_products)
                    else:
                        product = random.choice(self.medical_products)
                    
                    # Generate message text
                    message_text = self.generate_message_text(product, channel['type'])
                    
                    # Generate timestamp
                    message_date = self.generate_message_date(current_date)
                    
                    # Generate engagement metrics
                    views = random.randint(50, 5000)
                    forwards = random.randint(0, int(views * 0.1))  # 0-10% of views
                    
                    # Determine if message has media
                    has_media = random.random() > 0.6  # 40% chance
                    image_path = ""
                    
                    if has_media and sample_images:
                        image_path = random.choice(sample_images)
                    
                    # Create message data
                    message_data = {
                        'message_id': message_id_start + msg_idx + (day_offset * 1000),
                        'channel_name': channel['username'],
                        'channel_title': channel['title'],
                        'message_date': message_date.isoformat(),
                        'message_text': message_text,
                        'has_media': has_media,
                        'image_path': image_path if has_media else "",
                        'views': views,
                        'forwards': forwards,
                        'replies': random.randint(0, int(views * 0.05)),  # 0-5% of views
                        'scraped_at': (message_date + timedelta(minutes=random.randint(1, 60))).isoformat(),
                        'product_category': product['category'],
                        'product_name_english': product['english'],
                        'product_name_amharic': product['amharic'],
                        'estimated_price': random.randint(*product['price_range'])
                    }
                    
                    channel_messages.append(message_data)
                
                # Sort messages by time for realism
                channel_messages.sort(key=lambda x: x['message_date'])
            
            all_messages.extend(channel_messages)
            print(f"  Generated {len(channel_messages)} messages")
        
        return all_messages
    
    def save_data_to_json(self, messages, partition_by_date=True):
        """Save generated messages to JSON files"""
        
        if partition_by_date:
            # Group messages by date and channel
            messages_by_date_channel = {}
            
            for msg in messages:
                msg_date = datetime.fromisoformat(msg['message_date'].replace('Z', '+00:00'))
                date_str = msg_date.strftime('%Y-%m-%d')
                channel = msg['channel_name']
                
                key = (date_str, channel)
                if key not in messages_by_date_channel:
                    messages_by_date_channel[key] = []
                messages_by_date_channel[key].append(msg)
            
            # Save each group to separate file
            files_saved = 0
            for (date_str, channel), channel_messages in messages_by_date_channel.items():
                date_dir = self.json_dir / date_str
                date_dir.mkdir(parents=True, exist_ok=True)
                
                file_path = date_dir / f"{channel}.json"
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(channel_messages, f, ensure_ascii=False, indent=2)
                
                files_saved += 1
            
            print(f"\n✅ Saved data to {files_saved} JSON files in partitioned structure")
            
        else:
            # Save all messages to single file (for testing)
            file_path = self.json_dir / 'all_messages.json'
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(messages, f, ensure_ascii=False, indent=2)
            
            print(f"\n✅ Saved all data to {file_path}")
    
    def create_data_quality_report(self, messages):
        """Generate a data quality report"""
        report = {
            'total_messages': len(messages),
            'channels': {},
            'date_range': {},
            'media_statistics': {
                'with_media': 0,
                'without_media': 0
            },
            'language_distribution': {
                'english': 0,
                'amharic': 0,
                'mixed': 0
            }
        }
        
        # Analyze messages
        earliest_date = None
        latest_date = None
        
        for msg in messages:
            channel = msg['channel_name']
            
            # Count by channel
            if channel not in report['channels']:
                report['channels'][channel] = 0
            report['channels'][channel] += 1
            
            # Track date range
            msg_date = datetime.fromisoformat(msg['message_date'].replace('Z', '+00:00'))
            if earliest_date is None or msg_date < earliest_date:
                earliest_date = msg_date
            if latest_date is None or msg_date > latest_date:
                latest_date = msg_date
            
            # Media statistics
            if msg['has_media']:
                report['media_statistics']['with_media'] += 1
            else:
                report['media_statistics']['without_media'] += 1
            
            # Language detection (simple)
            msg_text = msg['message_text']
            if 'ሚሊግራም' in msg_text or 'ብር' in msg_text:
                report['language_distribution']['amharic'] += 1
            elif 'Birr' in msg_text or 'ETB' in msg_text:
                report['language_distribution']['english'] += 1
            else:
                report['language_distribution']['mixed'] += 1
        
        report['date_range']['earliest'] = earliest_date.isoformat() if earliest_date else None
        report['date_range']['latest'] = latest_date.isoformat() if latest_date else None
        report['date_range']['days_covered'] = (latest_date - earliest_date).days if earliest_date and latest_date else 0
        
        return report
    
    def print_statistics(self, messages):
        """Print statistics about generated data"""
        print("\n" + "=" * 60)
        print("SAMPLE DATA STATISTICS")
        print("=" * 60)
        
        report = self.create_data_quality_report(messages)
        
        print(f"\n📊 Total Messages: {report['total_messages']:,}")
        print(f"📅 Date Range: {report['date_range']['earliest'][:10]} to {report['date_range']['latest'][:10]}")
        print(f"   (Covers {report['date_range']['days_covered']} days)")
        
        print(f"\n📈 Messages per Channel:")
        for channel, count in report['channels'].items():
            percentage = (count / report['total_messages']) * 100
            print(f"   @{channel}: {count:,} messages ({percentage:.1f}%)")
        
        print(f"\n🖼️  Media Statistics:")
        media_percentage = (report['media_statistics']['with_media'] / report['total_messages']) * 100
        print(f"   With media: {report['media_statistics']['with_media']:,} ({media_percentage:.1f}%)")
        print(f"   Without media: {report['media_statistics']['without_media']:,}")
        
        print(f"\n🗣️  Language Distribution:")
        total_lang = sum(report['language_distribution'].values())
        for lang, count in report['language_distribution'].items():
            percentage = (count / total_lang) * 100
            print(f"   {lang.capitalize()}: {count:,} ({percentage:.1f}%)")
        
        # Calculate average engagement
        avg_views = sum(m['views'] for m in messages) / len(messages)
        avg_forwards = sum(m['forwards'] for m in messages) / len(messages)
        
        print(f"\n📈 Engagement Metrics:")
        print(f"   Average views per message: {avg_views:.0f}")
        print(f"   Average forwards per message: {avg_forwards:.1f}")
        
        print("\n" + "=" * 60)

def main():
    """Main function to generate sample data"""
    print("=" * 60)
    print("ETHIOPIAN MEDICAL TELEGRAM SAMPLE DATA GENERATOR")
    print("=" * 60)
    
    # Create generator
    generator = EthiopianMedicalDataGenerator()
    
    # Get user input
    print("\n📅 Data Generation Parameters:")
    days_back = int(input("   How many days of data? (default: 30) ") or "30")
    avg_messages_per_day = int(input("   Average messages per day per channel? (default: 15) ") or "15")
    
    messages_per_day_range = (
        max(1, int(avg_messages_per_day * 0.5)),  # 50% lower
        int(avg_messages_per_day * 1.5)           # 50% higher
    )
    
    print(f"\n⚙️  Generating {days_back} days of data...")
    print(f"   Messages per day range: {messages_per_day_range[0]}-{messages_per_day_range[1]}")
    print("   This may take a moment...\n")
    
    # Generate data
    messages = generator.generate_channel_data(
        days_back=days_back,
        messages_per_day_range=messages_per_day_range
    )
    
    # Save data
    generator.save_data_to_json(messages, partition_by_date=True)
    
    # Print statistics
    generator.print_statistics(messages)
    
    # Show sample messages
    print("\n📝 SAMPLE MESSAGES:")
    print("-" * 60)
    
    for i, msg in enumerate(random.sample(messages, min(5, len(messages)))):
        print(f"\nMessage {i+1} (@{msg['channel_name']}):")
        print(f"Time: {msg['message_date'][:19]}")
        print(f"Text: {msg['message_text'][:100]}...")
        print(f"Views: {msg['views']:,} | Forwards: {msg['forwards']}")
        if msg['has_media']:
            print(f"📷 Includes media")
        print("-" * 40)
    
    print("\n✅ Sample data generation complete!")
    print(f"📁 Data saved to: {generator.json_dir}/")
    print(f"📁 Images saved to: {generator.images_dir}/")
    
    # Next steps
    print("\n" + "=" * 60)
    print("NEXT STEPS:")
    print("=" * 60)
    print("1. Load data to PostgreSQL:")
    print("   python scripts/load_raw_to_postgres.py")
    print("\n2. Run dbt transformations:")
    print("   cd medical_warehouse && dbt run")
    print("\n3. Test your pipeline with:")
    print("   dbt test")
    print("   dbt docs serve")
    print("\n4. View generated data:")
    print(f"   Explore: {generator.json_dir}/")

if __name__ == "__main__":
    # Install required package if not installed
    try:
        from faker import Faker
    except ImportError:
        print("Installing Faker library for realistic data generation...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "faker"])
        from faker import Faker
    
    main()