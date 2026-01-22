"""
Script to load raw JSON data from data lake into PostgreSQL
"""

import os
import json
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from loguru import logger
import pandas as pd

# Load environment variables
load_dotenv()

class PostgresLoader:
    """Load JSON data into PostgreSQL"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        
        # Database configuration
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', 5432),
            'database': os.getenv('POSTGRES_DB', 'telegram_warehouse'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
        }
        
        # Setup logging
        logs_dir = Path('logs')
        logs_dir.mkdir(exist_ok=True)
        logger.add(
            logs_dir / 'postgres_loader_{time}.log',
            rotation="100 MB",
            retention="7 days",
            level=os.getenv('LOG_LEVEL', 'INFO')
        )
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            logger.success("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Disconnected from database")
    
    def create_raw_schema(self):
        """Create raw schema and tables if they don't exist"""
        try:
            # Create raw schema
            self.cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS raw;
            """)
            
            # Create telegram_messages table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw.telegram_messages (
                    id SERIAL PRIMARY KEY,
                    message_id BIGINT NOT NULL,
                    channel_name VARCHAR(255) NOT NULL,
                    message_date TIMESTAMP WITH TIME ZONE,
                    message_text TEXT,
                    has_media BOOLEAN DEFAULT FALSE,
                    image_path VARCHAR(500),
                    views INTEGER DEFAULT 0,
                    forwards INTEGER DEFAULT 0,
                    scraped_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(message_id, channel_name)
                );
            """)
            
            # Create index for faster queries
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_channel_date 
                ON raw.telegram_messages (channel_name, message_date);
            """)
            
            self.connection.commit()
            logger.success("Created raw schema and tables")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error creating schema: {str(e)}")
            raise
    
    def load_json_files(self, base_path: str = "data/raw/telegram_messages"):
        """Load JSON files from data lake into database"""
        json_path = Path(base_path)
        
        if not json_path.exists():
            logger.error(f"JSON directory not found: {json_path}")
            return
        
        files_loaded = 0
        records_loaded = 0
        
        # Walk through all JSON files
        for json_file in json_path.rglob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    messages = json.load(f)
                
                if not messages:
                    logger.warning(f"No messages in {json_file}")
                    continue
                
                # Prepare data for insertion
                records = []
                for msg in messages:
                    record = (
                        msg.get('message_id'),
                        msg.get('channel_name'),
                        msg.get('message_date'),
                        msg.get('message_text', ''),
                        msg.get('has_media', False),
                        msg.get('image_path', ''),
                        msg.get('views', 0),
                        msg.get('forwards', 0),
                        msg.get('scraped_at')
                    )
                    records.append(record)
                
                # Insert records
                insert_query = """
                    INSERT INTO raw.telegram_messages 
                    (message_id, channel_name, message_date, message_text, 
                     has_media, image_path, views, forwards, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id, channel_name) DO NOTHING;
                """
                
                execute_batch(self.cursor, insert_query, records)
                self.connection.commit()
                
                files_loaded += 1
                records_loaded += len(records)
                logger.info(f"Loaded {len(records)} records from {json_file.name}")
                
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in {json_file}: {str(e)}")
            except Exception as e:
                logger.error(f"Error loading {json_file}: {str(e)}")
                self.connection.rollback()
        
        logger.success(f"Loaded {records_loaded} records from {files_loaded} files")
        return records_loaded
    
    def validate_data(self):
        """Perform basic data validation"""
        try:
            # Check total records
            self.cursor.execute("SELECT COUNT(*) FROM raw.telegram_messages;")
            total = self.cursor.fetchone()[0]
            logger.info(f"Total records in raw.telegram_messages: {total}")
            
            # Check records per channel
            self.cursor.execute("""
                SELECT channel_name, COUNT(*) 
                FROM raw.telegram_messages 
                GROUP BY channel_name 
                ORDER BY COUNT(*) DESC;
            """)
            channels = self.cursor.fetchall()
            
            logger.info("Records per channel:")
            for channel, count in channels:
                logger.info(f"  {channel}: {count}")
            
            # Check date range
            self.cursor.execute("""
                SELECT MIN(message_date), MAX(message_date) 
                FROM raw.telegram_messages;
            """)
            min_date, max_date = self.cursor.fetchone()
            logger.info(f"Date range: {min_date} to {max_date}")
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}")

def main():
    """Main function to load data into PostgreSQL"""
    loader = PostgresLoader()
    
    try:
        # Connect to database
        loader.connect()
        
        # Create schema and tables
        loader.create_raw_schema()
        
        # Load JSON files
        loader.load_json_files()
        
        # Validate loaded data
        loader.validate_data()
        
        logger.success("Data loading completed successfully")
        
    except Exception as e:
        logger.error(f"Datac   loading failed: {str(e)}")
        
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()