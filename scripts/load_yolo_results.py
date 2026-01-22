"""
Load YOLO detection results into PostgreSQL database
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from pathlib import Path
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

class YOLOResultsLoader:
    """Load YOLO detection results into PostgreSQL"""
    
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
            logs_dir / 'yolo_loader_{time}.log',
            rotation="100 MB",
            retention="7 days",
            level='INFO'
        )
        
        # Path to YOLO results
        self.results_csv = Path('data/processed/yolo_results/detection_results.csv')
        
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
    
    def create_external_schema(self):
        """Create external schema and table for YOLO results"""
        try:
            # Create external schema
            self.cursor.execute("""
                CREATE SCHEMA IF NOT EXISTS external;
            """)
            
            # Create YOLO detections table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS external.yolo_detections (
                    image_path TEXT,
                    channel_name TEXT,
                    filename TEXT,
                    detection_count INTEGER,
                    detected_objects TEXT,
                    medical_objects TEXT,
                    image_category TEXT,
                    classification_confidence FLOAT,
                    classification_reason TEXT,
                    detection_1_class TEXT,
                    detection_1_confidence FLOAT,
                    detection_2_class TEXT,
                    detection_2_confidence FLOAT,
                    detection_3_class TEXT,
                    detection_3_confidence FLOAT,
                    processing_timestamp TIMESTAMP
                );
            """)
            
            # Create index for faster queries
            self.cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_yolo_channel_category 
                ON external.yolo_detections (channel_name, image_category);
            """)
            
            self.connection.commit()
            logger.success("Created external schema and YOLO detections table")
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error creating schema: {str(e)}")
            raise
    
    def load_yolo_results(self):
        """Load YOLO results from CSV to database"""
        if not self.results_csv.exists():
            logger.error(f"YOLO results CSV not found: {self.results_csv}")
            return 0
        
        try:
            # Read CSV
            df = pd.read_csv(self.results_csv)
            logger.info(f"Read {len(df)} records from {self.results_csv}")
            
            # Prepare data for insertion
            records = []
            for _, row in df.iterrows():
                record = (
                    row.get('image_path', ''),
                    row.get('channel_name', ''),
                    row.get('filename', ''),
                    int(row.get('detection_count', 0)),
                    str(row.get('detected_objects', '')),
                    str(row.get('medical_objects', '')),
                    row.get('image_category', ''),
                    float(row.get('classification_confidence', 0.0)),
                    row.get('classification_reason', ''),
                    row.get('detection_1_class', ''),
                    float(row.get('detection_1_confidence', 0.0)) if pd.notna(row.get('detection_1_confidence')) else 0.0,
                    row.get('detection_2_class', ''),
                    float(row.get('detection_2_confidence', 0.0)) if pd.notna(row.get('detection_2_confidence')) else 0.0,
                    row.get('detection_3_class', ''),
                    float(row.get('detection_3_confidence', 0.0)) if pd.notna(row.get('detection_3_confidence')) else 0.0,
                    row.get('processing_timestamp', None)
                )
                records.append(record)
            
            # Insert records
            insert_query = """
                INSERT INTO external.yolo_detections 
                (image_path, channel_name, filename, detection_count, 
                 detected_objects, medical_objects, image_category, 
                 classification_confidence, classification_reason,
                 detection_1_class, detection_1_confidence,
                 detection_2_class, detection_2_confidence,
                 detection_3_class, detection_3_confidence,
                 processing_timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """
            
            execute_batch(self.cursor, insert_query, records)
            self.connection.commit()
            
            logger.success(f"Loaded {len(records)} YOLO detection records")
            return len(records)
            
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error loading YOLO results: {str(e)}")
            return 0
    
    def analyze_results(self):
        """Perform analysis on YOLO results"""
        try:
            # Category distribution
            self.cursor.execute("""
                SELECT 
                    image_category,
                    COUNT(*) as count,
                    ROUND(AVG(classification_confidence), 3) as avg_confidence
                FROM external.yolo_detections
                GROUP BY image_category
                ORDER BY count DESC;
            """)
            
            categories = self.cursor.fetchall()
            
            logger.info("\n📊 YOLO Results Analysis:")
            logger.info("=" * 50)
            logger.info("Category Distribution:")
            for category, count, avg_conf in categories:
                logger.info(f"  {category}: {count} images (avg confidence: {avg_conf:.3f})")
            
            # Channel-wise analysis
            self.cursor.execute("""
                SELECT 
                    channel_name,
                    image_category,
                    COUNT(*) as count
                FROM external.yolo_detections
                WHERE channel_name != 'unknown'
                GROUP BY channel_name, image_category
                ORDER BY channel_name, count DESC;
            """)
            
            channel_stats = self.cursor.fetchall()
            
            logger.info("\nChannel-wise Analysis:")
            current_channel = None
            for channel, category, count in channel_stats:
                if channel != current_channel:
                    logger.info(f"\n  {channel}:")
                    current_channel = channel
                logger.info(f"    {category}: {count} images")
            
            # Most detected objects
            self.cursor.execute("""
                SELECT 
                    detection_1_class as object_class,
                    COUNT(*) as detection_count
                FROM external.yolo_detections
                WHERE detection_1_class IS NOT NULL
                GROUP BY detection_1_class
                ORDER BY detection_count DESC
                LIMIT 10;
            """)
            
            top_objects = self.cursor.fetchall()
            
            logger.info("\nMost Detected Objects:")
            for obj, count in top_objects:
                logger.info(f"  {obj}: {count} times")
            
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Error analyzing results: {str(e)}")

def main():
    """Main function to load YOLO results"""
    loader = YOLOResultsLoader()
    
    try:
        # Connect to database
        loader.connect()
        
        # Create schema and table
        loader.create_external_schema()
        
        # Load results
        loader.load_yolo_results()
        
        # Analyze results
        loader.analyze_results()
        
        logger.success("YOLO results loading completed successfully!")
        
    except Exception as e:
        logger.error(f"YOLO results loading failed: {str(e)}")
        
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()