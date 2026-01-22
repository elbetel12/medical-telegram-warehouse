"""
Dagster resources for the Telegram data pipeline
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from dagster import resource, Field, String
import psycopg2
from loguru import logger

# Load environment variables
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

class PostgreSQLResource:
    """Resource for PostgreSQL database connections"""
    
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
            logger.success(f"Connected to PostgreSQL: {self.config['database']}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL")
    
    def execute_query(self, query, params=None):
        """Execute a SQL query"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            if query.strip().upper().startswith('SELECT'):
                return self.cursor.fetchall()
            else:
                self.connection.commit()
                return self.cursor.rowcount
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Query failed: {e}")
            raise
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

@resource(
    config_schema={
        'host': Field(String, default_value='localhost', description='PostgreSQL host'),
        'port': Field(int, default_value=5432, description='PostgreSQL port'),
        'database': Field(String, default_value='telegram_warehouse', description='Database name'),
        'user': Field(String, default_value='postgres', description='Database user'),
        'password': Field(String, description='Database password', is_required=True),
    }
)
def postgres_resource(context):
    """Dagster resource for PostgreSQL"""
    config = {
        'host': context.resource_config['host'],
        'port': context.resource_config['port'],
        'database': context.resource_config['database'],
        'user': context.resource_config['user'],
        'password': context.resource_config['password'],
    }
    return PostgreSQLResource(config)

@resource(
    config_schema={
        'data_dir': Field(String, default_value='data', description='Base data directory'),
        'logs_dir': Field(String, default_value='logs', description='Logs directory'),
    }
)
def file_system_resource(context):
    """Resource for file system operations"""
    class FileSystem:
        def __init__(self, config):
            self.base_dir = Path(config['data_dir'])
            self.logs_dir = Path(config['logs_dir'])
            self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        def get_raw_data_path(self, date=None):
            """Get path for raw data"""
            if date:
                return self.base_dir / 'raw' / 'telegram_messages' / date.strftime('%Y-%m-%d')
            return self.base_dir / 'raw' / 'telegram_messages'
        
        def get_images_path(self, channel=None):
            """Get path for images"""
            if channel:
                return self.base_dir / 'raw' / 'images' / channel
            return self.base_dir / 'raw' / 'images'
        
        def get_processed_path(self):
            """Get path for processed data"""
            return self.base_dir / 'processed'
        
        def list_json_files(self, date=None):
            """List all JSON files in raw data directory"""
            path = self.get_raw_data_path(date)
            return list(path.rglob('*.json')) if path.exists() else []
        
        def list_image_files(self, channel=None):
            """List all image files"""
            path = self.get_images_path(channel)
            extensions = {'.jpg', '.jpeg', '.png', '.bmp'}
            files = []
            for ext in extensions:
                files.extend(list(path.rglob(f'*{ext}')))
                files.extend(list(path.rglob(f'*{ext.upper()}')))
            return files
    
    return FileSystem(context.resource_config)