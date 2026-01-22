"""
Dagster sensors for the Telegram data pipeline
"""

from dagster import sensor, RunRequest, SensorEvaluationContext
from pathlib import Path
import json
from datetime import datetime

@sensor(job_name="manual_scraping_job")
def new_channel_sensor(context: SensorEvaluationContext):
    """
    Sensor that triggers when new Telegram channels are added to configuration.
    """
    config_file = Path("config/channels.json")
    
    if not config_file.exists():
        return
    
    # Get last checked state
    last_checked = context.cursor
    if last_checked is None:
        last_checked = "0"
    
    # Read channel configuration
    with open(config_file, 'r') as f:
        channels = json.load(f)
    
    current_channels = set(channels.get('active_channels', []))
    
    # Check if channels have changed
    if len(current_channels) > int(last_checked):
        context.update_cursor(str(len(current_channels)))
        
        return RunRequest(
            run_key=f"new_channels_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            tags={
                "trigger": "new_channels",
                "channel_count": len(current_channels)
            }
        )

@sensor(job_name="manual_scraping_job", minimum_interval_seconds=300)  # Check every 5 minutes
def emergency_data_sensor(context: SensorEvaluationContext):
    """
    Sensor that triggers when emergency data is detected.
    For example, when certain keywords appear in recent messages.
    """
    import psycopg2
    
    # Connect to database
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='telegram_warehouse',
        user='postgres',
        password='postgres123'
    )
    
    # Check for emergency keywords
    emergency_keywords = ['urgent', 'emergency', 'shortage', 'out of stock', 'critical']
    
    query = """
        SELECT COUNT(*) 
        FROM raw.telegram_messages 
        WHERE message_text ILIKE ANY(%s)
        AND message_date > NOW() - INTERVAL '1 hour';
    """
    
    with conn.cursor() as cursor:
        cursor.execute(query, ([f'%{kw}%' for kw in emergency_keywords],))
        count = cursor.fetchone()[0]
    
    conn.close()
    
    if count > 0:
        return RunRequest(
            run_key=f"emergency_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            tags={
                "trigger": "emergency_data",
                "keyword_count": count,
                "priority": "high"
            }
        )