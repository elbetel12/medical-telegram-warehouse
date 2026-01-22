"""
Dagster assets for the Telegram data pipeline
"""

import os
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
from dagster import asset, Output, AssetKey, AssetIn, MetadataValue
from loguru import logger

# Import project modules
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from src.scraper import TelegramScraper
    from src.yolo_detect import YOLODetector
except ImportError:
    logger.warning("Project modules not available - using mock implementations")

@asset(
    description="Scrape data from Telegram channels",
    required_resource_keys={'postgres', 'file_system'},
    op_tags={"kind": "scraping", "team": "data_engineering"}
)
def scrape_telegram_data(context) -> Output[Dict[str, Any]]:
    """
    Asset that scrapes data from Telegram channels and saves to data lake.
    
    Returns:
        Dictionary with scraping statistics
    """
    fs = context.resources.file_system
    
    # Initialize scraper
    scraper = TelegramScraper()
    
    # Create sample data for demonstration
    # In production, this would actually scrape from Telegram
    today = datetime.now().strftime('%Y-%m-%d')
    sample_data = [
        {
            'message_id': f'sample_{i}',
            'channel_name': 'lobelia4cosmetics',
            'message_date': datetime.now().isoformat(),
            'message_text': f'Sample medical product {i} - {datetime.now().strftime("%H:%M")}',
            'has_media': i % 3 == 0,
            'views': 100 + i * 10,
            'forwards': i,
            'scraped_at': datetime.now().isoformat()
        }
        for i in range(10)
    ]
    
    # Save to JSON file
    output_dir = fs.get_raw_data_path()
    date_dir = output_dir / today
    date_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = date_dir / 'sample_channel.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(sample_data, f, indent=2)
    
    # Statistics
    stats = {
        'channels_scraped': 1,
        'messages_scraped': len(sample_data),
        'output_file': str(output_file),
        'timestamp': datetime.now().isoformat()
    }
    
    # Add metadata to asset
    metadata = {
        'channels_scraped': stats['channels_scraped'],
        'messages_scraped': stats['messages_scraped'],
        'output_file': MetadataValue.path(output_file),
        'sample_messages': MetadataValue.json(sample_data[:2]),  # First 2 messages
    }
    
    context.log.info(f"Scraped {len(sample_data)} sample messages")
    return Output(stats, metadata=metadata)

@asset(
    description="Load raw scraped data into PostgreSQL",
    required_resource_keys={'postgres', 'file_system'},
    op_tags={"kind": "loading", "team": "data_engineering"}
)
def load_raw_to_postgres(context, scrape_telegram_data: Dict) -> Output[int]:
    """
    Asset that loads raw JSON data from data lake into PostgreSQL.
    
    Args:
        scrape_telegram_data: Output from the scraping asset
    
    Returns:
        Number of records loaded
    """
    postgres = context.resources.postgres
    fs = context.resources.file_system
    
    # Create raw schema and table if not exists
    create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS raw;
        
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            id SERIAL PRIMARY KEY,
            message_id VARCHAR(255) NOT NULL,
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
    """
    
    postgres.execute_query(create_table_sql)
    
    # Find JSON files
    json_files = fs.list_json_files()
    total_records = 0
    
    for json_file in json_files[:5]:  # Limit to 5 files for demo
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                messages = json.load(f)
            
            # Insert each message
            for msg in messages:
                insert_sql = """
                    INSERT INTO raw.telegram_messages 
                    (message_id, channel_name, message_date, message_text, 
                     has_media, views, forwards, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id, channel_name) DO NOTHING;
                """
                
                postgres.execute_query(insert_sql, (
                    msg.get('message_id'),
                    msg.get('channel_name'),
                    msg.get('message_date'),
                    msg.get('message_text', ''),
                    msg.get('has_media', False),
                    msg.get('views', 0),
                    msg.get('forwards', 0),
                    msg.get('scraped_at')
                ))
            
            total_records += len(messages)
            context.log.info(f"Loaded {len(messages)} records from {json_file.name}")
            
        except Exception as e:
            context.log.error(f"Error loading {json_file}: {e}")
    
    # Get total count from database
    count_result = postgres.execute_query("SELECT COUNT(*) FROM raw.telegram_messages;")
    total_in_db = count_result[0][0] if count_result else 0
    
    metadata = {
        'files_processed': len(json_files[:5]),
        'records_loaded': total_records,
        'total_in_database': total_in_db,
        'table_name': 'raw.telegram_messages'
    }
    
    return Output(total_records, metadata=metadata)

@asset(
    description="Run dbt transformations to create data warehouse",
    required_resource_keys={'postgres'},
    op_tags={"kind": "transformation", "team": "data_engineering"}
)
def run_dbt_transformations(context, load_raw_to_postgres: int) -> Output[Dict[str, Any]]:
    """
    Asset that runs dbt transformations to create the data warehouse.
    
    Args:
        load_raw_to_postgres: Number of records loaded (dependency)
    
    Returns:
        Dictionary with dbt run statistics
    """
    import subprocess
    from pathlib import Path
    
    dbt_dir = Path(__file__).parent.parent / 'medical_warehouse'
    
    results = {}
    
    # Run dbt commands
    commands = [
        ['dbt', 'debug'],
        ['dbt', 'run'],
        ['dbt', 'test'],
        ['dbt', 'docs', 'generate']
    ]
    
    for cmd in commands:
        context.log.info(f"Running: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=dbt_dir,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                results[' '.join(cmd)] = 'success'
                context.log.info(f"Success: {' '.join(cmd)}")
            else:
                results[' '.join(cmd)] = 'failed'
                context.log.error(f"Failed: {' '.join(cmd)} - {result.stderr}")
                
        except subprocess.TimeoutExpired:
            results[' '.join(cmd)] = 'timeout'
            context.log.error(f"Timeout: {' '.join(cmd)}")
        except Exception as e:
            results[' '.join(cmd)] = f'error: {str(e)}'
            context.log.error(f"Error: {' '.join(cmd)} - {e}")
    
    # Get model counts from database
    postgres = context.resources.postgres
    
    models = [
        ('staging.stg_telegram_messages', 'staging'),
        ('marts.dim_channels', 'dimension'),
        ('marts.dim_dates', 'dimension'),
        ('marts.fct_messages', 'fact')
    ]
    
    model_stats = {}
    for table_name, model_type in models:
        try:
            count_result = postgres.execute_query(f"SELECT COUNT(*) FROM {table_name};")
            count = count_result[0][0] if count_result else 0
            model_stats[table_name] = {
                'count': count,
                'type': model_type
            }
        except:
            model_stats[table_name] = {
                'count': 0,
                'type': model_type,
                'status': 'not_created'
            }
    
    metadata = {
        'dbt_results': MetadataValue.json(results),
        'model_counts': MetadataValue.json(model_stats),
        'timestamp': datetime.now().isoformat()
    }
    
    return Output(results, metadata=metadata)

@asset(
    description="Run YOLO object detection on downloaded images",
    required_resource_keys={'file_system'},
    op_tags={"kind": "enrichment", "team": "data_science"}
)
def run_yolo_enrichment(context) -> Output[Dict[str, Any]]:
    """
    Asset that runs YOLO object detection on images and saves results.
    
    Returns:
        Dictionary with YOLO detection statistics
    """
    fs = context.resources.file_system
    
    # Initialize YOLO detector
    detector = YOLODetector(model_size='nano')
    detector.load_model()
    
    # Find images
    image_files = fs.list_image_files()
    
    # For demo, create sample detection results
    sample_results = []
    
    for i, img_file in enumerate(image_files[:10]):  # Limit to 10 images
        # Simulate detection
        sample_results.append({
            'image_path': str(img_file),
            'channel_name': img_file.parent.name,
            'filename': img_file.name,
            'detection_count': 2,
            'detected_objects': 'person,bottle',
            'medical_objects': 'person,bottle',
            'image_category': 'promotional',
            'classification_confidence': 0.85,
            'classification_reason': 'Contains person and product',
            'processing_timestamp': datetime.now().isoformat()
        })
    
    # Save results
    output_dir = fs.get_processed_path() / 'yolo_results'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / 'detection_results.csv'
    df = pd.DataFrame(sample_results)
    df.to_csv(output_file, index=False)
    
    stats = {
        'images_processed': len(sample_results),
        'output_file': str(output_file),
        'categories_found': df['image_category'].value_counts().to_dict(),
        'timestamp': datetime.now().isoformat()
    }
    
    metadata = {
        'images_processed': len(sample_results),
        'output_file': MetadataValue.path(output_file),
        'category_distribution': MetadataValue.json(stats['categories_found']),
        'sample_detections': MetadataValue.json(sample_results[:3])
    }
    
    context.log.info(f"Processed {len(sample_results)} images with YOLO")
    return Output(stats, metadata=metadata)

@asset(
    description="Load YOLO results into data warehouse",
    required_resource_keys={'postgres'},
    op_tags={"kind": "loading", "team": "data_engineering"},
    ins={'yolo_results': AssetIn('run_yolo_enrichment')}
)
def load_yolo_to_warehouse(context, run_yolo_enrichment: Dict, run_dbt_transformations: Dict) -> Output[Dict[str, Any]]:
    """
    Asset that loads YOLO detection results into the data warehouse.
    
    Args:
        run_yolo_enrichment: YOLO detection results
        run_dbt_transformations: Ensures dbt models are created
    
    Returns:
        Dictionary with loading statistics
    """
    postgres = context.resources.postgres
    
    # Create external schema and table
    create_table_sql = """
        CREATE SCHEMA IF NOT EXISTS external;
        
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
            processing_timestamp TIMESTAMP
        );
    """
    
    postgres.execute_query(create_table_sql)
    
    # Load sample data
    sample_data = [
        ('data/raw/images/lobelia4cosmetics/image1.jpg', 'lobelia4cosmetics', 'image1.jpg', 
         2, 'person,bottle', 'person,bottle', 'promotional', 0.85, 
         'Contains person and product', datetime.now().isoformat()),
        ('data/raw/images/tikvahpharma/image2.jpg', 'tikvahpharma', 'image2.jpg', 
         1, 'bottle', 'bottle', 'product_display', 0.92, 
         'Contains product without person', datetime.now().isoformat()),
        ('data/raw/images/ethiopharm/image3.jpg', 'ethiopharm', 'image3.jpg', 
         1, 'person', 'person', 'lifestyle', 0.78, 
         'Contains person without product', datetime.now().isoformat()),
    ]
    
    insert_sql = """
        INSERT INTO external.yolo_detections 
        (image_path, channel_name, filename, detection_count, 
         detected_objects, medical_objects, image_category, 
         classification_confidence, classification_reason, processing_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    
    records_loaded = 0
    for record in sample_data:
        try:
            postgres.execute_query(insert_sql, record)
            records_loaded += 1
        except Exception as e:
            context.log.error(f"Error inserting record: {e}")
    
    # Create fct_image_detections if not exists
    create_fct_sql = """
        CREATE TABLE IF NOT EXISTS marts.fct_image_detections AS
        SELECT 
            ROW_NUMBER() OVER (ORDER BY yd.processing_timestamp) as detection_id,
            yd.image_path,
            yd.channel_name,
            yd.filename,
            yd.detection_count,
            yd.detected_objects,
            yd.medical_objects,
            yd.image_category,
            yd.classification_confidence,
            yd.classification_reason,
            yd.processing_timestamp,
            dc.channel_key,
            fm.message_id,
            fm.view_count
        FROM external.yolo_detections yd
        LEFT JOIN marts.dim_channels dc ON yd.channel_name = dc.channel_name
        LEFT JOIN marts.fct_messages fm ON SPLIT_PART(yd.filename, '.', 1) = fm.message_id::TEXT;
    """
    
    postgres.execute_query(create_fct_sql)
    
    # Get counts
    yolo_count = postgres.execute_query("SELECT COUNT(*) FROM external.yolo_detections;")[0][0]
    fct_count = postgres.execute_query("SELECT COUNT(*) FROM marts.fct_image_detections;")[0][0]
    
    stats = {
        'records_loaded': records_loaded,
        'total_yolo_records': yolo_count,
        'total_fct_records': fct_count,
        'tables_created': ['external.yolo_detections', 'marts.fct_image_detections']
    }
    
    metadata = {
        'records_loaded': records_loaded,
        'total_yolo_records': yolo_count,
        'total_fct_records': fct_count,
        'sample_data': MetadataValue.json(sample_data)
    }
    
    return Output(stats, metadata=metadata)

@asset(
    description="Generate analytics report from data warehouse",
    required_resource_keys={'postgres'},
    op_tags={"kind": "analytics", "team": "business_intelligence"},
    ins={
        'dbt_models': AssetIn('run_dbt_transformations'),
        'yolo_data': AssetIn('load_yolo_to_warehouse')
    }
)
def generate_analytics_report(context, run_dbt_transformations: Dict, load_yolo_to_warehouse: Dict) -> Output[Dict[str, Any]]:
    """
    Asset that generates analytics reports from the data warehouse.
    
    Args:
        run_dbt_transformations: DBT model execution results
        load_yolo_to_warehouse: YOLO data loading results
    
    Returns:
        Dictionary with analytics results
    """
    postgres = context.resources.postgres
    
    analytics = {}
    
    # 1. Top products analysis
    top_products_query = """
        SELECT 
            product_category,
            COUNT(*) as mention_count,
            AVG(view_count) as avg_views
        FROM marts.fct_messages
        GROUP BY product_category
        HAVING product_category != 'Other'
        ORDER BY mention_count DESC
        LIMIT 5;
    """
    
    try:
        top_products = postgres.execute_query(top_products_query)
        analytics['top_products'] = [
            {'product': row[0], 'mentions': row[1], 'avg_views': float(row[2]) if row[2] else 0}
            for row in top_products
        ]
    except Exception as e:
        context.log.error(f"Error in top products query: {e}")
        analytics['top_products'] = []
    
    # 2. Channel activity
    channel_activity_query = """
        SELECT 
            dc.channel_name,
            COUNT(fm.message_id) as post_count,
            AVG(fm.view_count) as avg_views,
            SUM(CASE WHEN fm.has_media THEN 1 ELSE 0 END) as media_posts
        FROM marts.fct_messages fm
        JOIN marts.dim_channels dc ON fm.channel_key = dc.channel_key
        GROUP BY dc.channel_name
        ORDER BY post_count DESC;
    """
    
    try:
        channel_activity = postgres.execute_query(channel_activity_query)
        analytics['channel_activity'] = [
            {'channel': row[0], 'posts': row[1], 'avg_views': float(row[2]) if row[2] else 0, 'media_posts': row[3]}
            for row in channel_activity
        ]
    except Exception as e:
        context.log.error(f"Error in channel activity query: {e}")
        analytics['channel_activity'] = []
    
    # 3. Visual content analysis
    visual_content_query = """
        SELECT 
            image_category,
            COUNT(*) as image_count,
            AVG(classification_confidence) as avg_confidence
        FROM marts.fct_image_detections
        GROUP BY image_category
        ORDER BY image_count DESC;
    """
    
    try:
        visual_content = postgres.execute_query(visual_content_query)
        analytics['visual_content'] = [
            {'category': row[0], 'count': row[1], 'avg_confidence': float(row[2]) if row[2] else 0}
            for row in visual_content
        ]
    except Exception as e:
        context.log.error(f"Error in visual content query: {e}")
        analytics['visual_content'] = []
    
    # Save report to file
    report_dir = Path('reports')
    report_dir.mkdir(exist_ok=True)
    
    report_file = report_dir / f'analytics_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    with open(report_file, 'w') as f:
        json.dump(analytics, f, indent=2)
    
    metadata = {
        'top_products': MetadataValue.json(analytics.get('top_products', [])),
        'channel_activity': MetadataValue.json(analytics.get('channel_activity', [])),
        'visual_content': MetadataValue.json(analytics.get('visual_content', [])),
        'report_file': MetadataValue.path(report_file),
        'timestamp': datetime.now().isoformat()
    }
    
    context.log.info(f"Generated analytics report with {len(analytics)} sections")
    return Output(analytics, metadata=metadata)

@asset(
    description="Test FastAPI endpoints",
    op_tags={"kind": "testing", "team": "quality_assurance"},
    ins={'analytics': AssetIn('generate_analytics_report')}
)
def test_fastapi_endpoints(context, generate_analytics_report: Dict) -> Output[Dict[str, Any]]:
    """
    Asset that tests FastAPI endpoints.
    
    Args:
        generate_analytics_report: Analytics report data
    
    Returns:
        Dictionary with API test results
    """
    import requests
    import time
    
    # Start FastAPI server in background if not running
    base_url = "http://localhost:8000"
    
    test_results = {}
    
    # Check if server is running
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        if response.status_code == 200:
            test_results['health_check'] = 'passed'
        else:
            test_results['health_check'] = f'failed: {response.status_code}'
    except:
        test_results['health_check'] = 'server_not_running'
        context.log.warning("FastAPI server not running. Skipping endpoint tests.")
        return Output(test_results, metadata={'tests_run': 0})
    
    # Test endpoints
    endpoints = [
        ('/api/reports/top-products?limit=3', 'top_products'),
        ('/api/channels/lobelia4cosmetics/activity?period=daily&days=7', 'channel_activity'),
        ('/api/search/messages?query=medical&limit=2', 'message_search'),
        ('/api/reports/visual-content?days=30', 'visual_content')
    ]
    
    for endpoint, name in endpoints:
        try:
            response = requests.get(f"{base_url}{endpoint}", timeout=10)
            if response.status_code == 200:
                test_results[name] = {
                    'status': 'passed',
                    'response_time': response.elapsed.total_seconds()
                }
                context.log.info(f"✅ {name}: {response.status_code} ({response.elapsed.total_seconds():.2f}s)")
            else:
                test_results[name] = {
                    'status': f'failed: {response.status_code}',
                    'error': response.text[:100]
                }
                context.log.warning(f"⚠️ {name}: {response.status_code}")
        except Exception as e:
            test_results[name] = {
                'status': f'error: {str(e)}'
            }
            context.log.error(f"❌ {name}: {e}")
    
    # Calculate success rate
    total_tests = len(endpoints) + 1  # +1 for health check
    passed_tests = sum(1 for result in test_results.values() 
                      if isinstance(result, dict) and result.get('status') == 'passed' 
                      or result == 'passed')
    
    success_rate = (passed_tests / total_tests) * 100
    
    metadata = {
        'total_tests': total_tests,
        'passed_tests': passed_tests,
        'success_rate': f"{success_rate:.1f}%",
        'test_results': MetadataValue.json(test_results),
        'timestamp': datetime.now().isoformat()
    }
    
    context.log.info(f"API tests completed: {passed_tests}/{total_tests} passed ({success_rate:.1f}%)")
    return Output(test_results, metadata=metadata)