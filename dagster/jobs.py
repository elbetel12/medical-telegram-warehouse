"""
Dagster jobs for the Telegram data pipeline
"""

from dagster import job, OpExecutionContext, in_process_executor, default_executor
from dagster import schedule, ScheduleEvaluationContext
from .assets import *
from .resources import postgres_resource, file_system_resource

@job(
    resource_defs={
        'postgres': postgres_resource.configured({
            'host': 'localhost',
            'port': 5432,
            'database': 'telegram_warehouse',
            'user': 'postgres',
            'password': 'postgres123'
        }),
        'file_system': file_system_resource.configured({
            'data_dir': 'data',
            'logs_dir': 'logs'
        })
    },
    executor_def=in_process_executor,
    description="Complete Telegram data pipeline from scraping to analytics"
)
def telegram_pipeline():
    """
    Complete pipeline job that orchestrates all tasks:
    1. Scrape Telegram data
    2. Load to PostgreSQL
    3. Run dbt transformations
    4. Run YOLO object detection
    5. Load YOLO results to warehouse
    6. Generate analytics reports
    7. Test FastAPI endpoints
    """
    # Define execution order
    scraped_data = scrape_telegram_data()
    loaded_data = load_raw_to_postgres(scraped_data)
    dbt_results = run_dbt_transformations(loaded_data)
    yolo_results = run_yolo_enrichment()
    yolo_loaded = load_yolo_to_warehouse(yolo_results, dbt_results)
    analytics = generate_analytics_report(dbt_results, yolo_loaded)
    test_results = test_fastapi_endpoints(analytics)
    
    return test_results

@job(
    resource_defs={
        'postgres': postgres_resource.configured({
            'host': 'localhost',
            'port': 5432,
            'database': 'telegram_warehouse',
            'user': 'postgres',
            'password': 'postgres123'
        })
    },
    executor_def=in_process_executor,
    description="Daily analytics report generation"
)
def daily_analytics_job():
    """
    Job that runs daily to generate analytics reports.
    Can be run independently when data is already loaded.
    """
    analytics = generate_analytics_report()
    test_results = test_fastapi_endpoints(analytics)
    return test_results

@job(
    resource_defs={
        'postgres': postgres_resource.configured({
            'host': 'localhost',
            'port': 5432,
            'database': 'telegram_warehouse',
            'user': 'postgres',
            'password': 'postgres123'
        }),
        'file_system': file_system_resource.configured({
            'data_dir': 'data',
            'logs_dir': 'logs'
        })
    },
    executor_def=in_process_executor,
    description="Manual data scraping and loading job"
)
def manual_scraping_job():
    """
    Job for manual triggering of data scraping and loading.
    """
    scraped_data = scrape_telegram_data()
    loaded_data = load_raw_to_postgres(scraped_data)
    return loaded_data

@job(
    resource_defs={
        'postgres': postgres_resource.configured({
            'host': 'localhost',
            'port': 5432,
            'database': 'telegram_warehouse',
            'user': 'postgres',
            'password': 'postgres123'
        })
    },
    executor_def=in_process_executor,
    description="DBT transformations only"
)
def dbt_only_job():
    """
    Job for running only DBT transformations.
    Useful when raw data is already loaded.
    """
    dbt_results = run_dbt_transformations()
    return dbt_results

@job(
    resource_defs={
        'file_system': file_system_resource.configured({
            'data_dir': 'data',
            'logs_dir': 'logs'
        })
    },
    executor_def=in_process_executor,
    description="YOLO object detection only"
)
def yolo_only_job():
    """
    Job for running only YOLO object detection.
    """
    yolo_results = run_yolo_enrichment()
    return yolo_results