"""
Main Dagster pipeline definition
"""

from dagster import pipeline, repository
from .jobs import (
    telegram_pipeline,
    daily_analytics_job,
    manual_scraping_job,
    dbt_only_job,
    yolo_only_job
)
from .schedules import (
    daily_pipeline_schedule,
    daily_analytics_schedule,
    frequent_scraping_schedule,
    weekly_full_pipeline_schedule
)
from .sensors import new_channel_sensor, emergency_data_sensor

@pipeline
def telegram_data_pipeline():
    """
    Main pipeline that orchestrates the complete data workflow.
    This is the pipeline that will be displayed in Dagster UI.
    """
    from .assets import (
        scrape_telegram_data,
        load_raw_to_postgres,
        run_dbt_transformations,
        run_yolo_enrichment,
        load_yolo_to_warehouse,
        generate_analytics_report,
        test_fastapi_endpoints
    )
    
    # Define the execution graph
    scraped = scrape_telegram_data()
    loaded = load_raw_to_postgres(scraped)
    dbt = run_dbt_transformations(loaded)
    yolo = run_yolo_enrichment()
    yolo_loaded = load_yolo_to_warehouse(yolo, dbt)
    analytics = generate_analytics_report(dbt, yolo_loaded)
    test_fastapi_endpoints(analytics)

@repository
def telegram_data_repository():
    """
    Dagster repository containing all jobs, schedules, and sensors.
    """
    jobs = [
        telegram_pipeline,
        daily_analytics_job,
        manual_scraping_job,
        dbt_only_job,
        yolo_only_job
    ]
    
    schedules = [
        daily_pipeline_schedule,
        daily_analytics_schedule,
        frequent_scraping_schedule,
        weekly_full_pipeline_schedule
    ]
    
    sensors = [
        new_channel_sensor,
        emergency_data_sensor
    ]
    
    return jobs + schedules + sensors