"""
Dagster schedules for the Telegram data pipeline
"""

from dagster import schedule, ScheduleEvaluationContext, RunRequest
from .jobs import telegram_pipeline, daily_analytics_job
from datetime import datetime, time

@schedule(
    job=telegram_pipeline,
    cron_schedule="0 2 * * *",  # Run daily at 2 AM
    execution_timezone="Africa/Addis_Ababa"
)
def daily_pipeline_schedule(context: ScheduleEvaluationContext):
    """
    Schedule to run the complete pipeline daily at 2 AM Addis Ababa time.
    """
    run_key = f"daily_pipeline_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M%S')}"
    
    return RunRequest(
        run_key=run_key,
        tags={
            "schedule": "daily",
            "pipeline": "complete",
            "scheduled_time": context.scheduled_execution_time.isoformat()
        },
        run_config={
            "loggers": {
                "console": {
                    "config": {
                        "log_level": "INFO"
                    }
                }
            }
        }
    )

@schedule(
    job=daily_analytics_job,
    cron_schedule="0 3 * * *",  # Run daily at 3 AM
    execution_timezone="Africa/Addis_Ababa"
)
def daily_analytics_schedule(context: ScheduleEvaluationContext):
    """
    Schedule to generate analytics reports daily at 3 AM.
    """
    run_key = f"daily_analytics_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M%S')}"
    
    return RunRequest(
        run_key=run_key,
        tags={
            "schedule": "daily",
            "job": "analytics",
            "scheduled_time": context.scheduled_execution_time.isoformat()
        }
    )

@schedule(
    job=telegram_pipeline,
    cron_schedule="0 */6 * * *",  # Run every 6 hours
    execution_timezone="Africa/Addis_Ababa"
)
def frequent_scraping_schedule(context: ScheduleEvaluationContext):
    """
    Schedule for frequent scraping (every 6 hours).
    Useful for monitoring active channels.
    """
    run_key = f"frequent_scrape_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M%S')}"
    
    return RunRequest(
        run_key=run_key,
        tags={
            "schedule": "frequent",
            "frequency": "6_hours",
            "scheduled_time": context.scheduled_execution_time.isoformat()
        },
        run_config={
            "ops": {
                "scrape_telegram_data": {
                    "config": {
                        "days_back": 1,  # Only scrape last day
                        "limit_per_channel": 100
                    }
                }
            }
        }
    )

@schedule(
    job=telegram_pipeline,
    cron_schedule="0 0 * * 0",  # Run weekly on Sunday at midnight
    execution_timezone="Africa/Addis_Ababa"
)
def weekly_full_pipeline_schedule(context: ScheduleEvaluationContext):
    """
    Schedule for weekly full pipeline execution.
    Includes comprehensive scraping and analysis.
    """
    run_key = f"weekly_full_{context.scheduled_execution_time.strftime('%Y%m%d_%H%M%S')}"
    
    return RunRequest(
        run_key=run_key,
        tags={
            "schedule": "weekly",
            "type": "full",
            "scheduled_time": context.scheduled_execution_time.isoformat()
        },
        run_config={
            "ops": {
                "scrape_telegram_data": {
                    "config": {
                        "days_back": 7,  # Scrape full week
                        "limit_per_channel": 1000
                    }
                }
            }
        }
    )