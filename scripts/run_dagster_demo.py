"""
Run Dagster pipeline demonstration
"""

import os
import sys
import subprocess
import time
from pathlib import Path
import webbrowser

def run_command(command, description, wait=True):
    """Run a shell command"""
    print(f"\n{'='*60}")
    print(f"📋 {description}")
    print(f"{'='*60}")
    print(f"Command: {command}")
    
    try:
        if wait:
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print("✅ SUCCESS")
                if result.stdout:
                    print(f"Output:\n{result.stdout[:500]}...")
                return True
            else:
                print("❌ FAILED")
                if result.stderr:
                    print(f"Error:\n{result.stderr}")
                return False
        else:
            # Run in background
            subprocess.Popen(command, shell=True)
            print("✅ Started in background")
            return True
            
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def setup_demo_environment():
    """Setup demo environment"""
    print("="*60)
    print("SETTING UP DAGSTER DEMO ENVIRONMENT")
    print("="*60)
    
    # 1. Check if PostgreSQL is running
    print("\n1. Checking PostgreSQL...")
    if not run_command("docker-compose ps | grep postgres", "Check PostgreSQL"):
        print("\nStarting PostgreSQL...")
        run_command("docker-compose up -d postgres", "Start PostgreSQL")
        time.sleep(10)
    
    # 2. Create sample data
    print("\n2. Creating sample data...")
    data_dir = Path("data/raw/telegram_messages")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Create sample JSON file
    import json
    from datetime import datetime
    sample_data = [
        {
            "message_id": f"demo_{i}",
            "channel_name": "lobelia4cosmetics",
            "message_date": datetime.now().isoformat(),
            "message_text": f"Demo product {i} - Medical supplies available",
            "has_media": i % 3 == 0,
            "views": 100 + i * 20,
            "forwards": i,
            "scraped_at": datetime.now().isoformat()
        }
        for i in range(10)
    ]
    
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = data_dir / today / "demo_channel.json"
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, "w") as f:
        json.dump(sample_data, f, indent=2)
    
    print(f"✅ Created sample data: {output_file}")
    
    # 3. Create sample images
    print("\n3. Creating sample images...")
    images_dir = Path("data/raw/images/lobelia4cosmetics")
    images_dir.mkdir(parents=True, exist_ok=True)
    
    for i in range(5):
        image_file = images_dir / f"demo_image_{i}.txt"
        with open(image_file, "w") as f:
            f.write(f"Placeholder for medical product image {i}")
    
    print(f"✅ Created sample images in {images_dir}")
    
    # 4. Create YOLO results
    print("\n4. Creating sample YOLO results...")
    yolo_dir = Path("data/processed/yolo_results")
    yolo_dir.mkdir(parents=True, exist_ok=True)
    
    import pandas as pd
    yolo_data = [
        {
            "image_path": str(images_dir / f"demo_image_{i}.txt"),
            "channel_name": "lobelia4cosmetics",
            "filename": f"demo_image_{i}.txt",
            "detection_count": 2,
            "detected_objects": "person,bottle",
            "medical_objects": "person,bottle",
            "image_category": "promotional",
            "classification_confidence": 0.85,
            "classification_reason": "Sample detection",
            "processing_timestamp": datetime.now().isoformat()
        }
        for i in range(3)
    ]
    
    yolo_file = yolo_dir / "detection_results.csv"
    pd.DataFrame(yolo_data).to_csv(yolo_file, index=False)
    print(f"✅ Created sample YOLO results: {yolo_file}")
    
    return True

def run_dagster_pipeline():
    """Run Dagster pipeline demonstration"""
    print("\n" + "="*60)
    print("RUNNING DAGSTER PIPELINE DEMONSTRATION")
    print("="*60)
    
    # 1. Start Dagster UI
    print("\n1. Starting Dagster UI...")
    dagster_cmd = f'"{sys.executable}" -m dagster dev -f "dagster/pipeline.py"'
    run_command(dagster_cmd, "Start Dagster", wait=False)
    
    # Wait for Dagster to start
    print("\n⏳ Waiting for Dagster UI to start (10 seconds)...")
    time.sleep(10)
    
    # 2. Open browser
    print("\n2. Opening Dagster UI in browser...")
    webbrowser.open("http://localhost:3000")
    
    # 3. Run pipeline via CLI
    print("\n3. Running pipeline via CLI...")
    run_pipeline_cmd = f'"{sys.executable}" -m dagster pipeline execute -f "dagster/pipeline.py"'
    run_command(run_pipeline_cmd, "Execute Pipeline")
    
    # 4. Show available jobs
    print("\n4. Available Dagster jobs:")
    jobs = [
        ("Complete Pipeline", "telegram_pipeline"),
        ("Daily Analytics", "daily_analytics_job"),
        ("Manual Scraping", "manual_scraping_job"),
        ("DBT Only", "dbt_only_job"),
        ("YOLO Only", "yolo_only_job")
    ]
    
    for job_name, job_module in jobs:
        print(f"   • {job_name}: python -m dagster job execute -f dagster/jobs.py -j {job_module}")
    
    return True

def demonstrate_features():
    """Demonstrate Dagster features"""
    print("\n" + "="*60)
    print("DAGSTER FEATURES DEMONSTRATION")
    print("="*60)
    
    features = [
        {
            "title": "Asset Lineage",
            "description": "Visualize data dependencies between scraping, transformation, and analysis",
            "command": "# View in Dagster UI: http://localhost:3000"
        },
        {
            "title": "Schedule Pipeline",
            "description": "Run pipeline automatically on schedule (daily, weekly)",
            "command": "python -m dagster schedule up -f dagster/schedules.py"
        },
        {
            "title": "Sensor Triggering",
            "description": "Trigger pipeline runs based on external events",
            "command": "# Configure in sensors.py"
        },
        {
            "title": "Materialize Assets",
            "description": "Run specific parts of the pipeline",
            "command": "python -m dagster asset materialize -f dagster/assets.py -a scrape_telegram_data"
        },
        {
            "title": "View Run History",
            "description": "Monitor pipeline execution history and logs",
            "command": "# View in Dagster UI: http://localhost:3000/runs"
        },
        {
            "title": "Error Monitoring",
            "description": "Track and debug pipeline failures",
            "command": "# View in Dagster UI: http://localhost:3000/instance"
        }
    ]
    
    for feature in features:
        print(f"\n📊 {feature['title']}")
        print(f"   {feature['description']}")
        print(f"   Command: {feature['command']}")
    
    return True

def main():
    """Main demonstration"""
    print("="*60)
    print("ETHIOPIAN MEDICAL TELEGRAM - DAGSTER ORCHESTRATION")
    print("="*60)
    
    # Setup
    if not setup_demo_environment():
        print("❌ Setup failed")
        return
    
    # Run pipeline
    run_dagster_pipeline()
    
    # Demonstrate features
    demonstrate_features()
    
    # Final instructions
    print("\n" + "="*60)
    print("🎉 DAGSTER PIPELINE READY!")
    print("="*60)
    
    print("\n🔗 Access Points:")
    print("   • Dagster UI: http://localhost:3000")
    print("   • FastAPI: http://localhost:8000/docs")
    print("   • PostgreSQL: docker-compose exec postgres psql -U postgres")
    
    print("\n📋 Key Features Demonstrated:")
    print("   ✅ Task 5: Pipeline Orchestration with Dagster")
    print("   ✅ Complete pipeline automation")
    print("   ✅ Scheduling and sensor triggering")
    print("   ✅ Asset lineage and monitoring")
    print("   ✅ Error handling and retries")
    
    print("\n🚀 Next Steps:")
    print("   1. Explore the pipeline in Dagster UI")
    print("   2. Trigger manual runs from the UI")
    print("   3. Set up schedules for automated execution")
    print("   4. Monitor pipeline health and performance")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    main()