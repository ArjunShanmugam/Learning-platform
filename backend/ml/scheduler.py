import schedule
import time
import logging
import subprocess
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('training_scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_training_job():
    """Run the training job using subprocess."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_dir = Path(f"data/models/{timestamp}")
        model_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Starting scheduled training job at {timestamp}")
        logger.info(f"Output directory: {model_dir}")

        # Run training script as subprocess
        cmd = ["python", "-m", "ml.scripts.train_model"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)

        if result.returncode == 0:
            logger.info(f"[SUCCESS] Training completed successfully at {timestamp}")
            logger.info(f"Models saved to {model_dir}")
            return True
        else:
            logger.error(f"[FAILED] Training failed: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"Training job timed out (600 seconds)")
        return False
    except Exception as e:
        logger.error(f"Training job failed: {str(e)}")
        return False

def main():
    """Main scheduler loop."""
    # Schedule daily training at 02:00 AM
    schedule.every().day.at("02:00").do(run_training_job)

    logger.info("=" * 60)
    logger.info("Training scheduler started")
    logger.info("Scheduled time: 02:00 AM (UTC)")
    logger.info("Press Ctrl+C to exit")
    logger.info("=" * 60)

    # Run initial training job
    logger.info("Running initial training job...")
    run_training_job()

    # Keep scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    main()
