"""
Background scheduler for periodic model training and maintenance tasks
"""

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
import sys
import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "ml"))
sys.path.insert(0, str(Path(__file__).parent.parent))

def train_ranking_model_job():
    """Scheduled job to train ranking model with latest data"""
    try:
        logger.info(f"[{datetime.now().isoformat()}] Starting ranking model training...")
        
        from ml.train_ranking_model import TrainingPipeline
        
        pipeline = TrainingPipeline()
        result = pipeline.run()
        
        status = result.get('status', 'unknown')
        logger.info(f"[{datetime.now().isoformat()}] Training completed with status: {status}")
        
        if status == 'success':
            logger.info(f"Training successful: {result}")
        else:
            logger.error(f"Training failed: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"[{datetime.now().isoformat()}] Training failed with exception: {e}", exc_info=True)

def refresh_embeddings_job():
    """Scheduled job to refresh course embeddings"""
    try:
        logger.info(f"[{datetime.now().isoformat()}] Starting embedding refresh...")
        
        from app.database import SessionLocal
        from app.models import Course, CourseEmbedding
        from app.services.embedding_service import EmbeddingService
        
        db = SessionLocal()
        embedding_service = EmbeddingService()
        
        # Get all courses
        courses = db.query(Course).all()
        updated_count = 0
        
        for course in courses:
            # Check if embedding needs update
            existing = db.query(CourseEmbedding).filter(
                CourseEmbedding.course_id == course.id
            ).first()
            
            if not existing:
                # Compute embedding
                combined_text = f"{course.title} {course.description or ''} {course.difficulty or ''}".strip()
                embedding = embedding_service.compute_embedding(combined_text)
                
                # Store in database
                course_embedding = CourseEmbedding(
                    course_id=course.id,
                    embedding=embedding,
                    embedding_model="sentence-transformers/all-MiniLM-L6-v2"
                )
                db.add(course_embedding)
                updated_count += 1
        
        db.commit()
        db.close()
        
        logger.info(f"[{datetime.now().isoformat()}] Embedding refresh completed. Updated: {updated_count}")
        
    except Exception as e:
        logger.error(f"[{datetime.now().isoformat()}] Embedding refresh failed: {e}", exc_info=True)

def clear_old_recommendations_cache():
    """Scheduled job to clear old cached recommendations"""
    try:
        logger.info(f"[{datetime.now().isoformat()}] Clearing old recommendations cache...")
        
        # If using Redis, flush old keys
        try:
            import redis
            redis_client = redis.Redis(host='localhost', port=6379, db=0)
            redis_client.flushdb()
            logger.info("Redis cache cleared")
        except:
            logger.debug("Redis not available, skipping cache clear")
        
    except Exception as e:
        logger.error(f"Cache cleanup failed: {e}")

def start_scheduler():
    """Start the background scheduler with all jobs"""
    scheduler = BackgroundScheduler(daemon=True)
    
    try:
        # Train ranking model daily at 2 AM UTC
        scheduler.add_job(
            train_ranking_model_job,
            CronTrigger(hour=2, minute=0, second=0),
            id='daily_model_training',
            name='Daily Ranking Model Training',
            replace_existing=True
        )
        
        # Refresh embeddings daily at 3 AM UTC
        scheduler.add_job(
            refresh_embeddings_job,
            CronTrigger(hour=3, minute=0, second=0),
            id='daily_embedding_refresh',
            name='Daily Embedding Refresh',
            replace_existing=True
        )
        
        # Clear cache every 6 hours
        scheduler.add_job(
            clear_old_recommendations_cache,
            'interval',
            hours=6,
            id='periodic_cache_cleanup',
            name='Periodic Cache Cleanup',
            replace_existing=True
        )
        
        scheduler.start()
        
        logger.info("✓ Background scheduler started")
        logger.info("  - Daily model training at 2 AM UTC")
        logger.info("  - Daily embedding refresh at 3 AM UTC")
        logger.info("  - Cache cleanup every 6 hours")
        
        return scheduler
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        return None
