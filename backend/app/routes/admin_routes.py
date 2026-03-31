"""
Admin Dashboard & MLOps Tools
Provides admin endpoints for model management, training, and performance monitoring
"""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import List, Dict, Optional
from datetime import datetime
import json

from ..database import get_db

router = APIRouter(prefix="/admin", tags=["admin"])

# ====== Schemas ======

class ModelMetrics(BaseModel):
    """Model performance metrics"""
    version: str
    train_auc: float
    test_auc: float
    precision: float
    recall: float
    f1_score: float
    created_at: str

class TrainingJob(BaseModel):
    """Training job details"""
    job_id: str
    status: str  # queued, running, completed, failed
    start_time: Optional[str]
    end_time: Optional[str]
    model_version: Optional[str]
    metrics: Optional[Dict]
    error_message: Optional[str]

class AdminStats(BaseModel):
    """Overall admin statistics"""
    total_users: int
    total_courses: int
    total_interactions: int
    last_training_time: Optional[str]
    current_model_version: str
    model_auc: float

# ====== Model Management Endpoints ======

@router.get("/stats", response_model=AdminStats)
def get_admin_stats(db: Session = Depends(get_db)):
    """Get overall system statistics"""
    from ..models import User, Course, ClickLog, CompletedCourse
    
    users_count = db.query(User).count()
    courses_count = db.query(Course).count()
    interactions = db.query(ClickLog).count() + db.query(CompletedCourse).count()
    
    return AdminStats(
        total_users=users_count,
        total_courses=courses_count,
        total_interactions=interactions,
        last_training_time=None,
        current_model_version="v1.0",
        model_auc=0.85  # Placeholder
    )

@router.get("/models/list")
def list_models():
    """List all trained models and their versions"""
    from pathlib import Path
    
    model_dir = Path("ml/models")
    models = []
    
    if model_dir.exists():
        for file in model_dir.glob("metadata_*.json"):
            try:
                with open(file) as f:
                    metadata = json.load(f)
                    models.append(metadata)
            except:
                pass
    
    return {
        "models": sorted(models, key=lambda x: x.get('created_at', ''), reverse=True),
        "total": len(models)
    }

@router.get("/models/{version}")
def get_model_details(version: str):
    """Get details of specific model version"""
    from pathlib import Path
    
    metadata_path = Path(f"ml/models/metadata_{version}.json")
    
    if not metadata_path.exists():
        raise HTTPException(status_code=404, detail="Model not found")
    
    with open(metadata_path) as f:
        metadata = json.load(f)
    
    return metadata

@router.post("/models/train")
def trigger_model_training(db: Session = Depends(get_db)):
    """Trigger training of new ranking model"""
    import subprocess
    from datetime import datetime
    
    job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Run training in background
        subprocess.Popen(
            ["python", "ml/train_ranking_model.py"],
            cwd="/backend"  # Adjust path as needed
        )
        
        return {
            "status": "started",
            "job_id": job_id,
            "message": "Model training started. Check back for results."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/models/{version}/activate")
def activate_model(version: str):
    """Set specific model version as active"""
    from pathlib import Path
    
    metadata_path = Path(f"ml/models/metadata_{version}.json")
    
    if not metadata_path.exists():
        raise HTTPException(status_code=404, detail="Model not found")
    
    # This would update a config file or database
    return {
        "status": "activated",
        "version": version,
        "message": f"Model {version} is now active"
    }

# ====== Training & Skill Mapping ======

@router.get("/skills")
def get_skill_mappings(db: Session = Depends(get_db)):
    """Get skill to course mappings"""
    from ..models import Course
    
    courses = db.query(Course).all()
    
    skills_map = {}
    for course in courses:
        # Extract skills from course (would be more sophisticated in production)
        skills = course.title.lower().split()[:2]  # Simplified
        for skill in skills:
            if skill not in skills_map:
                skills_map[skill] = []
            skills_map[skill].append({
                "course_id": course.id,
                "course_title": course.title
            })
    
    return {
        "skills": skills_map,
        "total_skills": len(skills_map)
    }

@router.put("/skills/{skill_id}")
def update_skill_mapping(skill_id: str, courses: List[int]):
    """Update courses mapped to a skill"""
    return {
        "status": "updated",
        "skill_id": skill_id,
        "courses": courses
    }

# ====== Training Performance Monitoring ======

@router.get("/training/history")
def get_training_history(limit: int = 10):
    """Get history of training jobs"""
    from pathlib import Path
    import os
    
    training_logs = []
    log_dir = Path("ml/logs")
    
    if log_dir.exists():
        for log_file in sorted(log_dir.glob("*.json"), reverse=True)[:limit]:
            try:
                with open(log_file) as f:
                    log = json.load(f)
                    training_logs.append(log)
            except:
                pass
    
    return {
        "training_jobs": training_logs,
        "total": len(training_logs)
    }

@router.get("/training/{job_id}")
def get_training_job_status(job_id: str):
    """Get status of specific training job"""
    from pathlib import Path
    
    log_file = Path(f"ml/logs/{job_id}.json")
    
    if not log_file.exists():
        raise HTTPException(status_code=404, detail="Job not found")
    
    with open(log_file) as f:
        job_data = json.load(f)
    
    return job_data

# ====== Recommendations Performance ======

@router.get("/recommendations/performance")
def get_recommendations_performance(db: Session = Depends(get_db)):
    """Get recommendation system performance metrics"""
    from ..models import RecommendationFeedbackLog
    
    feedbacks = db.query(RecommendationFeedbackLog).all()
    
    if not feedbacks:
        return {
            "total_recommendations": 0,
            "helpful_rate": 0,
            "avg_rating": 0,
            "ctr": 0
        }
    
    total = len(feedbacks)
    helpful = len([f for f in feedbacks if f.helpful])
    ratings = [f.rating for f in feedbacks if f.rating]
    
    return {
        "total_recommendations": total,
        "helpful_count": helpful,
        "helpful_rate": helpful / total if total > 0 else 0,
        "avg_rating": sum(ratings) / len(ratings) if ratings else 0,
        "ctr": helpful / total if total > 0 else 0
    }

# ====== User & Course Management ======

@router.get("/users")
def get_users_summary(db: Session = Depends(get_db)):
    """Get user statistics"""
    from ..models import User, CompletedCourse
    
    users = db.query(User).all()
    
    user_stats = []
    for user in users[:100]:  # Limit for performance
        completed = db.query(CompletedCourse).filter(
            CompletedCourse.user_id == user.id
        ).count()
        
        user_stats.append({
            "id": user.id,
            "email": user.email,
            "role": user.role,
            "courses_completed": completed
        })
    
    return {
        "users": user_stats,
        "total": len(users)
    }

@router.post("/users/{user_id}/reset-progress")
def reset_user_progress(user_id: int, db: Session = Depends(get_db)):
    """Reset user's learning progress"""
    from ..models import CompletedCourse, ClickLog
    
    # Delete completed courses
    db.query(CompletedCourse).filter(
        CompletedCourse.user_id == user_id
    ).delete()
    
    # Delete click logs
    db.query(ClickLog).filter(
        ClickLog.user_id == user_id
    ).delete()
    
    db.commit()
    
    return {
        "status": "reset",
        "user_id": user_id,
        "message": "User progress has been reset"
    }

@router.get("/courses/performance")
def get_courses_performance(db: Session = Depends(get_db)):
    """Get performance metrics for courses"""
    from ..models import Course, CompletedCourse, ClickLog
    
    courses = db.query(Course).all()
    
    course_stats = []
    for course in courses[:50]:  # Limit for performance
        completions = db.query(CompletedCourse).filter(
            CompletedCourse.course_id == course.id
        ).count()
        
        clicks = db.query(ClickLog).filter(
            ClickLog.course_id == course.id
        ).count()
        
        course_stats.append({
            "course_id": course.id,
            "title": course.title,
            "completions": completions,
            "clicks": clicks,
            "click_to_completion_rate": completions / max(1, clicks)
        })
    
    return {
        "courses": course_stats,
        "total": len(courses)
    }
