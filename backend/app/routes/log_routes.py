from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from typing import Optional
from sqlalchemy.orm import Session
from ..db import SessionLocal
from ..models import SearchLog, ClickLog, CompletedCourse, Course

router = APIRouter(prefix="/logs", tags=["logs"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
class SearchLogIn(BaseModel):
    user_id: Optional[int] = None
    query: str

class ClickLogIn(BaseModel):
    user_id: Optional[int] = None
    course_id: int
    event: Optional[str] = "open"  # open/start/complete

class CompleteIn(BaseModel):
    user_id: int
    course_id: int

class StartIn(BaseModel):
    user_id: int
    course_id: int

@router.post("/start", status_code=status.HTTP_201_CREATED)
def mark_start(payload: StartIn, db: Session = Depends(get_db)):
    """Log course start event"""
    # Just return success - we track this via ClickLog
    return {"status": "started", "user_id": payload.user_id, "course_id": payload.course_id}

# POST /logs/search
@router.post("/search", status_code=status.HTTP_201_CREATED)
def create_search_log(payload: SearchLogIn, db: Session = Depends(get_db)):
    row = SearchLog(user_id=payload.user_id, query=payload.query)
    db.add(row)
    db.commit()
    db.refresh(row)
    return {"id": row.id, "created_at": row.created_at, "query": row.query}

# POST /logs/click
@router.post("/click", status_code=status.HTTP_201_CREATED)
def create_click_log(payload: ClickLogIn, db: Session = Depends(get_db)):
    # Optional: validate course id exists
    course = db.query(Course).filter(Course.id == payload.course_id).first()
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    row = ClickLog(user_id=payload.user_id, course_id=payload.course_id, event=payload.event)
    db.add(row)
    db.commit()
    db.refresh(row)
    return {"id": row.id, "course_id": row.course_id, "event": row.event, "created_at": row.created_at}

# POST /logs/complete
@router.post("/complete", status_code=status.HTTP_201_CREATED)
def mark_complete(payload: CompleteIn, db: Session = Depends(get_db)):
    # avoid duplicate completion records
    exists = db.query(CompletedCourse).filter(
        CompletedCourse.user_id == payload.user_id,
        CompletedCourse.course_id == payload.course_id
    ).first()
    if exists:
        return {"status": "already_marked", "id": exists.id}
    row = CompletedCourse(user_id=payload.user_id, course_id=payload.course_id)
    db.add(row)
    db.commit()
    db.refresh(row)
    
    # FIX #2: Auto-bump skill level on course completion
    try:
        from ..services.skill_progression_service import get_skill_progression_service
        skill_service = get_skill_progression_service()
        progressed, new_level = skill_service.auto_bump_skill_level(payload.user_id, db)
        
        response = {
            "status": "completed",
            "id": row.id,
            "completed_at": row.completed_at,
            "progression": None
        }
        
        if progressed and new_level:
            response["progression"] = {
                "progressed": True,
                "new_level": new_level,
                "message": f"🎉 Congratulations! You've been promoted to {new_level} level!"
            }
        
        return response
    except Exception as e:
        # Fallback to basic response if skill service fails
        return {"status": "completed", "id": row.id, "completed_at": row.completed_at, "error": str(e)}

# GET /logs/completed-courses
@router.get("/completed-courses")
def get_completed_courses(user_id: Optional[int] = None, db: Session = Depends(get_db)):
    """Get all completed courses for a user"""
    if user_id is None:
        # Try to get from header or default to user 1
        user_id = 1
    
    # Get completed course IDs
    completed = db.query(CompletedCourse).filter(
        CompletedCourse.user_id == user_id
    ).all()
    
    completed_ids = [c.course_id for c in completed]
    
    if not completed_ids:
        return {"courses": [], "count": 0}
    
    # Get course details for completed courses
    courses = db.query(Course).filter(
        Course.id.in_(completed_ids)
    ).all()
    
    return {
        "courses": [
            {
                "id": c.id,
                "title": c.title,
                "description": c.description,
                "difficulty": c.difficulty,
                "career_path": c.career_path,
                "created_at": c.created_at.isoformat() if c.created_at else None
            }
            for c in courses
        ],
        "count": len(courses)
    }
