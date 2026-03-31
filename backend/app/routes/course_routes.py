from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from sqlalchemy.orm import Session

from ..database import SessionLocal
from ..models.course import Course, CompletedCourse
from ..auth.oauth2 import get_current_user, require_admin

router = APIRouter(prefix="/courses", tags=["courses"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
class CourseCreate(BaseModel):
    title: str
    description: Optional[str] = None
    difficulty: Optional[str] = "Beginner"
    career_path: Optional[str] = None

class CourseOut(BaseModel):
    id: int
    title: str
    description: Optional[str]
    difficulty: Optional[str]
    career_path: Optional[str]
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

# PUBLIC: list courses (optionally exclude completed by user_id)
@router.get("", response_model=List[CourseOut])
@router.get("/", response_model=List[CourseOut])
async def list_courses(
    limit: int = Query(50, ge=1, le=100),
    skip: int = 0,
    user_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Get a list of courses with optional user-specific filtering.
    - limit: Number of courses to return (1-100, default 50)
    - skip: Number of courses to skip (for pagination)
    - user_id: If provided, will exclude courses already completed by this user
    """
    try:
        # Get completed course IDs for the user if user_id is provided
        excluded = []
        if user_id is not None:
            rows = db.query(CompletedCourse).filter(CompletedCourse.user_id == user_id).all()
            excluded = [r.course_id for r in rows]

        # Build the base query
        query = db.query(Course)
        
        # Apply exclusions if needed
        if excluded:
            query = query.filter(~Course.id.in_(excluded))
            
        # Apply ordering and pagination
        courses = query.order_by(Course.created_at.desc())\
                      .offset(skip)\
                      .limit(limit)\
                      .all()
                      
        return courses
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching courses: {str(e)}"
        )

# ADMIN: create a course (protected)
@router.post("/admin", response_model=CourseOut, status_code=status.HTTP_201_CREATED, dependencies=[Depends(require_admin)])
def create_course(req: CourseCreate, db: Session = Depends(get_db)):
    course = Course(
        title=req.title,
        description=req.description,
        difficulty=req.difficulty,
        career_path=req.career_path,
    )
    db.add(course)
    db.commit()
    db.refresh(course)
    return course

# ADMIN: update a course (protected)
@router.put("/admin/{course_id}", response_model=CourseOut, dependencies=[Depends(require_admin)])
def update_course(course_id: int, req: CourseCreate, db: Session = Depends(get_db)):
    course = db.query(Course).filter(Course.id == course_id).first()
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    course.title = req.title
    course.description = req.description
    course.difficulty = req.difficulty
    course.career_path = req.career_path
    db.commit()
    db.refresh(course)
    return course

# ADMIN: delete a course (protected)
@router.delete("/admin/{course_id}", status_code=status.HTTP_204_NO_CONTENT, dependencies=[Depends(require_admin)])
def delete_course(course_id: int, db: Session = Depends(get_db)):
    course = db.query(Course).filter(Course.id == course_id).first()
    if not course:
        raise HTTPException(status_code=404, detail="Course not found")
    db.delete(course)
    db.commit()
    return None
