"""
Search Routes for Phase 2 - Semantic Search and Recommendations
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import List, Optional
from ..db import SessionLocal
from ..models import Course, CourseEmbedding, CompletedCourse, User
from ..services.embedding_service import get_embedding_service
from ..services.recommender_service import get_recommender_service
from ..auth.oauth2 import get_current_user, get_current_user_optional

router = APIRouter(prefix="/search", tags=["search"])

class SearchRequest(BaseModel):
    q: str
    user_id: Optional[int] = None

class SearchResult(BaseModel):
    course_id: int
    title: str
    description: Optional[str]
    difficulty: Optional[str]
    career_path: Optional[str]
    similarity_score: float

class RecommendationResult(BaseModel):
    course_id: int
    title: str
    description: Optional[str]
    difficulty: Optional[str]
    career_path: Optional[str]
    score: float
    reason: str

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# In-memory search cache for instant repeated queries
_search_cache = {}

@router.post("/semantic")
def semantic_search(
    request: SearchRequest,
    db: Session = Depends(get_db)
) -> List[SearchResult]:
    """
    Fast Google-style search - returns ALL matching courses
    Uses caching for instant repeated searches
    
    Args:
        request: Search query and optional user_id
        db: Database session
        
    Returns:
        All courses matching the search query, ranked by relevance
    """
    if not request.q or len(request.q.strip()) < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Search query cannot be empty"
        )
    
    query = request.q.strip()
    query_lower = query.lower()
    
    # Check cache first (instant response for repeated searches)
    if query_lower in _search_cache:
        return _search_cache[query_lower]
    
    from sqlalchemy import or_, func
    
    search_pattern = f"%{query_lower}%"
    
    # Get ALL matching courses - no limit
    courses = db.query(Course).filter(
        or_(
            func.lower(Course.title).like(search_pattern),
            func.lower(Course.description).like(search_pattern)
        )
    ).all()
    
    results = []
    for course in courses:
        title_lower = (course.title or "").lower()
        
        # Title match gets higher score
        if query_lower in title_lower:
            score = 10.0
        else:
            score = 7.0  # Description match
        
        results.append(SearchResult(
            course_id=course.id,
            title=course.title,
            description=course.description,
            difficulty=course.difficulty,
            career_path=course.career_path,
            similarity_score=score
        ))
    
    # Sort by score (title matches first)
    results.sort(key=lambda x: x.similarity_score, reverse=True)
    
    # Cache results for next search
    _search_cache[query_lower] = results
    
    return results

@router.get("/recommendations")
def get_recommendations(
    k: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> List[RecommendationResult]:
    """
    Get personalized hybrid recommendations for current user
    
    Args:
        k: Number of recommendations to return (default 10, max 50)
        db: Database session
        current_user: Current authenticated user
        
    Returns:
        List of personalized course recommendations with reasons
    """
    recommender_service = get_recommender_service()
    
    recommendations = recommender_service.get_hybrid_recommendations(
        user_id=current_user.id,
        db=db,
        k=k
    )
    
    results = [
        RecommendationResult(
            course_id=rec['course_id'],
            title=rec['title'],
            description=rec['description'],
            difficulty=rec['difficulty'],
            career_path=rec['career_path'],
            score=rec['score'],
            reason=rec['reason']
        )
        for rec in recommendations
    ]
    
    return results

@router.post("/log-interaction")
def log_interaction(
    course_id: int = Query(...),
    interaction_type: str = Query(..., pattern="^(view|click|start|complete)$"),
    db: Session = Depends(get_db),
    current_user: Optional[User] = Depends(get_current_user_optional)
):
    """
    Log user interaction for collaborative filtering
    
    Args:
        course_id: Course ID
        interaction_type: Type of interaction (view, click, start, complete)
        db: Database session
        current_user: Current authenticated user (optional)
        
    Returns:
        Success message
    """
    # Only log if user is authenticated
    if not current_user:
        return {"status": "skipped", "message": "Not authenticated - interaction not logged"}
    
    recommender_service = get_recommender_service()
    
    try:
        recommender_service.log_interaction(
            user_id=current_user.id,
            course_id=course_id,
            interaction_type=interaction_type,
            db=db
        )
        return {"status": "success", "message": "Interaction logged"}
    except Exception as e:
        # Don't fail the request if interaction logging fails
        return {"status": "error", "message": str(e)}

@router.get("/trending")
def get_trending_courses(
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
) -> List[dict]:
    """
    Get trending courses based on recent interactions
    
    Args:
        limit: Number of trending courses to return
        db: Database session
        
    Returns:
        List of trending courses
    """
    from sqlalchemy import func, desc
    from ..models import UserInteraction
    
    # Get most interacted courses in last 30 days
    trending = db.query(
        Course.id,
        Course.title,
        Course.description,
        Course.difficulty,
        Course.career_path,
        func.count(UserInteraction.id).label('interaction_count'),
        func.avg(UserInteraction.weight).label('avg_weight')
    ).join(
        UserInteraction, Course.id == UserInteraction.course_id
    ).group_by(
        Course.id
    ).order_by(
        desc('interaction_count')
    ).limit(limit).all()
    
    results = []
    for course in trending:
        results.append({
            'course_id': course.id,
            'title': course.title,
            'description': course.description,
            'difficulty': course.difficulty,
            'career_path': course.career_path,
            'interaction_count': course.interaction_count,
            'popularity_score': course.avg_weight or 1.0
        })
    
    return results
