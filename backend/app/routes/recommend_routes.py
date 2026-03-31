"""
FastAPI routes for recommendations with hybrid scoring.
"""
from fastapi import APIRouter, Depends, HTTPException, status, Request
from typing import List, Optional
from pydantic import BaseModel
from sqlalchemy.orm import Session
from ..models import User, UserProfile, Course
from ..services.recommender_service_v2 import get_recommender_service_v2 as get_recommender_service
from ..auth.oauth2 import get_current_user
from ..db import SessionLocal

router = APIRouter(tags=["recommendations"])

class RecommendationResult(BaseModel):
    course_id: int
    title: str
    description: Optional[str] = None
    difficulty: Optional[str] = None
    career_path: Optional[str] = None
    score: float
    reason: str
    content_score: float
    collab_score: float

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("", response_model=List[RecommendationResult])
@router.get("/home", response_model=List[RecommendationResult])
def get_recommendations(
    limit: Optional[int] = None,
    top_n: Optional[int] = None,
    user_id: Optional[int] = None,
    db: Session = Depends(get_db)
) -> List[RecommendationResult]:
    """
    Get personalized recommendations using hybrid scoring.
    
    Combines:
    - 60% Content-based filtering (skill level, career path matching)
    - 40% Collaborative filtering (similar user cohorts)
    
    Args:
        limit: Number of recommendations (from frontend, legacy param name)
        top_n: Number of recommendations (backend param name)
        user_id: Optional user ID override
        current_user: Authenticated user (from JWT token)
        
    Returns:
        List of recommended courses with scores and reasons
    """
    try:
        # Use limit if provided, otherwise top_n, default to 9
        k = limit or top_n or 9
        k = max(1, min(20, k))
        
        # Get user ID - prefer param, default to 1
        target_user_id = user_id or 1
        
        # Get user profile
        user_profile = db.query(UserProfile).filter(
            UserProfile.user_id == target_user_id
        ).first()
        
        if not user_profile:
            # Create default profile if doesn't exist
            user_profile = UserProfile(
                user_id=target_user_id,
                skill_level="Beginner",
                career_path="General"
            )
            db.add(user_profile)
            db.commit()
            db.refresh(user_profile)
        
        # Get recommendations from service
        recommender = get_recommender_service()
        recommendations = recommender.get_hybrid_recommendations(
            user_id=target_user_id,
            db=db,
            k=k
        )
        
        return [RecommendationResult(**r) for r in recommendations]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error generating recommendations: {str(e)}"
        )


@router.post("/feedback")
def log_recommendation_feedback(
    course_id: int,
    feedback: str,  # "like" or "dislike"
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Log user feedback on a recommendation for model improvement.
    
    Args:
        course_id: ID of the course
        feedback: "like" or "dislike"
    """
    try:
        if feedback not in ["like", "dislike"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Feedback must be 'like' or 'dislike'"
            )
        
        # Log interaction (implicit feedback)
        recommender = get_recommender_service()
        interaction_type = "click" if feedback == "like" else "skip"
        recommender.log_interaction(current_user.id, course_id, interaction_type, db)
        
        return {
            "status": "feedback_logged",
            "course_id": course_id,
            "feedback": feedback
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error logging feedback: {str(e)}"
        )

