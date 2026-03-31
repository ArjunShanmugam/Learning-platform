from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from .. import models, schemas
from ..auth.oauth2 import get_current_user
from ..database import get_db
from ..services.interaction_service import get_interaction_service

router = APIRouter(
    prefix="/api/v1/interactions",
    tags=['Interactions']
)

@router.post("/", response_model=schemas.interaction_schemas.Interaction)
async def log_interaction(
    interaction: schemas.interaction_schemas.InteractionCreate,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Log a user interaction with a course
    """
    # Verify the user is logging their own interaction
    if interaction.user_id != current_user.id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to log interactions for this user"
        )
    
    # Verify the course exists
    course = db.query(models.Course).filter(models.Course.id == interaction.course_id).first()
    if not course:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Course not found"
        )
    
    # Log the interaction
    interaction_service = get_interaction_service(db)
    return interaction_service.log_interaction(
        user_id=interaction.user_id,
        course_id=interaction.course_id,
        interaction_type=interaction.interaction_type,
        rating=interaction.rating
    )

@router.get("/user/{user_id}", response_model=List[schemas.interaction_schemas.Interaction])
async def get_user_interactions(
    user_id: int,
    days: Optional[int] = None,
    interaction_type: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Get a user's interactions
    """
    # Users can only view their own interactions unless they're an admin
    if user_id != current_user.id and current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view these interactions"
        )
    
    interaction_service = get_interaction_service(db)
    return interaction_service.get_user_interactions(
        user_id=user_id,
        days=days,
        interaction_type=interaction_type
    )

@router.get("/stats/user/{user_id}", response_model=schemas.interaction_schemas.InteractionStats)
async def get_user_interaction_stats(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Get interaction statistics for a user
    """
    # Users can only view their own stats unless they're an admin
    if user_id != current_user.id and current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to view these stats"
        )
    
    interaction_service = get_interaction_service(db)
    
    # Get all interactions for the user
    interactions = interaction_service.get_user_interactions(user_id=user_id)
    
    if not interactions:
        return schemas.interaction_schemas.InteractionStats()
    
    # Calculate stats
    stats = {
        'total_views': 0,
        'total_enrollments': 0,
        'total_completions': 0,
        'ratings': [],
        'last_interaction': max(i.created_at for i in interactions)
    }
    
    for interaction in interactions:
        if interaction.interaction_type == 'view':
            stats['total_views'] += 1
        elif interaction.interaction_type == 'enroll':
            stats['total_enrollments'] += 1
        elif interaction.interaction_type == 'complete':
            stats['total_completions'] += 1
        elif interaction.interaction_type == 'rate' and interaction.rating is not None:
            stats['ratings'].append(interaction.rating)
    
    # Calculate average rating if ratings exist
    avg_rating = sum(stats['ratings']) / len(stats['ratings']) if stats['ratings'] else None
    
    return {
        'total_views': stats['total_views'],
        'total_enrollments': stats['total_enrollments'],
        'total_completions': stats['total_completions'],
        'average_rating': avg_rating,
        'last_interaction': stats['last_interaction']
    }
