from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List, Optional
from .. import models
from ..schemas import interaction_schemas

class InteractionService:
    def __init__(self, db: Session):
        self.db = db

    def log_interaction(
        self,
        user_id: int,
        course_id: int,
        interaction_type: str,
        rating: Optional[float] = None
    ) -> models.UserInteraction:
        """
        Log a user interaction with a course
        
        Args:
            user_id: ID of the user
            course_id: ID of the course
            interaction_type: Type of interaction ('view', 'enroll', 'complete', 'rate')
            rating: Optional rating (1-5) if interaction_type is 'rate'
            
        Returns:
            The created interaction record
        """
        interaction = models.UserInteraction(
            user_id=user_id,
            course_id=course_id,
            interaction_type=interaction_type,
            rating=rating if interaction_type == 'rate' else None
        )
        
        self.db.add(interaction)
        self.db.commit()
        self.db.refresh(interaction)
        return interaction

    def get_user_interactions(
        self,
        user_id: int,
        days: Optional[int] = None,
        interaction_type: Optional[str] = None
    ) -> List[models.UserInteraction]:
        """
        Get a user's interactions
        
        Args:
            user_id: ID of the user
            days: Optional number of days to look back
            interaction_type: Optional filter by interaction type
            
        Returns:
            List of interaction records
        """
        query = self.db.query(models.UserInteraction).filter(
            models.UserInteraction.user_id == user_id
        )
        
        if days:
            since_date = datetime.utcnow() - timedelta(days=days)
            query = query.filter(models.UserInteraction.created_at >= since_date)
            
        if interaction_type:
            query = query.filter(models.UserInteraction.interaction_type == interaction_type)
            
        return query.order_by(models.UserInteraction.created_at.desc()).all()

    def has_interacted_with_course(
        self,
        user_id: int,
        course_id: int,
        interaction_type: Optional[str] = None
    ) -> bool:
        """
        Check if a user has interacted with a course
        
        Args:
            user_id: ID of the user
            course_id: ID of the course
            interaction_type: Optional filter by interaction type
            
        Returns:
            True if interaction exists, False otherwise
        """
        query = self.db.query(models.UserInteraction).filter(
            models.UserInteraction.user_id == user_id,
            models.UserInteraction.course_id == course_id
        )
        
        if interaction_type:
            query = query.filter(models.UserInteraction.interaction_type == interaction_type)
            
        return self.db.query(query.exists()).scalar()


def get_interaction_service(db: Session) -> InteractionService:
    return InteractionService(db)
