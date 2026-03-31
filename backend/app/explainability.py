"""
Explainability & Feedback System
Provides "why recommended" explanations and collects user feedback for model improvement
"""

from typing import List, Dict, Optional
from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

class RecommendationExplanation(BaseModel):
    """Explanation for why a course was recommended"""
    course_id: int
    course_title: str
    reason: str
    factors: List[str]  # e.g., ["matches_career_path", "high_rating", "user_skill_level"]
    confidence: float  # 0-1
    model_version: str

class UserFeedback(Base):
    """Store user feedback on recommendations"""
    __tablename__ = "recommendation_feedback"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False)
    course_id = Column(Integer, nullable=False)
    recommendation_id = Column(String, nullable=True)  # ID of the recommendation
    helpful = Column(Integer, default=0)  # 0=not helpful, 1=helpful
    rating = Column(Float, nullable=True)  # 1-5 star rating
    feedback_text = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<UserFeedback user={self.user_id} course={self.course_id} helpful={self.helpful}>"

class ExplanationEngine:
    """Generates explanations for recommendations"""
    
    @staticmethod
    def explain_recommendation(
        user_id: int,
        course_id: int,
        course_title: str,
        ranking_score: float,
        user_features: Dict,
        course_features: Dict,
        model_version: str = "v1.0"
    ) -> RecommendationExplanation:
        """
        Generate explanation for why course was recommended
        
        Args:
            user_id: User ID
            course_id: Course ID
            course_title: Course title
            ranking_score: ML model ranking score
            user_features: Dictionary of user feature values
            course_features: Dictionary of course feature values
            model_version: Version of recommendation model
        
        Returns:
            RecommendationExplanation object
        """
        factors = []
        confidence = min(ranking_score, 1.0)
        
        # Analyze factors for explanation
        if course_features.get('difficulty', 'Beginner') == 'Beginner' and \
           user_features.get('level', 'Beginner') == 'Beginner':
            factors.append("matches_your_level")
        
        if course_features.get('career_path') == user_features.get('career_path'):
            factors.append("aligns_with_career_goals")
        
        if course_features.get('rating', 0) >= 4.5:
            factors.append("highly_rated_by_students")
        
        if user_features.get('completed_similar', 0) > 0:
            factors.append("complements_your_learning_path")
        
        if course_features.get('student_count', 0) > 1000:
            factors.append("popular_and_trusted")
        
        # Generate human-readable reason
        if factors:
            reason = ExplanationEngine._generate_reason(factors, course_title)
        else:
            reason = f"{course_title} was selected based on your learning profile and interests."
        
        return RecommendationExplanation(
            course_id=course_id,
            course_title=course_title,
            reason=reason,
            factors=factors,
            confidence=float(confidence),
            model_version=model_version
        )
    
    @staticmethod
    def _generate_reason(factors: List[str], course_title: str) -> str:
        """Generate human-readable explanation from factors"""
        reasons = {
            "matches_your_level": "matches your current skill level",
            "aligns_with_career_goals": "aligns with your career goals",
            "highly_rated_by_students": "is highly rated by other students",
            "complements_your_learning_path": "complements your previous courses",
            "popular_and_trusted": "is popular and well-trusted",
        }
        
        reason_parts = [reasons.get(f, f) for f in factors[:3]]
        reason_text = ", ".join(reason_parts)
        
        return f"{course_title} was recommended because it {reason_text}."
    
    @staticmethod
    def get_explanation_for_search(
        query: str,
        course_title: str,
        similarity_score: float
    ) -> str:
        """Generate explanation for search result"""
        percentage = int(similarity_score * 100)
        return f"{course_title} is {percentage}% relevant to your search for '{query}'"


class FeedbackCollector:
    """Collects and aggregates user feedback"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def record_feedback(
        self,
        user_id: int,
        course_id: int,
        helpful: bool,
        rating: Optional[int] = None,
        feedback_text: Optional[str] = None,
        recommendation_id: Optional[str] = None
    ) -> UserFeedback:
        """Record user feedback on a recommendation"""
        feedback = UserFeedback(
            user_id=user_id,
            course_id=course_id,
            helpful=1 if helpful else 0,
            rating=rating,
            feedback_text=feedback_text,
            recommendation_id=recommendation_id
        )
        
        self.db.add(feedback)
        self.db.commit()
        self.db.refresh(feedback)
        
        return feedback
    
    def get_feedback_stats(self, course_id: int) -> Dict:
        """Get feedback statistics for a course"""
        feedbacks = self.db.query(UserFeedback).filter(
            UserFeedback.course_id == course_id
        ).all()
        
        if not feedbacks:
            return {"total": 0, "helpful_rate": 0, "avg_rating": 0}
        
        total = len(feedbacks)
        helpful_count = len([f for f in feedbacks if f.helpful == 1])
        ratings = [f.rating for f in feedbacks if f.rating is not None]
        avg_rating = sum(ratings) / len(ratings) if ratings else 0
        
        return {
            "total": total,
            "helpful_count": helpful_count,
            "helpful_rate": helpful_count / total,
            "avg_rating": avg_rating
        }
    
    def get_feedback_for_retraining(self) -> List[Dict]:
        """Get feedback data for model retraining"""
        feedbacks = self.db.query(UserFeedback).filter(
            UserFeedback.helpful.isnot(None)
        ).all()
        
        training_data = [
            {
                "user_id": f.user_id,
                "course_id": f.course_id,
                "label": f.helpful,
                "rating": f.rating
            }
            for f in feedbacks
        ]
        
        return training_data


if __name__ == "__main__":
    pass
