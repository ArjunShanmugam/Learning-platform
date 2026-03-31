"""
Database models for recommendation system.
"""
from sqlalchemy import Column, Integer, String, DateTime, JSON, ForeignKey
from sqlalchemy.sql import func
from .base import Base

class RecommendationLog(Base):
    __tablename__ = "recommendation_logs"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    recommendation_id = Column(String(255), unique=True, index=True)
    model_version = Column(String(50))
    recommended_items = Column(JSON)  # List of recommended item IDs
    client_ip = Column(String(45))
    user_agent = Column(String(512))
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class RecommendationFeedbackLog(Base):
    __tablename__ = "recommendation_feedback"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True)
    item_id = Column(Integer, index=True)
    feedback = Column(String(10))  # "like" or "dislike"
    recommendation_id = Column(String(255), index=True, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

# UserInteraction model moved to interaction.py
