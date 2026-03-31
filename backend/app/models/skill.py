from sqlalchemy import Column, Integer, ForeignKey, Float, String, DateTime, func
from sqlalchemy.orm import relationship
from .base import Base

class SkillProgression(Base):
    __tablename__ = "skill_progressions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)

    # Historical fields for progression events
    previous_level = Column(String(50), nullable=True)
    new_level = Column(String(50), nullable=True)
    reason = Column(String(256), nullable=True)

    # Backwards-compatible fields
    skill_name = Column(String(100), nullable=True)
    level = Column(Integer, default=1)
    experience = Column(Float, default=0.0)

    created_at = Column(DateTime, server_default=func.now())

    # Relationships
    user = relationship("User", back_populates="skill_progressions")