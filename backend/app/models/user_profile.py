from sqlalchemy import Column, Integer, String, ForeignKey, Nullable
from sqlalchemy.orm import relationship
from .base import Base

class UserProfile(Base):
    __tablename__ = "user_profiles"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True)
    role = Column(String(50), default="user")
    skill_level = Column(String(50), default="Beginner")
    career_path = Column(String(100), default="general")
    full_name = Column(String(255), nullable=True, default=None)
    bio = Column(String(1000), default="")
    profile_picture = Column(String(500), default="")
    
    user = relationship("User", back_populates="profile")