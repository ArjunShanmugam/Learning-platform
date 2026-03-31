from sqlalchemy import String, Boolean, ForeignKey, Column, Text, DateTime, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import BaseModel, Base

class User(BaseModel, Base):
    """User model for authentication and authorization."""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)
    is_verified = Column(Boolean, default=False)
    role = Column(String(50), default='user', nullable=False)
    last_login = Column(DateTime, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    profile = relationship("UserProfile", 
                         back_populates="user", 
                         uselist=False, 
                         cascade="all, delete-orphan",
                         foreign_keys="UserProfile.user_id")
    
    courses_taught = relationship("Course", 
                                back_populates="instructor",
                                cascade="all, delete-orphan",
                                foreign_keys="Course.instructor_id")
    
    completed_courses = relationship("CompletedCourse", 
                                   back_populates="user",
                                   cascade="all, delete-orphan",
                                   foreign_keys="CompletedCourse.user_id")
    
    search_logs = relationship("SearchLog", 
                             back_populates="user",
                             cascade="all, delete-orphan",
                             foreign_keys="SearchLog.user_id")
    
    click_logs = relationship("ClickLog", 
                            back_populates="user",
                            cascade="all, delete-orphan",
                            foreign_keys="ClickLog.user_id")
    
    interactions = relationship("UserInteraction", 
                              back_populates="user",
                              cascade="all, delete-orphan",
                              foreign_keys="UserInteraction.user_id")
    
    skill_progressions = relationship("SkillProgression", 
                                    back_populates="user",
                                    cascade="all, delete-orphan",
                                    foreign_keys="SkillProgression.user_id")
    
    def __repr__(self):
        return f"<User(id={self.id}, email={self.email}, role={self.role})>"
    
    @property
    def is_admin(self):
        """Check if user has admin role."""
        return self.role == "admin"
    
    @classmethod
    def get_by_email(cls, db, email: str):
        """Get user by email."""
        return db.query(cls).filter(cls.email == email).first()
    
    def verify_password(self, password: str) -> bool:
        """Verify user password."""
        from ..auth.oauth2 import verify_password
        return verify_password(password, self.hashed_password)
    
    def update_last_login(self, db):
        """Update user's last login timestamp."""
        from sqlalchemy import func
        self.last_login = func.now()
        db.commit()