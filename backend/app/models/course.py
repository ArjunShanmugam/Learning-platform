from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
from .base import Base

class Course(Base):
    __tablename__ = "courses"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(255), index=True)
    description = Column(Text)
    difficulty = Column(String(50), default="Beginner")
    career_path = Column(String(100), nullable=True)
    instructor_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    instructor = relationship("User", back_populates="courses_taught", foreign_keys=[instructor_id])
    completed_by = relationship("CompletedCourse", back_populates="course")
    embedding = relationship("CourseEmbedding", back_populates="course", uselist=False)
    interactions = relationship("UserInteraction", back_populates="course")

class CompletedCourse(Base):
    __tablename__ = "completed_courses"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    course_id = Column(Integer, ForeignKey("courses.id"))
    completed_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relationships
    user = relationship("User", back_populates="completed_courses")
    course = relationship("Course", back_populates="completed_by")