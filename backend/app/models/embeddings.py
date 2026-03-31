from sqlalchemy import Column, Integer, Float, ForeignKey, JSON, String, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .base import Base

class CourseEmbedding(Base):
    __tablename__ = "course_embeddings"

    id = Column(Integer, primary_key=True, index=True)
    course_id = Column(Integer, ForeignKey("courses.id"), unique=True)
    # Store embedding as JSON array (compatible with all databases)
    embedding = Column(JSON, nullable=False)
    embedding_model = Column(String(128), default="sentence-transformers/all-MiniLM-L6-v2")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    course = relationship("Course", back_populates="embedding")