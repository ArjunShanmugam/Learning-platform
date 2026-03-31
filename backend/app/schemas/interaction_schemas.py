from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class InteractionBase(BaseModel):
    user_id: int
    course_id: int
    interaction_type: str = Field(..., description="Type of interaction (view, enroll, complete, rate)")
    rating: Optional[float] = Field(None, ge=1, le=5, description="Rating value (1-5) for 'rate' interaction type")

class InteractionCreate(InteractionBase):
    pass

class Interaction(InteractionBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True  # Updated from orm_mode = True for Pydantic v2

class InteractionStats(BaseModel):
    total_views: int = 0
    total_enrollments: int = 0
    total_completions: int = 0
    average_rating: Optional[float] = None
    last_interaction: Optional[datetime] = None
