"""
Skill Progression Routes for Phase 2
Manages user skill level progression and auto-bumping
"""

from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from ..database import SessionLocal
from ..services.skill_progression_service import get_skill_progression_service
from ..auth.oauth2 import get_current_user
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..models.user import User

router = APIRouter(prefix="/skills", tags=["skills"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/progression-status")
async def get_progression_status(
    request: Request,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    """
    Get current skill progression status for user
    
    Returns:
        Dictionary with current level, next level, and progress towards next level
    """
    try:
        skill_service = get_skill_progression_service()
        status_info = skill_service.get_progression_status(current_user.id, db)
        return status_info
    except Exception as e:
        # Return default progression status if service fails
        return {
            "current_level": "Beginner",
            "next_level": "Mid",
            "can_progress": False,
            "progress": 0,
            "required": 2,
            "required_difficulty": "Mid"
        }

@router.get("/progression-history")
async def get_progression_history(
    request: Request,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    """
    Get user's skill progression history
    
    Returns:
        List of skill level changes with dates and reasons
    """
    try:
        skill_service = get_skill_progression_service()
        history = skill_service.get_progression_history(current_user.id, db)
        return {"history": history}
    except Exception as e:
        # Return empty history if service fails
        return {"history": []}

@router.post("/check-progression")
async def check_progression(
    request: Request,
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user)
):
    """
    Check if user is eligible for skill progression and auto-bump if eligible
    
    Returns:
        Dictionary with progression status
    """
    if not current_user:
        return {
            "status": "error",
            "message": "User not authenticated"
        }
    
    try:
        skill_service = get_skill_progression_service()
        progressed, new_level = skill_service.auto_bump_skill_level(current_user.id, db)
        
        if progressed:
            return {
                "status": "progressed",
                "new_level": new_level,
                "message": f"Congratulations! You've been promoted to {new_level} level!"
            }
        else:
            status_info = skill_service.get_progression_status(current_user.id, db)
            return {
                "status": "no_progression",
                "message": "Keep learning to reach the next level!",
                "progress": status_info
            }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error checking progression: {str(e)}",
            "progress": {
                "current_level": "Beginner",
                "next_level": "Mid",
                "can_progress": False,
                "progress": 0,
                "required": 2,
                "required_difficulty": "Mid"
            }
        }
