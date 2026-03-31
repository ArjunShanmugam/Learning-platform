"""
Skill Progression Service for Phase 2
Manages user skill level progression and auto-bumping rules
"""

from typing import Tuple, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func
from ..models import (
    User, UserProfile, CompletedCourse, Course, SkillProgression
)

class SkillProgressionService:
    def __init__(self):
        # Define progression rules
        self.skill_levels = ["Beginner", "Mid", "Expert"]
        
        # Rules for auto-progression
        self.progression_rules = {
            "Beginner": {
                "next_level": "Mid",
                "required_courses": 2,
                "required_difficulty": "Mid"
            },
            "Mid": {
                "next_level": "Expert",
                "required_courses": 2,
                "required_difficulty": "Expert"
            },
            "Expert": {
                "next_level": None,
                "required_courses": None,
                "required_difficulty": None
            }
        }
    
    def check_progression(
        self,
        user_id: int,
        db: Session
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if user is eligible for skill progression
        
        Args:
            user_id: User ID
            db: Database session
            
        Returns:
            Tuple of (should_progress, new_level)
        """
        user = db.query(User).filter(User.id == user_id).first()
        if not user or not user.profile:
            return False, None
        
        current_level = user.profile.skill_level
        if current_level not in self.progression_rules:
            return False, None
        
        rule = self.progression_rules[current_level]
        
        # Check if next level exists
        if rule["next_level"] is None:
            return False, None
        
        # Count completed courses at required difficulty
        completed_at_difficulty = db.query(
            func.count(CompletedCourse.id)
        ).join(
            Course, CompletedCourse.course_id == Course.id
        ).filter(
            CompletedCourse.user_id == user_id,
            Course.difficulty == rule["required_difficulty"]
        ).scalar()
        
        # If user has a career_path, ensure at least one of the completed courses is in that career path
        career_required_met = True
        user_career = user.profile.career_path if user.profile and user.profile.career_path else None
        if user_career:
            completed_with_career = db.query(
                func.count(CompletedCourse.id)
            ).join(
                Course, CompletedCourse.course_id == Course.id
            ).filter(
                CompletedCourse.user_id == user_id,
                Course.difficulty == rule["required_difficulty"],
                Course.career_path == user_career
            ).scalar()
            career_required_met = (completed_with_career >= 1)
        
        # Check if user has completed enough courses AND (career requirement satisfied or not set)
        if completed_at_difficulty >= rule["required_courses"] and career_required_met:
            return True, rule["next_level"]
        
        return False, None
    
    def auto_bump_skill_level(
        self,
        user_id: int,
        db: Session
    ) -> Tuple[bool, Optional[str]]:
        """
        Automatically bump user skill level if eligible
        
        Args:
            user_id: User ID
            db: Database session
            
        Returns:
            Tuple of (progressed, new_level)
        """
        should_progress, new_level = self.check_progression(user_id, db)
        
        if not should_progress or new_level is None:
            return False, None
        
        user = db.query(User).filter(User.id == user_id).first()
        if not user or not user.profile:
            return False, None
        
        old_level = user.profile.skill_level
        
        # Update user profile
        user.profile.skill_level = new_level
        
        # Build a reason string that mentions career requirement if applicable
        req = self.progression_rules[old_level]['required_courses']
        diff = self.progression_rules[old_level]['required_difficulty']
        career_note = ""
        if user.profile and user.profile.career_path:
            career_note = f" including at least one {user.profile.career_path} course"
        reason_msg = f"Completed {req} {diff} courses{career_note}"
        
        # Log progression
        progression = SkillProgression(
            user_id=user_id,
            previous_level=old_level,
            new_level=new_level,
            reason=reason_msg
        )
        db.add(progression)
        db.commit()
        
        return True, new_level
    
    def get_progression_status(
        self,
        user_id: int,
        db: Session
    ) -> dict:
        """
        Get detailed progression status for user
        
        Args:
            user_id: User ID
            db: Database session
            
        Returns:
            Dictionary with progression details
        """
        user = db.query(User).filter(User.id == user_id).first()
        if not user or not user.profile:
            return {}
        
        current_level = user.profile.skill_level
        
        if current_level not in self.progression_rules:
            return {
                "current_level": current_level,
                "can_progress": False,
                "next_level": None,
                "progress": 0,
                "required": 0
            }
        
        rule = self.progression_rules[current_level]
        
        if rule["next_level"] is None:
            return {
                "current_level": current_level,
                "can_progress": False,
                "next_level": None,
                "progress": "Max level reached",
                "required": None
            }
        
        # Count completed courses at required difficulty
        completed_at_difficulty = db.query(
            func.count(CompletedCourse.id)
        ).join(
            Course, CompletedCourse.course_id == Course.id
        ).filter(
            CompletedCourse.user_id == user_id,
            Course.difficulty == rule["required_difficulty"]
        ).scalar()
        
        # If user has career_path, check at least one completed course matches that career
        user_career = user.profile.career_path if user.profile and user.profile.career_path else None
        career_requirement_met = True
        completed_with_career = 0
        if user_career:
            completed_with_career = db.query(
                func.count(CompletedCourse.id)
            ).join(
                Course, CompletedCourse.course_id == Course.id
            ).filter(
                CompletedCourse.user_id == user_id,
                Course.difficulty == rule["required_difficulty"],
                Course.career_path == user_career
            ).scalar()
            career_requirement_met = (completed_with_career >= 1)
        
        required = rule["required_courses"]
        progress = min(completed_at_difficulty, required)
        can_progress = (completed_at_difficulty >= required) and career_requirement_met
        
        return {
            "current_level": current_level,
            "next_level": rule["next_level"],
            "can_progress": can_progress,
            "progress": progress,
            "required": required,
            "required_difficulty": rule["required_difficulty"],
            "career_requirement_met": career_requirement_met,
            "completed_with_career": int(completed_with_career),
            "career_path": user.profile.career_path if user.profile and user.profile.career_path else None
        }
    
    def get_progression_history(
        self,
        user_id: int,
        db: Session
    ) -> list:
        """
        Get user's skill progression history
        
        Args:
            user_id: User ID
            db: Database session
            
        Returns:
            List of progression events
        """
        progressions = db.query(SkillProgression).filter(
            SkillProgression.user_id == user_id
        ).order_by(SkillProgression.created_at.desc()).all()
        
        return [
            {
                "from": p.previous_level,
                "to": p.new_level,
                "reason": p.reason,
                "date": p.created_at.isoformat() if p.created_at else None
            }
            for p in progressions
        ]


_skill_progression_service = None

def get_skill_progression_service() -> SkillProgressionService:
    """Get or create the global skill progression service instance"""
    global _skill_progression_service
    if _skill_progression_service is None:
        _skill_progression_service = SkillProgressionService()
    return _skill_progression_service
