"""
Recommender Service for Phase 2 - Hybrid Recommendations
Combines content-based and collaborative filtering approaches with advanced personalization
"""

from typing import List, Dict, Tuple, Set
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import datetime, timedelta
from ..models import (
    User, UserProfile, Course, CompletedCourse, 
    UserInteraction, CourseEmbedding
)
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

class RecommenderService:
    def __init__(self):
        self.content_weight = 0.6  # Weight for content-based recommendations
        self.collab_weight = 0.4   # Weight for collaborative filtering
    
    def get_completed_courses_info(
        self, 
        user_id: int, 
        db: Session
    ) -> Tuple[List[Course], Set[int], Dict[int, str]]:
        """
        Get user's completed courses and extract their instructors
        
        Returns:
            Tuple of (completed_courses, completed_ids_set, instructor_map)
        """
        completed_course_records = db.query(CompletedCourse).filter(
            CompletedCourse.user_id == user_id
        ).all()
        
        completed_courses = []
        completed_ids = set()
        instructor_map = {}  # course_id -> instructor_id
        
        for cc in completed_course_records:
            course = db.query(Course).filter(Course.id == cc.course_id).first()
            if course:
                completed_courses.append(course)
                completed_ids.add(course.id)
                if course.instructor_id:
                    instructor_map[course.id] = course.instructor_id
        
        return completed_courses, completed_ids, instructor_map
    
    def calculate_prerequisite_match(
        self,
        completed_courses: List[Course],
        candidate_course: Course
    ) -> Tuple[float, str]:
        """
        Calculate if user has skills needed for this course
        
        Args:
            completed_courses: Courses user has completed
            candidate_course: Course to recommend
            
        Returns:
            Tuple of (score_boost, reason)
        """
        if not completed_courses:
            return 0.0, ""
        
        # Extract topics/skills from completed courses
        completed_careers = set()
        for course in completed_courses:
            if course.career_path:
                completed_careers.add(course.career_path.lower())
        
        # Check if candidate course relates to completed skills
        candidate_career = candidate_course.career_path.lower() if candidate_course.career_path else ""
        
        # If candidate course is in same career path as completed courses
        for completed_career in completed_careers:
            if completed_career in candidate_career or candidate_career in completed_career:
                return 0.3, "Builds on your existing skills"
        
        return 0.0, ""
    
    def calculate_progression_score(
        self,
        user_profile: UserProfile,
        candidate_course: Course,
        completed_courses: List[Course]
    ) -> Tuple[float, str]:
        """
        Calculate if course is natural next step in learning journey
        
        Args:
            user_profile: User's profile with skill level
            candidate_course: Course to recommend
            completed_courses: Courses user completed
            
        Returns:
            Tuple of (score_boost, reason)
        """
        if not user_profile.skill_level or not candidate_course.difficulty:
            return 0.0, ""
        
        skill_levels = ["Beginner", "Mid", "Expert"]
        user_idx = skill_levels.index(user_profile.skill_level) if user_profile.skill_level in skill_levels else 0
        candidate_idx = skill_levels.index(candidate_course.difficulty) if candidate_course.difficulty in skill_levels else 0
        
        # User just completed same level → next level is perfect
        if completed_courses:
            last_completed = completed_courses[-1]  # Most recent
            if last_completed.difficulty and last_completed.difficulty.lower() == user_profile.skill_level.lower():
                if candidate_idx == user_idx + 1:
                    return 0.25, "Natural next level after your recent course"
        
        return 0.0, ""
    
    def calculate_topic_similarity(
        self,
        completed_courses: List[Course],
        candidate_course: Course
    ) -> Tuple[float, str]:
        """
        Calculate if course teaches related topics to completed courses
        
        Args:
            completed_courses: Courses user completed
            candidate_course: Course to recommend
            
        Returns:
            Tuple of (score_boost, reason)
        """
        if not completed_courses:
            return 0.0, ""
        
        # Check for similar keywords in titles/descriptions
        candidate_title = (candidate_course.title or "").lower()
        candidate_desc = (candidate_course.description or "").lower()
        
        for completed_course in completed_courses:
            completed_title = (completed_course.title or "").lower()
            completed_desc = (completed_course.description or "").lower()
            
            # Extract programming language/topic from titles
            keywords = ["python", "javascript", "java", "react", "django", "node", 
                       "sql", "database", "web", "api", "algorithm", "data structure"]
            
            for keyword in keywords:
                if keyword in completed_title and keyword in candidate_title:
                    return 0.15, f"Related to {completed_course.title}"
                if keyword in completed_desc and keyword in candidate_desc:
                    return 0.1, "Related topic to your learning path"
        
        return 0.0, ""
    
    def calculate_instructor_bonus(
        self,
        candidate_course: Course,
        instructor_map: Dict[int, str]
    ) -> Tuple[float, str]:
        """
        Calculate bonus if user likes this course's instructor
        
        Args:
            candidate_course: Course to recommend
            instructor_map: Map of completed course_id -> instructor_id
            
        Returns:
            Tuple of (score_boost, reason)
        """
        if not candidate_course.instructor_id or not instructor_map:
            return 0.0, ""
        
        # Check if user completed courses from same instructor
        for completed_course_id, instructor_id in instructor_map.items():
            if instructor_id == candidate_course.instructor_id:
                return 0.2, "From an instructor you've learned from"
        
        return 0.0, ""
    
    def calculate_knowledge_gap(
        self,
        completed_courses: List[Course],
        candidate_course: Course,
        user_profile: UserProfile
    ) -> Tuple[float, str]:
        """
        Calculate if course fills gaps in user's knowledge for their career path
        
        Args:
            completed_courses: Courses user completed
            candidate_course: Course to recommend
            user_profile: User's profile and career path
            
        Returns:
            Tuple of (score_boost, reason)
        """
        if not user_profile.career_path or not candidate_course.career_path:
            return 0.0, ""
        
        user_career = user_profile.career_path.lower()
        candidate_career = candidate_course.career_path.lower()
        
        # If both are in same career path
        if user_career == candidate_career:
            # Get list of completed career paths
            completed_careers = set()
            for course in completed_courses:
                if course.career_path:
                    completed_careers.add(course.career_path.lower())
            
            # If this is a different topic in same career, it fills a gap
            if candidate_career not in completed_careers or len(completed_careers) == 0:
                return 0.25, "Fills a gap in your learning path"
        
        return 0.0, ""
    
    def calculate_time_readiness(
        self,
        user_id: int,
        db: Session
    ) -> Tuple[float, str]:
        """
        Calculate if user is ready for next course (psychological readiness)
        
        Args:
            user_id: User ID
            db: Database session
            
        Returns:
            Tuple of (score_boost, reason)
        """
        # Get last completed course by order
        last_completed = db.query(CompletedCourse).filter(
            CompletedCourse.user_id == user_id
        ).order_by(desc(CompletedCourse.id)).first()
        
        if not last_completed:
            return 0.1, "Time to start learning"
        
        # Small boost to encourage continuation
        return 0.1, "Continue your learning journey"
        
    def get_content_based_score(
        self, 
        user_profile: UserProfile, 
        course: Course,
        course_embedding: CourseEmbedding
    ) -> Tuple[float, list]:
        """
        Calculate content-based score based on user profile and course attributes
        
        Returns a tuple of (score, list_of_reasons)
        """
        score = 0.0
        reasons = []
        
        # Skill level matching (up to 0.45)
        if user_profile.skill_level and course.difficulty:
            skill_levels = ["Beginner", "Mid", "Expert"]
            user_idx = skill_levels.index(user_profile.skill_level) if user_profile.skill_level in skill_levels else 0
            course_idx = skill_levels.index(course.difficulty) if course.difficulty in skill_levels else 0
            skill_gap = abs(course_idx - user_idx)
            
            # Prefer courses at current or next level
            if course_idx == user_idx:
                score += 0.45
                reasons.append("Matches your skill level")
            elif course_idx == user_idx + 1:
                score += 0.4
                reasons.append("Next level challenge")
            elif course_idx == user_idx - 1:
                score += 0.2
                reasons.append("Good for review")
            else:
                # Penalize courses that are two levels away (e.g., Beginner vs Expert)
                score += 0.05
                reasons.append("Different level - may be advanced")
        
        # Career path matching (boost up to 0.35)
        if user_profile.career_path and course.career_path:
            if user_profile.career_path.lower() == course.career_path.lower():
                score += 0.35
                reasons.append("Matches your career path")
            elif user_profile.career_path.lower() in course.career_path.lower():
                score += 0.18
                reasons.append("Related to your career")
        
        # Freshness small bonus
        score += 0.05
        
        # Clamp score
        score = min(score, 1.0)
        return score, reasons
    
    def get_collaborative_score(
        self,
        user_id: int,
        course_id: int,
        db: Session
    ) -> Tuple[float, str]:
        """
        Calculate collaborative filtering score based on user cohort interactions
        
        Args:
            user_id: User ID
            course_id: Course ID
            db: Database session
            
        Returns:
            Tuple of (score, reason_tag)
        """
        # Get user's career path (cohort)
        user = db.query(User).filter(User.id == user_id).first()
        if not user or not user.profile:
            return 0.0, "No cohort data"
        
        user_career = user.profile.career_path
        
        # Count interactions for this course from users in same cohort
        cohort_interactions = db.query(
            func.count(UserInteraction.id).label('count'),
            func.avg(UserInteraction.weight).label('avg_weight')
        ).join(
            User, UserInteraction.user_id == User.id
        ).join(
            UserProfile, User.id == UserProfile.user_id
        ).filter(
            UserInteraction.course_id == course_id,
            UserProfile.career_path == user_career
        ).first()
        
        if not cohort_interactions or cohort_interactions.count == 0:
            return 0.0, "New in your cohort"
        
        # Normalize score (0-1 range)
        # More interactions = higher score
        interaction_count = cohort_interactions.count
        avg_weight = cohort_interactions.avg_weight or 1.0
        
        # Score based on interaction frequency and weight
        score = min(interaction_count / 100.0 * avg_weight, 1.0)
        
        reason = f"Popular in {user_career} cohort"
        return score, reason
    
    def get_hybrid_recommendations(
        self,
        user_id: int,
        db: Session,
        k: int = 10
    ) -> List[Dict]:
        """
        Get hybrid recommendations combining multiple personalized factors:
        1. Content-based (skill level, career path)
        2. Collaborative filtering (user cohort)
        3. Progression (next natural level)
        4. Prerequisite skills
        5. Topic similarity
        6. Instructor preference
        7. Knowledge gaps
        8. Time readiness
        
        Args:
            user_id: User ID
            db: Database session
            k: Number of recommendations to return
            
        Returns:
            List of recommendation dicts with course info and reason
        """
        # Get user profile
        user = db.query(User).filter(User.id == user_id).first()
        if not user or not user.profile:
            return []
        
        user_profile = user.profile
        
        # Get completed courses and instructor info
        completed_courses, completed_ids, instructor_map = self.get_completed_courses_info(user_id, db)
        
        # Get all courses (exclude completed ones)
        all_courses = db.query(Course).filter(
            ~Course.id.in_(completed_ids)
        ).all()
        
        recommendations = []
        all_reasons = []  # Track all reasons for each course
        
        for course in all_courses:
            # Get content-based score
            course_embedding = db.query(CourseEmbedding).filter(
                CourseEmbedding.course_id == course.id
            ).first()
            
            content_score, content_reasons = self.get_content_based_score(
                user_profile, course, course_embedding
            )
            
            # Get collaborative score
            collab_score, collab_reason = self.get_collaborative_score(
                user_id, course.id, db
            )
            
            # Base hybrid score
            hybrid_score = (
                self.content_weight * content_score +
                self.collab_weight * collab_score
            )
            
            # NEW PERSONALIZED FACTORS
            all_course_reasons = content_reasons.copy()
            
            # 1. Prerequisite/Skill Match
            prereq_boost, prereq_reason = self.calculate_prerequisite_match(
                completed_courses, course
            )
            hybrid_score += prereq_boost
            if prereq_reason:
                all_course_reasons.append(prereq_reason)
            
            # 2. Natural Progression
            progression_boost, progression_reason = self.calculate_progression_score(
                user_profile, course, completed_courses
            )
            hybrid_score += progression_boost
            if progression_reason:
                all_course_reasons.append(progression_reason)
            
            # 3. Topic Similarity
            topic_boost, topic_reason = self.calculate_topic_similarity(
                completed_courses, course
            )
            hybrid_score += topic_boost
            if topic_reason:
                all_course_reasons.append(topic_reason)
            
            # 4. Instructor Preference
            instructor_boost, instructor_reason = self.calculate_instructor_bonus(
                course, instructor_map
            )
            hybrid_score += instructor_boost
            if instructor_reason:
                all_course_reasons.append(instructor_reason)
            
            # 5. Knowledge Gap Filling
            gap_boost, gap_reason = self.calculate_knowledge_gap(
                completed_courses, course, user_profile
            )
            hybrid_score += gap_boost
            if gap_reason:
                all_course_reasons.append(gap_reason)
            
            # 6. Time Readiness
            time_boost, time_reason = self.calculate_time_readiness(user_id, db)
            hybrid_score += time_boost
            if time_reason:
                all_course_reasons.append(time_reason)
            
            # Clamp score to 0-1
            hybrid_score = min(hybrid_score, 1.0)
            
            # Build final reason string
            if all_course_reasons:
                reason = " + ".join(all_course_reasons)
            else:
                reason = collab_reason if collab_reason else "Recommended for you"
            
            recommendations.append({
                'course_id': course.id,
                'title': course.title,
                'description': course.description,
                'difficulty': course.difficulty,
                'career_path': course.career_path,
                'score': hybrid_score,
                'reason': reason,
                'content_score': content_score,
                'collab_score': collab_score
            })
        
        # Sort by score and return top k
        recommendations.sort(key=lambda x: x['score'], reverse=True)
        return recommendations[:k]
    
    def log_interaction(
        self,
        user_id: int,
        course_id: int,
        interaction_type: str,
        db: Session
    ) -> None:
        """
        Log user interaction for collaborative filtering
        
        Args:
            user_id: User ID
            course_id: Course ID
            interaction_type: Type of interaction (view, click, start, complete)
            db: Database session
        """
        # Weight interactions differently
        weights = {
            'view': 1.0,
            'click': 1.5,
            'start': 2.0,
            'complete': 5.0
        }
        
        weight = weights.get(interaction_type, 1.0)
        
        # Check if interaction already exists
        existing = db.query(UserInteraction).filter(
            UserInteraction.user_id == user_id,
            UserInteraction.course_id == course_id,
            UserInteraction.interaction_type == interaction_type
        ).first()
        
        if existing:
            # Update weight
            existing.weight = weight
        else:
            # Create new interaction
            interaction = UserInteraction(
                user_id=user_id,
                course_id=course_id,
                interaction_type=interaction_type,
                weight=weight
            )
            db.add(interaction)
        
        db.commit()


_recommender_service = None

def get_recommender_service() -> RecommenderService:
    """Get or create the global recommender service instance"""
    global _recommender_service
    if _recommender_service is None:
        _recommender_service = RecommenderService()
    return _recommender_service
