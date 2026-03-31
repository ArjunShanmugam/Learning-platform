from .user import User
from .user_profile import UserProfile
from .course import Course, CompletedCourse
from .logs import SearchLog, ClickLog
from .embeddings import CourseEmbedding
from .interaction import UserInteraction
from .skill import SkillProgression
from .recommendation_models import RecommendationLog, RecommendationFeedbackLog

__all__ = [
    'User', 
    'UserProfile',
    'Course', 
    'CompletedCourse',
    'SearchLog',
    'ClickLog',
    'CourseEmbedding',
    'UserInteraction',
    'SkillProgression',
    'RecommendationLog',
    'RecommendationFeedbackLog'
]