"""
Services package for Learning Platform
Contains business logic for embeddings, recommendations, and skill progression
"""

from .embedding_service import EmbeddingService, get_embedding_service
from .recommender_service import RecommenderService, get_recommender_service
from .skill_progression_service import SkillProgressionService, get_skill_progression_service

__all__ = [
    'EmbeddingService',
    'get_embedding_service',
    'RecommenderService',
    'get_recommender_service',
    'SkillProgressionService',
    'get_skill_progression_service',
]
