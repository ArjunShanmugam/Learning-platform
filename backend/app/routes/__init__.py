"""
Routes package for Learning Platform
Contains all API endpoints for authentication, courses, search, recommendations, and skill progression
"""

from . import auth_routes
from . import course_routes
from . import log_routes
from . import recommend_routes
from . import search_routes
from . import skill_routes
from . import autosuggest_routes
from . import admin_routes

__all__ = [
    'auth_routes',
    'course_routes',
    'log_routes',
    'recommend_routes',
    'search_routes',
    'skill_routes',
    'autosuggest_routes',
    'admin_routes',
    'autosuggest_routes',
]
