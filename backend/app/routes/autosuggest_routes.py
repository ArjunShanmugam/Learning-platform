"""
Autosuggest API Routes
Provides query suggestions and course recommendations in real-time
"""

from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from typing import List
from pydantic import BaseModel
import sys
from pathlib import Path

from ..database import get_db
from ..models import Course

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

try:
    from ml.query_expansion import Autosuggest, QueryExpander
except ImportError:
    # Fallback if ml module not available
    class Autosuggest:
        def get_suggestions(self, q, limit=5):
            return []
        def get_course_suggestions(self, q, limit=5):
            return []
    
    class QueryExpander:
        def expand_query(self, q):
            return [q]

# Initialize global autosuggest instance
_autosuggest_instance = None
_query_expander_instance = None

def get_autosuggest():
    global _autosuggest_instance
    if _autosuggest_instance is None:
        _autosuggest_instance = Autosuggest()
    return _autosuggest_instance

def get_query_expander():
    global _query_expander_instance
    if _query_expander_instance is None:
        _query_expander_instance = QueryExpander()
    return _query_expander_instance

router = APIRouter(prefix="/autosuggest", tags=["autosuggest"])

class SuggestionResponse(BaseModel):
    suggestions: List[str]

class CoursesuggestionResponse(BaseModel):
    title: str
    difficulty: str
    career_path: str
    relevance: float

@router.get("/query", response_model=SuggestionResponse)
def get_query_suggestions(
    q: str = Query(..., min_length=1, max_length=100),
    limit: int = Query(5, ge=1, le=10)
):
    """
    Get autocomplete query suggestions
    
    Args:
        q: Partial query string
        limit: Maximum suggestions (default 5)
    
    Returns:
        List of suggested queries
    """
    autosuggest = get_autosuggest()
    suggestions = autosuggest.get_suggestions(q, limit=limit)
    return {"suggestions": suggestions}


@router.get("/courses", response_model=List[CoursesuggestionResponse])
def get_course_suggestions(
    q: str = Query(..., min_length=1, max_length=100),
    limit: int = Query(5, ge=1, le=10)
):
    """
    Get course suggestions based on query
    
    Args:
        q: Search query
        limit: Maximum courses (default 5)
    
    Returns:
        List of suggested courses with relevance scores
    """
    autosuggest = get_autosuggest()
    suggestions = autosuggest.get_course_suggestions(q, limit=limit)
    return suggestions


@router.post("/initialize")
def initialize_autosuggest(db: Session = Depends(get_db)):
    """
    Initialize autosuggest with all courses
    Call this on application startup
    """
    courses = db.query(Course).all()
    course_data = [
        {
            'id': c.id,
            'title': c.title,
            'difficulty': c.difficulty or 'Beginner',
            'career_path': c.career_path or 'General'
        }
        for c in courses
    ]
    
    set_autosuggest_courses(course_data)
    
    return {
        "status": "initialized",
        "courses_indexed": len(course_data)
    }


@router.get("/expanded-queries")
def get_expanded_queries(
    q: str = Query(..., min_length=1, max_length=100),
    num_expansions: int = Query(3, ge=1, le=5)
):
    """
    Get query expansions for better search coverage
    
    Args:
        q: Original query
        num_expansions: Number of expansion terms (default 3)
    
    Returns:
        List of expanded queries including original
    """
    expander = get_query_expander()
    expansions = expander.expand_query(q, num_expansions=num_expansions)
    
    return {
        "original_query": q,
        "expanded_queries": expansions,
        "total": len(expansions)
    }
