"""
Query Expansion & Autosuggest
Provides intelligent query suggestions and expansion for better search results
"""

from typing import List, Dict
import numpy as np
from sentence_transformers import SentenceTransformer, util
from functools import lru_cache
import json
from pathlib import Path

class QueryExpander:
    """Expands queries using semantic similarity and synonyms"""
    
    def __init__(self, model_name="all-MiniLM-L6-v2"):
        self.model = SentenceTransformer(model_name)
        self.synonyms = self._load_synonyms()
        self.cache_dir = Path("ml/cache")
        self.cache_dir.mkdir(exist_ok=True)
    
    def _load_synonyms(self) -> Dict[str, List[str]]:
        """Load synonym mappings for common terms"""
        return {
            "python": ["python3", "py", "programming"],
            "javascript": ["js", "node", "web dev"],
            "machine learning": ["ml", "deep learning", "ai"],
            "data science": ["data analysis", "analytics", "big data"],
            "web development": ["web dev", "frontend", "backend"],
            "react": ["reactjs", "react.js"],
            "django": ["django framework"],
            "fastapi": ["fast api"],
            "database": ["sql", "nosql", "postgres", "mongodb"],
            "cloud": ["aws", "azure", "gcp", "kubernetes"],
            "api": ["rest", "graphql", "endpoint"],
        }
    
    def expand_query(self, query: str, num_expansions: int = 3) -> List[str]:
        """
        Expand a query with related terms
        
        Args:
            query: Original search query
            num_expansions: Number of expansion terms to generate
        
        Returns:
            List of expanded queries
        """
        expanded = [query]
        
        # Add synonyms
        query_lower = query.lower()
        for key, syns in self.synonyms.items():
            if key in query_lower:
                expanded.extend(syns[:num_expansions])
                break
        
        # Add semantic expansions
        semantic_expansions = self._get_semantic_expansions(query, num_expansions)
        expanded.extend(semantic_expansions)
        
        # Remove duplicates and return top N
        expanded = list(set(expanded))[:num_expansions + 1]
        return expanded
    
    def _get_semantic_expansions(self, query: str, num: int = 2) -> List[str]:
        """Get semantically similar terms"""
        # This would use embeddings in a production system
        # For now, return common expansions
        query_lower = query.lower()
        
        expansions_map = {
            "beginner": ["introductory", "starter", "fundamentals"],
            "advanced": ["expert", "professional", "master"],
            "web": ["website", "frontend", "application"],
            "mobile": ["app", "ios", "android"],
            "project": ["capstone", "portfolio", "hands-on"],
        }
        
        for key, expansions in expansions_map.items():
            if key in query_lower:
                return expansions[:num]
        
        return []


class Autosuggest:
    """Generates autocomplete suggestions as user types"""
    
    def __init__(self):
        self.expander = QueryExpander()
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        self.suggestion_cache = {}
        self.all_courses = None
        self.course_embeddings = None
    
    def set_courses(self, courses: List[Dict]):
        """Cache courses and their embeddings for fast suggestions"""
        self.all_courses = courses
        
        # Generate embeddings for all course titles
        titles = [c['title'] for c in courses]
        self.course_embeddings = self.model.encode(titles, convert_to_tensor=True)
        print(f"✓ Cached embeddings for {len(courses)} courses")
    
    def get_suggestions(self, partial_query: str, limit: int = 5) -> List[str]:
        """
        Get autocomplete suggestions for partial query
        
        Args:
            partial_query: User's partial search input
            limit: Max suggestions to return
        
        Returns:
            List of suggested queries
        """
        if not partial_query or len(partial_query) < 1:
            return []
        
        # Check cache
        cache_key = f"{partial_query}_{limit}"
        if cache_key in self.suggestion_cache:
            return self.suggestion_cache[cache_key]
        
        suggestions = []
        partial_lower = partial_query.lower()
        
        # Direct matches (prefix matching)
        if self.all_courses:
            for course in self.all_courses:
                title = course['title'].lower()
                if title.startswith(partial_lower):
                    suggestions.append(course['title'])
                if len(suggestions) >= limit:
                    break
        
        # Add common expansions if direct matches are few
        if len(suggestions) < limit:
            expansions = self.expander.expand_query(partial_query, num_expansions=3)
            suggestions.extend(expansions)
        
        # Deduplicate and limit
        suggestions = list(dict.fromkeys(suggestions))[:limit]
        
        # Cache
        self.suggestion_cache[cache_key] = suggestions
        
        return suggestions
    
    def get_course_suggestions(self, query: str, limit: int = 5) -> List[Dict]:
        """
        Get course suggestions based on query
        
        Args:
            query: Search query
            limit: Max courses to return
        
        Returns:
            List of suggested courses with relevance scores
        """
        if not self.all_courses or not self.course_embeddings:
            return []
        
        # Encode query
        query_embedding = self.model.encode(query, convert_to_tensor=True)
        
        # Calculate similarities
        similarities = util.pytorch_cos_sim(query_embedding, self.course_embeddings)[0]
        
        # Get top matches
        top_indices = similarities.argsort(descending=True)[:limit]
        
        suggestions = []
        for idx in top_indices:
            course = self.all_courses[int(idx)]
            suggestions.append({
                'title': course['title'],
                'difficulty': course.get('difficulty', 'Beginner'),
                'career_path': course.get('career_path', 'General'),
                'relevance': float(similarities[int(idx)])
            })
        
        return suggestions


# Global instances
_query_expander = None
_autosuggest = None

def get_query_expander() -> QueryExpander:
    """Get or create QueryExpander instance"""
    global _query_expander
    if _query_expander is None:
        _query_expander = QueryExpander()
    return _query_expander

def get_autosuggest() -> Autosuggest:
    """Get or create Autosuggest instance"""
    global _autosuggest
    if _autosuggest is None:
        _autosuggest = Autosuggest()
    return _autosuggest

def set_autosuggest_courses(courses: List[Dict]):
    """Initialize autosuggest with courses"""
    autosuggest = get_autosuggest()
    autosuggest.set_courses(courses)


if __name__ == "__main__":
    print("Query Expansion & Autosuggest modules loaded")
