"""
Weaviate Vector Database Integration Module
"""
import weaviate
from weaviate.util import generate_uuid5
import numpy as np
import logging
import os
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)


class WeaviateClient:
    """Wrapper around Weaviate client for semantic search."""
    
    def __init__(self, url: str = None):
        """Initialize Weaviate client."""
        self.url = url or os.getenv('WEAVIATE_URL', 'http://localhost:8080')
        try:
            self.client = weaviate.Client(self.url)
            logger.info(f"Connected to Weaviate at {self.url}")
        except Exception as e:
            logger.error(f"Failed to connect to Weaviate: {str(e)}")
            self.client = None
    
    def ensure_schema(self):
        """Ensure required schema exists in Weaviate."""
        try:
            # Create Course class
            course_schema = {
                "class": "Course",
                "properties": [
                    {
                        "name": "title",
                        "dataType": ["text"],
                        "description": "Course title"
                    },
                    {
                        "name": "description",
                        "dataType": ["text"],
                        "description": "Course description"
                    },
                    {
                        "name": "difficulty",
                        "dataType": ["text"],
                        "description": "Difficulty level"
                    },
                    {
                        "name": "careerPath",
                        "dataType": ["text"],
                        "description": "Career path"
                    },
                    {
                        "name": "courseId",
                        "dataType": ["int"],
                        "description": "Reference to course ID"
                    },
                    {
                        "name": "instructor",
                        "dataType": ["text"],
                        "description": "Instructor name"
                    }
                ],
                "vectorizer": "text2vec-transformers",
                "vectorIndexConfig": {
                    "distance": "cosine"
                }
            }
            
            if not self.client.schema.exists("Course"):
                self.client.schema.create_class(course_schema)
                logger.info("Created Course schema in Weaviate")
            else:
                logger.info("Course schema already exists")
            
            # Create UserProfile class
            profile_schema = {
                "class": "UserProfile",
                "properties": [
                    {
                        "name": "fullName",
                        "dataType": ["text"],
                        "description": "User full name"
                    },
                    {
                        "name": "bio",
                        "dataType": ["text"],
                        "description": "User bio"
                    },
                    {
                        "name": "skills",
                        "dataType": ["text[]"],
                        "description": "User skills"
                    },
                    {
                        "name": "userId",
                        "dataType": ["int"],
                        "description": "Reference to user ID"
                    }
                ],
                "vectorizer": "text2vec-transformers",
                "vectorIndexConfig": {
                    "distance": "cosine"
                }
            }
            
            if not self.client.schema.exists("UserProfile"):
                self.client.schema.create_class(profile_schema)
                logger.info("Created UserProfile schema in Weaviate")
            else:
                logger.info("UserProfile schema already exists")
            
            return True
        
        except Exception as e:
            logger.error(f"Failed to ensure schema: {str(e)}")
            return False
    
    def add_course(self, course_id: int, title: str, description: str, 
                   difficulty: str, career_path: str, instructor: str) -> bool:
        """Add course to Weaviate."""
        try:
            data = {
                "title": title,
                "description": description,
                "difficulty": difficulty,
                "careerPath": career_path,
                "courseId": course_id,
                "instructor": instructor
            }
            
            uuid = generate_uuid5(f"course-{course_id}")
            self.client.data_object.create(
                data_object=data,
                class_name="Course",
                uuid=uuid
            )
            
            logger.info(f"Added course {course_id} to Weaviate")
            return True
        
        except Exception as e:
            logger.error(f"Failed to add course: {str(e)}")
            return False
    
    def update_course(self, course_id: int, title: str, description: str,
                      difficulty: str, career_path: str, instructor: str) -> bool:
        """Update course in Weaviate."""
        try:
            data = {
                "title": title,
                "description": description,
                "difficulty": difficulty,
                "careerPath": career_path,
                "courseId": course_id,
                "instructor": instructor
            }
            
            uuid = generate_uuid5(f"course-{course_id}")
            self.client.data_object.update(
                data_object=data,
                class_name="Course",
                uuid=uuid
            )
            
            logger.info(f"Updated course {course_id} in Weaviate")
            return True
        
        except Exception as e:
            logger.error(f"Failed to update course: {str(e)}")
            return False
    
    def delete_course(self, course_id: int) -> bool:
        """Delete course from Weaviate."""
        try:
            uuid = generate_uuid5(f"course-{course_id}")
            self.client.data_object.delete(
                uuid=uuid,
                class_name="Course"
            )
            
            logger.info(f"Deleted course {course_id} from Weaviate")
            return True
        
        except Exception as e:
            logger.error(f"Failed to delete course: {str(e)}")
            return False
    
    def search_courses(self, query: str, limit: int = 10, 
                      certainty: float = 0.5) -> List[Dict]:
        """
        Semantic search for courses.
        """
        try:
            response = self.client.query.get("Course", ["title", "description", "difficulty", 
                                                         "careerPath", "courseId", "instructor"]) \
                .with_near_text({"concepts": [query], "certainty": certainty}) \
                .with_limit(limit) \
                .do()
            
            results = response.get("data", {}).get("Get", {}).get("Course", [])
            logger.info(f"Found {len(results)} courses for query: {query}")
            return results
        
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            return []
    
    def add_profile(self, user_id: int, full_name: str, bio: str, skills: List[str]) -> bool:
        """Add user profile to Weaviate."""
        try:
            data = {
                "fullName": full_name,
                "bio": bio,
                "skills": skills,
                "userId": user_id
            }
            
            uuid = generate_uuid5(f"profile-{user_id}")
            self.client.data_object.create(
                data_object=data,
                class_name="UserProfile",
                uuid=uuid
            )
            
            logger.info(f"Added profile {user_id} to Weaviate")
            return True
        
        except Exception as e:
            logger.error(f"Failed to add profile: {str(e)}")
            return False
    
    def search_profiles(self, query: str, limit: int = 10,
                       certainty: float = 0.5) -> List[Dict]:
        """Semantic search for user profiles."""
        try:
            response = self.client.query.get("UserProfile", ["fullName", "bio", "skills", 
                                                              "userId"]) \
                .with_near_text({"concepts": [query], "certainty": certainty}) \
                .with_limit(limit) \
                .do()
            
            results = response.get("data", {}).get("Get", {}).get("UserProfile", [])
            logger.info(f"Found {len(results)} profiles for query: {query}")
            return results
        
        except Exception as e:
            logger.error(f"Profile search failed: {str(e)}")
            return []
    
    def batch_add_courses(self, courses: List[Dict]) -> bool:
        """Add multiple courses in batch."""
        try:
            client_batch = weaviate.util.BadRequestError
            
            with self.client.batch(
                batch_size=100,
                dynamic=True,
                timeout_retries=3
            ) as batch:
                for course in courses:
                    data = {
                        "title": course.get("title"),
                        "description": course.get("description"),
                        "difficulty": course.get("difficulty"),
                        "careerPath": course.get("career_path"),
                        "courseId": course.get("id"),
                        "instructor": course.get("instructor")
                    }
                    
                    uuid = generate_uuid5(f"course-{course.get('id')}")
                    batch.add_data_object(
                        data_object=data,
                        class_name="Course",
                        uuid=uuid
                    )
            
            logger.info(f"Batch added {len(courses)} courses to Weaviate")
            return True
        
        except Exception as e:
            logger.error(f"Batch add failed: {str(e)}")
            return False
    
    def get_all_courses(self, limit: int = None) -> List[Dict]:
        """Retrieve all courses from Weaviate."""
        try:
            query = self.client.query.get("Course", 
                                         ["title", "description", "difficulty", 
                                          "careerPath", "courseId", "instructor"])
            
            if limit:
                query = query.with_limit(limit)
            
            response = query.do()
            results = response.get("data", {}).get("Get", {}).get("Course", [])
            
            logger.info(f"Retrieved {len(results)} courses from Weaviate")
            return results
        
        except Exception as e:
            logger.error(f"Failed to retrieve courses: {str(e)}")
            return []
    
    def health_check(self) -> bool:
        """Check Weaviate server health."""
        try:
            return self.client.is_ready()
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False


_weaviate_client = None


def get_weaviate_client() -> Optional[WeaviateClient]:
    """Get or create Weaviate client instance."""
    global _weaviate_client
    
    if _weaviate_client is None:
        _weaviate_client = WeaviateClient()
        _weaviate_client.ensure_schema()
    
    return _weaviate_client
