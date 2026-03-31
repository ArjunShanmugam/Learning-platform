"""
Prediction script for generating recommendations.
Uses pre-trained similarity matrix to generate item recommendations.
"""
import os
import json
import logging
import numpy as np
from pathlib import Path
from typing import List, Dict, Optional
import redis
import time

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RecommendationEngine:
    def __init__(self, model_dir: str, redis_host: str = 'localhost', redis_port: int = 6379):
        """
        Initialize the recommendation engine.
        
        Args:
            model_dir: Directory containing model artifacts
            redis_host: Redis server host
            redis_port: Redis server port
        """
        self.model_dir = Path(model_dir)
        self.redis = redis.Redis(host=redis_host, port=redis_port, db=0, decode_responses=True)
        self.similarity_matrix = None
        self.mappings = None
        self.model_version = self.model_dir.name  # Use directory name as version
        self._load_model()

    def _load_model(self) -> None:
        """Load the trained model and mappings."""
        try:
            # Load similarity matrix
            self.similarity_matrix = np.load(self.model_dir / 'item_similarity.npy')
            
            # Load mappings
            with open(self.model_dir / 'mappings.json', 'r') as f:
                self.mappings = json.load(f)
                
            logger.info("✅ Model loaded successfully")
            
        except Exception as e:
            logger.error(f"❌ Error loading model: {str(e)}")
            raise

    def _get_user_history(self, user_id: int) -> List[int]:
        """
        Get user's interaction history.
        In a real system, this would come from your database.
        For now, we'll use a simple in-memory dictionary.
        """
        # This is a placeholder - replace with actual database query
        user_history = {
            1: [1, 2],  # User 1 has interacted with items 1 and 2
            2: [1, 3],
            3: [2, 3]
        }
        return user_history.get(user_id, [])

    def _get_cached_recommendations(self, user_id: int) -> Optional[List[int]]:
        """Get cached recommendations from Redis if available."""
        try:
            cached = self.redis.get(f"recs:{user_id}")
            if cached:
                return json.loads(cached)
            return None
        except redis.RedisError as e:
            logger.warning(f"Redis error: {str(e)}")
            return None

    def _cache_recommendations(self, user_id: int, recommendations: List[int], ttl: int = 3600) -> None:
        """Cache recommendations in Redis."""
        try:
            self.redis.setex(
                f"recs:{user_id}",
                ttl,
                json.dumps(recommendations)
            )
        except redis.RedisError as e:
            logger.warning(f"Failed to cache recommendations: {str(e)}")

    def get_recommendations(self, user_id: int, top_n: int = 5) -> List[int]:
        """
        Get top N item recommendations for a user.
        
        Args:
            user_id: ID of the user
            top_n: Number of recommendations to return
            
        Returns:
            List of recommended item IDs
        """
        start_time = time.time()
        
        # Check cache first
        if cached_recs := self._get_cached_recommendations(user_id):
            logger.info(f"Returning cached recommendations for user {user_id}")
            return cached_recs[:top_n]

        try:
            # Get user's interaction history
            user_history = self._get_user_history(user_id)
            if not user_history:
                logger.warning(f"No history found for user {user_id}")
                return []

            # Convert item IDs to indices
            item_indices = [
                self.mappings['item_mapping'].get(str(item_id))
                for item_id in user_history
                if str(item_id) in self.mappings['item_mapping']
            ]

            if not item_indices:
                logger.warning(f"No valid items found in user {user_id}'s history")
                return []

            # Calculate recommendation scores
            scores = np.sum(self.similarity_matrix[item_indices], axis=0)
            
            # Get top N items (excluding already interacted items)
            recommended_indices = np.argsort(scores)[::-1]
            recommended_items = []
            
            for idx in recommended_indices:
                item_id = self.mappings['item_reverse_mapping'].get(str(idx))
                if item_id and item_id not in user_history:
                    recommended_items.append(item_id)
                    if len(recommended_items) >= top_n:
                        break

            # Cache the results
            self._cache_recommendations(user_id, recommended_items)
            
            logger.info(
                f"Generated {len(recommended_items)} recommendations "
                f"for user {user_id} in {time.time() - start_time:.2f}s"
            )
            
            return recommended_items[:top_n]

        except Exception as e:
            logger.error(f"Error generating recommendations: {str(e)}")
            return []

def main():
    """Example usage of the recommendation engine."""
    try:
        # Get the latest model directory
        models_dir = Path(__file__).parent.parent.parent / 'data' / 'models'
        model_dirs = sorted(models_dir.glob('*'), key=os.path.getmtime, reverse=True)
        
        if not model_dirs:
            raise FileNotFoundError("No trained models found")
            
        latest_model = model_dirs[0]
        logger.info(f"Using model from: {latest_model}")
        
        # Initialize recommendation engine
        recommender = RecommendationEngine(
            model_dir=latest_model,
            redis_host='localhost',
            redis_port=6379
        )
        
        # Example: Get recommendations for user 1
        user_id = 1
        recommendations = recommender.get_recommendations(user_id, top_n=3)
        logger.info(f"Top 3 recommendations for user {user_id}: {recommendations}")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == "__main__":
    main()