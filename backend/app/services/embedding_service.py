"""
Embedding Service for Phase 2 - Semantic Search
Computes and manages course embeddings using SentenceTransformer
"""

import numpy as np
from sentence_transformers import SentenceTransformer
import faiss
from typing import List, Tuple
import json
import os
from pathlib import Path

class EmbeddingService:
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        """Initialize the embedding service with a pre-trained model"""
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        self.index = None
        self.course_ids = []
        self.index_path = Path("ml/models/faiss_index.index")
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        
        # FIX #3: Load FAISS index from disk if available
        self._load_index_on_startup()
    
    def _load_index_on_startup(self):
        """Try to load FAISS index from disk on startup"""
        try:
            if self.index_path.exists():
                self.load_index(str(self.index_path))
                print(f"✓ Loaded FAISS index from disk ({len(self.course_ids)} courses)")
        except Exception as e:
            print(f"⚠️ Could not load FAISS index: {e}")
        
    def compute_embedding(self, text: str) -> List[float]:
        """
        Compute embedding for a single text (course title + description)
        
        Args:
            text: Combined course title and description
            
        Returns:
            List of floats representing the embedding vector
        """
        embedding = self.model.encode(text, convert_to_numpy=True)
        return embedding.tolist()
    
    def compute_course_embedding(self, title: str, description: str, tags: str = "") -> List[float]:
        """
        Compute embedding for a course using title, description, and tags
        
        Args:
            title: Course title
            description: Course description
            tags: Comma-separated tags (optional)
            
        Returns:
            List of floats representing the embedding vector
        """
        # Combine all text fields for richer context
        combined_text = f"{title} {description} {tags}".strip()
        return self.compute_embedding(combined_text)
    
    def build_faiss_index(self, embeddings: List[List[float]], course_ids: List[int]) -> None:
        """
        Build FAISS index from embeddings for fast similarity search
        
        Args:
            embeddings: List of embedding vectors
            course_ids: Corresponding course IDs
        """
        if not embeddings:
            return
        
        # Convert to numpy array
        embeddings_array = np.array(embeddings, dtype=np.float32)
        
        # Create FAISS index
        self.index = faiss.IndexFlatL2(self.embedding_dim)
        self.index.add(embeddings_array)
        self.course_ids = course_ids
    
    def search_similar(self, query_embedding: List[float], k: int = 10) -> List[Tuple[int, float]]:
        """
        Search for k most similar courses using FAISS
        
        Args:
            query_embedding: Embedding vector of the search query
            k: Number of results to return
            
        Returns:
            List of tuples (course_id, similarity_score)
        """
        if self.index is None or len(self.course_ids) == 0:
            return []
        
        query_array = np.array([query_embedding], dtype=np.float32)
        distances, indices = self.index.search(query_array, min(k, len(self.course_ids)))
        
        results = []
        for idx, distance in zip(indices[0], distances[0]):
            if idx < len(self.course_ids):
                # Convert L2 distance to similarity score (lower distance = higher similarity)
                similarity = 1 / (1 + distance)
                results.append((self.course_ids[idx], similarity))
        
        return results
    
    def batch_compute_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Compute embeddings for multiple texts efficiently
        
        Args:
            texts: List of text strings
            
        Returns:
            List of embedding vectors
        """
        embeddings = self.model.encode(texts, convert_to_numpy=True)
        return embeddings.tolist()
    
    def save_index(self, filepath: str) -> None:
        """Save FAISS index to disk"""
        try:
            if self.index is not None:
                faiss.write_index(self.index, filepath)
                # Save course IDs separately
                ids_filepath = filepath.replace('.index', '_ids.json')
                with open(ids_filepath, 'w') as f:
                    json.dump(self.course_ids, f)
                print(f"✓ Saved FAISS index to {filepath}")
        except Exception as e:
            print(f"✗ Failed to save FAISS index: {e}")
    
    def load_index(self, filepath: str) -> None:
        """Load FAISS index from disk"""
        try:
            if os.path.exists(filepath):
                self.index = faiss.read_index(filepath)
                ids_filepath = filepath.replace('.index', '_ids.json')
                if os.path.exists(ids_filepath):
                    with open(ids_filepath, 'r') as f:
                        self.course_ids = json.load(f)
                print(f"✓ Loaded FAISS index ({len(self.course_ids)} courses)")
        except Exception as e:
            print(f"✗ Failed to load FAISS index: {e}")


_embedding_service = None

def get_embedding_service() -> EmbeddingService:
    """Get or create the global embedding service instance"""
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = EmbeddingService()
    return _embedding_service
