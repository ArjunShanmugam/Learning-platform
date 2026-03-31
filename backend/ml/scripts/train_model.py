"""
Basic item-based collaborative filtering model training.
Uses simple cosine similarity between items.
"""
import os
import sys
import json
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sklearn.metrics.pairwise import cosine_similarity
from scipy.sparse import csr_matrix

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_cleaned_data(input_path: str) -> pd.DataFrame:
    """Load the cleaned interaction data."""
    logger.info(f"Loading cleaned data from {input_path}")
    return pd.read_parquet(input_path)

def create_user_item_matrix(df: pd.DataFrame) -> tuple:
    """
    Create user-item interaction matrix and mappings.
    Returns: (interaction_matrix, user_mapping, item_mapping)
    """
    logger.info("Creating user-item interaction matrix...")
    
    # Create mappings
    user_mapping = {user: idx for idx, user in enumerate(df['user_id'].unique())}
    item_mapping = {item: idx for idx, item in enumerate(df['item_id'].unique())}
    
    # Create sparse matrix
    rows = df['user_id'].map(user_mapping)
    cols = df['item_id'].map(item_mapping)
    
    # Binary interactions (1 = interaction, 0 = no interaction)
    data = np.ones(len(df))
    
    # Create sparse matrix
    n_users = len(user_mapping)
    n_items = len(item_mapping)
    interaction_matrix = csr_matrix(
        (data, (rows, cols)), 
        shape=(n_users, n_items)
    )
    
    return interaction_matrix, user_mapping, item_mapping

def calculate_item_similarity(interaction_matrix: csr_matrix) -> np.ndarray:
    """Calculate cosine similarity between items."""
    logger.info("Calculating item similarities...")
    
    # Transpose to get items x users matrix
    item_user_matrix = interaction_matrix.T
    
    # Calculate cosine similarity
    similarity_matrix = cosine_similarity(item_user_matrix)
    
    # Fill diagonal with zeros (self-similarity)
    np.fill_diagonal(similarity_matrix, 0)
    
    return similarity_matrix

def save_model_artifacts(
    similarity_matrix: np.ndarray,
    user_mapping: dict,
    item_mapping: dict,
    output_dir: Path
) -> None:
    """Save model artifacts to disk."""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save similarity matrix
    np.save(output_dir / 'item_similarity.npy', similarity_matrix)
    
    # Convert numpy types to native Python types for JSON serialization
    user_mapping = {int(k): int(v) for k, v in user_mapping.items()}
    item_mapping = {int(k): int(v) for k, v in item_mapping.items()}
    
    # Save mappings
    mappings = {
        'user_mapping': user_mapping,
        'item_mapping': item_mapping,
        'item_reverse_mapping': {int(v): int(k) for k, v in item_mapping.items()},
        'created_at': datetime.now().isoformat()
    }
    
    with open(output_dir / 'mappings.json', 'w') as f:
        json.dump(mappings, f, indent=2)
    
    logger.info(f"Model artifacts saved to {output_dir}")

def main():
    """Main training pipeline."""
    try:
        # Set up paths
        data_dir = project_root / 'data'
        input_file = data_dir / 'processed' / 'interactions_cleaned.parquet'
        output_dir = data_dir / 'models' / datetime.now().strftime('%Y%m%d_%H%M%S')
        
        logger.info("Starting model training...")
        logger.info(f"Project root: {project_root}")
        logger.info(f"Input file: {input_file}")
        logger.info(f"Output directory: {output_dir}")
        
        # Load and prepare data
        df = load_cleaned_data(input_file)
        
        # Create interaction matrix
        interaction_matrix, user_mapping, item_mapping = create_user_item_matrix(df)
        
        # Calculate item similarities
        similarity_matrix = calculate_item_similarity(interaction_matrix)
        
        # Save model artifacts
        save_model_artifacts(
            similarity_matrix,
            user_mapping,
            item_mapping,
            output_dir
        )
        
        logger.info("✅ Model training completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error in model training: {str(e)}")
        raise

if __name__ == "__main__":
    main()