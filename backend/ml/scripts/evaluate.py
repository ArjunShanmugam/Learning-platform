"""
Evaluation script for recommendation models.
"""
import numpy as np
from typing import List, Dict, Any
from sklearn.metrics import ndcg_score

def calculate_recall_at_k(y_true: List[int], y_pred: List[int], k: int = 10) -> float:
    """
    Calculate Recall@K metric.
    
    Args:
        y_true: List of true item IDs
        y_pred: List of predicted item IDs
        k: Number of top predictions to consider
        
    Returns:
        Recall@K score
    """
    y_true = set(y_true)
    y_pred = y_pred[:k]
    hits = len(y_true.intersection(y_pred))
    return hits / min(k, len(y_true))

def calculate_ndcg(y_true: List[int], y_scores: List[float], k: int = 10) -> float:
    """
    Calculate NDCG@K metric.
    
    Args:
        y_true: List of true item IDs
        y_scores: List of predicted scores for each item
        k: Number of top predictions to consider
        
    Returns:
        NDCG@K score
    """
    # Convert to binary relevance
    y_true_binary = [1 if x in y_true else 0 for x in range(len(y_scores))]
    return ndcg_score([y_true_binary], [y_scores], k=k)

def evaluate_recommendations(
    test_data: List[Dict[str, Any]],
    predictions: List[List[int]],
    k_values: List[int] = [5, 10, 20]
) -> Dict[str, float]:
    """
    Evaluate recommendation quality using multiple metrics.
    
    Args:
        test_data: List of test interactions
        predictions: List of predicted item IDs for each user
        k_values: List of K values to evaluate at
        
    Returns:
        Dictionary of evaluation metrics
    """
    metrics = {}
    
    for k in k_values:
        recalls = []
        ndcgs = []
        
        for i, (true_items, pred_items) in enumerate(zip(test_data, predictions)):
            # Calculate metrics for this user
            true_set = set(true_items['item_ids'])
            pred_rank = [p for p in pred_items if p in true_set][:k]
            
            # Skip users with no relevant items
            if not true_set:
                continue
                
            # Calculate recall@k
            recall = len(pred_rank) / min(k, len(true_set))
            recalls.append(recall)
            
            # Calculate NDCG@k
            y_scores = [1 if i in pred_items[:k] else 0 for i in range(len(pred_items))]
            ndcg = ndcg_score([list(true_set)], [y_scores], k=k)
            ndcgs.append(ndcg)
        
        # Store average metrics
        if recalls:
            metrics[f'recall@{k}'] = np.mean(recalls)
            metrics[f'ndcg@{k}'] = np.mean(ndcgs)
    
    return metrics
