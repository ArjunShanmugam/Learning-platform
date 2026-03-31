"""
Advanced Ranking Model using LightGBM
Ranks courses based on user features, course features, and interaction history
"""

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import ndcg_score, mean_reciprocal_rank
import joblib
import os
from datetime import datetime
from pathlib import Path

class AdvancedRankingModel:
    """LightGBM-based ranking model for course recommendations"""
    
    def __init__(self, model_dir="ml/models"):
        self.model_dir = Path(model_dir)
        self.model_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.feature_names = None
        self.model_version = None
        
    def extract_features(self, user_data, course_data, interaction_data):
        """
        Extract features for ranking model
        
        User features: level, total_completed, avg_rating
        Course features: difficulty, popularity, avg_duration
        Interaction features: clicks, time_spent, completion_rate
        
        Returns:
            X (features), y (labels for LTR training)
        """
        features = []
        labels = []
        
        for idx, course in course_data.iterrows():
            course_id = course['id']
            
            # User features
            user_level = self._encode_user_level(user_data.get('level', 'beginner'))
            user_completed = user_data.get('courses_completed', 0)
            user_avg_rating = user_data.get('avg_rating', 0)
            
            # Course features
            course_difficulty = self._encode_difficulty(course.get('difficulty', 'Beginner'))
            course_career_path = self._encode_career_path(course.get('career_path', 'General'))
            course_duration = course.get('duration', 0) / 100  # Normalize
            
            # Interaction features (from interaction_data)
            interaction = interaction_data[interaction_data['course_id'] == course_id].iloc[0] if len(interaction_data[interaction_data['course_id'] == course_id]) > 0 else None
            
            clicks = interaction['clicks'] if interaction is not None else 0
            time_spent = interaction['total_time_spent'] if interaction is not None else 0
            completion_rate = interaction['completion_rate'] if interaction is not None else 0
            
            # Relevance label (1 if completed/clicked, 0 otherwise)
            label = 1 if (clicks > 0 or time_spent > 0) else 0
            
            # Combine all features
            feature_vector = [
                user_level,
                user_completed,
                user_avg_rating,
                course_difficulty,
                course_career_path,
                course_duration,
                clicks / max(1, clicks + 1),  # Click ratio
                time_spent / max(1, time_spent + 100),  # Time ratio
                completion_rate,
                (clicks * time_spent) / max(1, clicks + time_spent),  # Interaction strength
                course.get('rating', 0),  # Course rating
                course.get('student_count', 0) / 1000,  # Popularity (normalized)
            ]
            
            features.append(feature_vector)
            labels.append(label)
        
        self.feature_names = [
            'user_level', 'user_completed', 'user_avg_rating',
            'course_difficulty', 'course_career_path', 'course_duration',
            'click_ratio', 'time_ratio', 'completion_rate', 'interaction_strength',
            'course_rating', 'popularity'
        ]
        
        X = np.array(features)
        y = np.array(labels)
        
        return X, y
    
    def _encode_difficulty(self, difficulty):
        """Encode difficulty as numeric"""
        mapping = {'Beginner': 1, 'Intermediate': 2, 'Advanced': 3, 'Expert': 4}
        return mapping.get(difficulty, 1)
    
    def _encode_career_path(self, career_path):
        """Encode career path as numeric"""
        if 'Data' in career_path or 'data' in career_path:
            return 1
        elif 'Web' in career_path or 'web' in career_path:
            return 2
        elif 'Mobile' in career_path or 'mobile' in career_path:
            return 3
        elif 'Cloud' in career_path or 'cloud' in career_path:
            return 4
        else:
            return 0
    
    def _encode_user_level(self, level):
        """Encode user level as numeric"""
        mapping = {'Beginner': 1, 'Intermediate': 2, 'Advanced': 3, 'Expert': 4}
        return mapping.get(level, 1)
    
    def train(self, X, y, test_size=0.2, num_leaves=31, learning_rate=0.05):
        """
        Train LightGBM ranking model
        
        Args:
            X: Feature matrix
            y: Labels (1 for relevant, 0 for non-relevant)
            test_size: Test set fraction
            num_leaves: LightGBM parameter
            learning_rate: LightGBM parameter
        
        Returns:
            metrics: Training metrics
        """
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Create LightGBM dataset
        train_data = lgb.Dataset(
            X_train_scaled,
            label=y_train,
            feature_names=self.feature_names
        )
        
        test_data = lgb.Dataset(
            X_test_scaled,
            label=y_test,
            reference=train_data,
            feature_names=self.feature_names
        )
        
        # Train model
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'num_leaves': num_leaves,
            'learning_rate': learning_rate,
            'verbose': 1,
            'seed': 42
        }
        
        self.model = lgb.train(
            params,
            train_data,
            num_boost_round=100,
            valid_sets=[test_data],
            callbacks=[
                lgb.early_stopping(stopping_rounds=10),
                lgb.log_evaluation(period=10)
            ]
        )
        
        # Evaluate
        y_pred_train = self.model.predict(X_train_scaled)
        y_pred_test = self.model.predict(X_test_scaled)
        
        # Calculate metrics
        train_auc = self._calculate_auc(y_train, y_pred_train)
        test_auc = self._calculate_auc(y_test, y_pred_test)
        
        metrics = {
            'train_auc': train_auc,
            'test_auc': test_auc,
            'feature_importance': self._get_feature_importance(),
            'num_features': X.shape[1],
            'samples': X.shape[0]
        }
        
        return metrics
    
    def _calculate_auc(self, y_true, y_pred):
        """Calculate AUC score"""
        from sklearn.metrics import roc_auc_score
        try:
            return roc_auc_score(y_true, y_pred)
        except:
            return 0.0
    
    def _get_feature_importance(self):
        """Get feature importance from model"""
        importance = self.model.feature_importance(importance_type='gain')
        feature_importance = {
            name: float(score)
            for name, score in zip(self.feature_names, importance)
        }
        # Sort by importance
        return dict(sorted(feature_importance.items(), key=lambda x: x[1], reverse=True))
    
    def predict(self, X):
        """
        Predict ranking scores for courses
        
        Args:
            X: Feature matrix
        
        Returns:
            scores: Ranking scores (0-1)
        """
        if self.model is None:
            raise ValueError("Model not trained yet")
        
        X_scaled = self.scaler.transform(X)
        scores = self.model.predict(X_scaled)
        
        return scores
    
    def save_model(self, version=None):
        """Save model and scaler"""
        if version is None:
            version = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        self.model_version = version
        model_path = self.model_dir / f"lightgbm_ranker_{version}.pkl"
        scaler_path = self.model_dir / f"scaler_{version}.pkl"
        metadata_path = self.model_dir / f"metadata_{version}.json"
        
        joblib.dump(self.model, model_path)
        joblib.dump(self.scaler, scaler_path)
        
        # Save metadata
        import json
        metadata = {
            'version': version,
            'feature_names': self.feature_names,
            'created_at': datetime.now().isoformat()
        }
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"✓ Model saved: {model_path}")
        print(f"✓ Scaler saved: {scaler_path}")
        
        return version
    
    def load_model(self, version):
        """Load model and scaler"""
        model_path = self.model_dir / f"lightgbm_ranker_{version}.pkl"
        scaler_path = self.model_dir / f"scaler_{version}.pkl"
        
        self.model = joblib.load(model_path)
        self.scaler = joblib.load(scaler_path)
        self.model_version = version
        
        print(f"✓ Model loaded: {model_path}")
        return True


# Model management
model_registry = {}

def get_ranking_model(version=None):
    """Get or create ranking model instance"""
    if version is None:
        version = "latest"
    
    if version not in model_registry:
        model = AdvancedRankingModel()
        if version != "latest":
            model.load_model(version)
        model_registry[version] = model
    
    return model_registry[version]


if __name__ == "__main__":
    print("LightGBM Ranking Model ready for training")
    print("Use: from ml.advanced_ranking import get_ranking_model")
