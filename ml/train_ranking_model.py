"""
Training Pipeline for Advanced Ranking Model
Orchestrates data collection, feature engineering, and model training
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from app.database import SessionLocal
from app.models import Course, User, ClickLog, CompletedCourse
from ml.advanced_ranking import AdvancedRankingModel

load_dotenv()

class TrainingPipeline:
    """Orchestrates the complete training pipeline"""
    
    def __init__(self):
        self.db = SessionLocal()
        self.model = AdvancedRankingModel()
        self.training_log = {
            'start_time': datetime.now().isoformat(),
            'stages': {}
        }
    
    def run(self):
        """Execute full training pipeline"""
        try:
            print("\n" + "="*60)
            print("  ADVANCED RANKING MODEL - TRAINING PIPELINE")
            print("="*60)
            
            # Stage 1: Collect data
            print("\n[1/4] Collecting training data...")
            users, courses, interactions = self._collect_data()
            self.training_log['stages']['data_collection'] = {
                'users': len(users),
                'courses': len(courses),
                'interactions': len(interactions)
            }
            print(f"✓ Collected {len(users)} users, {len(courses)} courses, {len(interactions)} interactions")
            
            # Stage 2: Feature engineering
            print("\n[2/4] Engineering features...")
            X, y = self._engineer_features(users, courses, interactions)
            self.training_log['stages']['feature_engineering'] = {
                'samples': X.shape[0],
                'features': X.shape[1],
                'positive_samples': int(y.sum())
            }
            print(f"✓ Created {X.shape[0]} samples with {X.shape[1]} features")
            
            # Stage 3: Train model
            print("\n[3/4] Training LightGBM model...")
            metrics = self.model.train(X, y)
            self.training_log['stages']['model_training'] = metrics
            print(f"✓ Training AUC: {metrics['train_auc']:.4f}")
            print(f"✓ Testing AUC: {metrics['test_auc']:.4f}")
            
            # Stage 4: Save model
            print("\n[4/4] Saving model and metadata...")
            version = self.model.save_model()
            self.training_log['stages']['model_saving'] = {'version': version}
            print(f"✓ Model saved as version {version}")
            
            # Print feature importance
            print("\n[Feature Importance - Top 5]")
            importance = metrics['feature_importance']
            for i, (feat, score) in enumerate(list(importance.items())[:5], 1):
                print(f"  {i}. {feat}: {score:.4f}")
            
            self.training_log['status'] = 'success'
            self.training_log['end_time'] = datetime.now().isoformat()
            
            print("\n" + "="*60)
            print("✓ TRAINING COMPLETE")
            print("="*60 + "\n")
            
            return self.training_log
            
        except Exception as e:
            print(f"\n✗ TRAINING FAILED: {str(e)}")
            self.training_log['status'] = 'failed'
            self.training_log['error'] = str(e)
            raise
        finally:
            self.db.close()
    
    def _collect_data(self):
        """Collect training data from database"""
        # Get users
        users = self.db.query(User).all()
        users_data = [{'id': u.id, 'role': u.role} for u in users]
        
        # Get courses
        courses = self.db.query(Course).all()
        courses_data = [
            {
                'id': c.id,
                'title': c.title,
                'difficulty': c.difficulty or 'Beginner',
                'career_path': c.career_path or 'General',
                'duration': c.duration or 0,
                'rating': c.rating or 0,
                'student_count': c.student_count or 0
            }
            for c in courses
        ]
        
        # Get interactions (clicks)
        clicks = self.db.query(ClickLog).all()
        click_counts = {}
        for click in clicks:
            if click.course_id not in click_counts:
                click_counts[click.course_id] = 0
            click_counts[click.course_id] += 1
        
        # Get completions
        completions = self.db.query(CompletedCourse).all()
        completion_counts = {}
        for comp in completions:
            if comp.course_id not in completion_counts:
                completion_counts[comp.course_id] = 0
            completion_counts[comp.course_id] += 1
        
        # Build interaction data
        interactions = []
        for course_id in [c['id'] for c in courses_data]:
            interactions.append({
                'course_id': course_id,
                'clicks': click_counts.get(course_id, 0),
                'total_time_spent': 0,  # Would come from logs in production
                'completion_rate': completion_counts.get(course_id, 0) / max(1, len(users_data))
            })
        
        return pd.DataFrame(users_data), pd.DataFrame(courses_data), pd.DataFrame(interactions)
    
    def _engineer_features(self, users_df, courses_df, interactions_df):
        """Engineer features for all user-course pairs"""
        X_list = []
        y_list = []
        
        # Get completed courses for all users
        completed_courses = self.db.query(CompletedCourse).all()
        completed_set = {(cc.user_id, cc.course_id) for cc in completed_courses}
        
        # Create features for each user-course pair
        for user_idx, user in users_df.iterrows():
            user_id = user['id']
            
            # Calculate user features
            user_completed = len([c for uid, c in completed_set if uid == user_id])
            
            for course_idx, course in courses_df.iterrows():
                course_id = course['id']
                
                # Get interaction data
                interaction_row = interactions_df[interactions_df['course_id'] == course_id]
                if len(interaction_row) == 0:
                    continue
                
                # Extract features
                user_data = {
                    'level': 'Intermediate',
                    'courses_completed': user_completed,
                    'avg_rating': 0
                }
                
                # Use model's feature extraction
                try:
                    features, label = self.model.extract_features(
                        user_data,
                        pd.DataFrame([course]),
                        pd.DataFrame([interaction_row.iloc[0]])
                    )
                    
                    X_list.extend(features)
                    y_list.extend(label)
                except:
                    continue
        
        import numpy as np
        X = np.array(X_list) if X_list else np.zeros((0, 12))
        y = np.array(y_list) if y_list else np.array([])
        
        return X, y


def main():
    """Run training pipeline"""
    pipeline = TrainingPipeline()
    log = pipeline.run()
    
    # Print summary
    print("Training Log:")
    import json
    print(json.dumps(log, indent=2, default=str))


if __name__ == "__main__":
    main()
