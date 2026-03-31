"""
MLOps Workflow - Model Versioning, Deployment, and Monitoring
Handles model versioning, A/B testing, canary deployments, and performance tracking
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from enum import Enum
import json
from pathlib import Path
import joblib
import numpy as np

# ====== Data Classes ======

class ModelStatus(str, Enum):
    """Model deployment status"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    DEPRECATED = "deprecated"
    ROLLBACK = "rollback"

@dataclass
class ModelVersion:
    """Model version metadata"""
    version_id: str
    created_at: datetime
    status: ModelStatus
    metrics: Dict[str, float]
    feature_importance: Dict[str, float]
    training_data_size: int
    training_duration: float  # in seconds
    model_path: str
    parent_version: Optional[str] = None
    
    def to_dict(self):
        return {
            "version_id": self.version_id,
            "created_at": self.created_at.isoformat(),
            "status": self.status.value,
            "metrics": self.metrics,
            "feature_importance": self.feature_importance,
            "training_data_size": self.training_data_size,
            "training_duration": self.training_duration,
            "model_path": self.model_path,
            "parent_version": self.parent_version
        }

@dataclass
class DeploymentConfig:
    """Configuration for model deployment"""
    version_id: str
    deployment_strategy: str  # "direct", "canary", "shadow", "ab_test"
    canary_traffic_percentage: float = 10.0  # % of traffic for canary
    shadow_percentage: float = 50.0  # % of traffic for shadow mode
    ab_test_split: float = 50.0  # % traffic for A/B test
    rollback_on_degradation: bool = True
    performance_threshold: float = 0.05  # 5% degradation threshold

# ====== Model Registry ======

class ModelRegistry:
    """Central registry for model versions and metadata"""
    
    def __init__(self, registry_path: str = "ml/models/registry"):
        self.registry_path = Path(registry_path)
        self.registry_path.mkdir(parents=True, exist_ok=True)
        self.versions: Dict[str, ModelVersion] = self._load_versions()
        self.active_version: Optional[str] = self._load_active_version()
    
    def _load_versions(self) -> Dict[str, ModelVersion]:
        """Load all versions from registry"""
        versions = {}
        
        for metadata_file in self.registry_path.glob("metadata_*.json"):
            try:
                with open(metadata_file) as f:
                    data = json.load(f)
                    version_id = data['version_id']
                    versions[version_id] = ModelVersion(
                        version_id=version_id,
                        created_at=datetime.fromisoformat(data['created_at']),
                        status=ModelStatus(data['status']),
                        metrics=data['metrics'],
                        feature_importance=data['feature_importance'],
                        training_data_size=data['training_data_size'],
                        training_duration=data['training_duration'],
                        model_path=data['model_path'],
                        parent_version=data.get('parent_version')
                    )
            except Exception as e:
                print(f"Failed to load {metadata_file}: {e}")
        
        return versions
    
    def _load_active_version(self) -> Optional[str]:
        """Load currently active model version"""
        active_file = self.registry_path / "active.json"
        if active_file.exists():
            with open(active_file) as f:
                data = json.load(f)
                return data.get('active_version')
        return None
    
    def register_version(self, version: ModelVersion) -> bool:
        """Register a new model version"""
        try:
            metadata_file = self.registry_path / f"metadata_{version.version_id}.json"
            with open(metadata_file, 'w') as f:
                json.dump(version.to_dict(), f, indent=2)
            
            self.versions[version.version_id] = version
            return True
        except Exception as e:
            print(f"Failed to register version: {e}")
            return False
    
    def set_active_version(self, version_id: str) -> bool:
        """Set a model version as active (production)"""
        if version_id not in self.versions:
            return False
        
        try:
            # Update previous active version status
            if self.active_version and self.active_version in self.versions:
                self.versions[self.active_version].status = ModelStatus.STAGING
            
            # Update new active version status
            self.versions[version_id].status = ModelStatus.PRODUCTION
            self.active_version = version_id
            
            # Save active version
            active_file = self.registry_path / "active.json"
            with open(active_file, 'w') as f:
                json.dump({'active_version': version_id}, f)
            
            return True
        except Exception as e:
            print(f"Failed to set active version: {e}")
            return False
    
    def get_version(self, version_id: str) -> Optional[ModelVersion]:
        """Get specific model version"""
        return self.versions.get(version_id)
    
    def list_versions(self, status: Optional[ModelStatus] = None) -> List[ModelVersion]:
        """List all versions, optionally filtered by status"""
        versions = list(self.versions.values())
        if status:
            versions = [v for v in versions if v.status == status]
        return sorted(versions, key=lambda v: v.created_at, reverse=True)
    
    def get_active_version(self) -> Optional[ModelVersion]:
        """Get currently active model version"""
        if self.active_version:
            return self.versions.get(self.active_version)
        return None

# ====== A/B Testing Framework ======

class ABTestManager:
    """Manages A/B testing between model versions"""
    
    def __init__(self, test_log_path: str = "ml/logs/ab_tests"):
        self.test_log_path = Path(test_log_path)
        self.test_log_path.mkdir(parents=True, exist_ok=True)
        self.active_tests: Dict[str, Dict] = {}
    
    def start_ab_test(self, version_a: str, version_b: str, 
                      traffic_split: float = 0.5) -> str:
        """Start A/B test between two model versions"""
        test_id = f"ab_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        test_config = {
            "test_id": test_id,
            "version_a": version_a,
            "version_b": version_b,
            "traffic_split": traffic_split,
            "start_time": datetime.now().isoformat(),
            "results_a": {"predictions": 0, "sum_score": 0, "user_ratings": []},
            "results_b": {"predictions": 0, "sum_score": 0, "user_ratings": []}
        }
        
        self.active_tests[test_id] = test_config
        return test_id
    
    def record_prediction(self, test_id: str, version: str, 
                         prediction_score: float, user_rating: Optional[float] = None):
        """Record prediction for A/B test"""
        if test_id not in self.active_tests:
            return
        
        test = self.active_tests[test_id]
        result_key = "results_a" if version == test["version_a"] else "results_b"
        
        test[result_key]["predictions"] += 1
        test[result_key]["sum_score"] += prediction_score
        
        if user_rating is not None:
            test[result_key]["user_ratings"].append(user_rating)
    
    def get_test_results(self, test_id: str) -> Optional[Dict]:
        """Get results of A/B test"""
        if test_id not in self.active_tests:
            return None
        
        test = self.active_tests[test_id]
        results_a = test["results_a"]
        results_b = test["results_b"]
        
        avg_score_a = results_a["sum_score"] / max(1, results_a["predictions"])
        avg_score_b = results_b["sum_score"] / max(1, results_b["predictions"])
        
        avg_rating_a = np.mean(results_a["user_ratings"]) if results_a["user_ratings"] else 0
        avg_rating_b = np.mean(results_b["user_ratings"]) if results_b["user_ratings"] else 0
        
        # Determine winner (higher average score and rating is better)
        score_diff = abs(avg_score_a - avg_score_b)
        rating_diff = abs(avg_rating_a - avg_rating_b)
        
        if score_diff > 0.05:  # 5% threshold
            winner = test["version_a"] if avg_score_a > avg_score_b else test["version_b"]
        else:
            winner = test["version_a"] if avg_rating_a > avg_rating_b else test["version_b"]
        
        return {
            "test_id": test_id,
            "version_a": test["version_a"],
            "version_b": test["version_b"],
            "results_a": {
                "predictions": results_a["predictions"],
                "avg_score": avg_score_a,
                "avg_rating": avg_rating_a
            },
            "results_b": {
                "predictions": results_b["predictions"],
                "avg_score": avg_score_b,
                "avg_rating": avg_rating_b
            },
            "winner": winner,
            "statistical_significance": score_diff > 0.05
        }
    
    def end_ab_test(self, test_id: str) -> Dict:
        """End A/B test and get final results"""
        results = self.get_test_results(test_id)
        
        if results:
            # Save results to file
            results_file = self.test_log_path / f"{test_id}_results.json"
            with open(results_file, 'w') as f:
                json.dump(results, f, indent=2)
            
            del self.active_tests[test_id]
        
        return results or {}

# ====== Drift Detection ======

class DriftDetector:
    """Monitors model performance drift"""
    
    def __init__(self, baseline_metrics: Dict[str, float], 
                 degradation_threshold: float = 0.05):
        self.baseline_metrics = baseline_metrics
        self.degradation_threshold = degradation_threshold
        self.performance_history: List[Dict] = []
    
    def check_drift(self, current_metrics: Dict[str, float]) -> Tuple[bool, Dict]:
        """
        Check if current metrics show drift from baseline
        Returns: (has_drift, drift_details)
        """
        drift_details = {
            "has_drift": False,
            "degraded_metrics": [],
            "degradation_severity": 0,
            "timestamp": datetime.now().isoformat()
        }
        
        for metric_name, baseline_value in self.baseline_metrics.items():
            if metric_name not in current_metrics:
                continue
            
            current_value = current_metrics[metric_name]
            degradation = (baseline_value - current_value) / baseline_value if baseline_value != 0 else 0
            
            if degradation > self.degradation_threshold:
                drift_details["has_drift"] = True
                drift_details["degraded_metrics"].append({
                    "metric": metric_name,
                    "baseline": baseline_value,
                    "current": current_value,
                    "degradation": degradation
                })
                drift_details["degradation_severity"] = max(
                    drift_details["degradation_severity"],
                    degradation
                )
        
        self.performance_history.append(drift_details)
        return drift_details["has_drift"], drift_details
    
    def get_drift_alerts(self) -> List[Dict]:
        """Get all drift alerts"""
        return [d for d in self.performance_history if d["has_drift"]]

# ====== Canary Deployment ======

class CanaryDeployer:
    """Manages canary deployments"""
    
    def __init__(self):
        self.active_deployments: Dict[str, DeploymentConfig] = {}
        self.traffic_routing: Dict[str, Dict] = {}
    
    def start_canary_deployment(self, config: DeploymentConfig) -> str:
        """Start canary deployment"""
        deployment_id = f"canary_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.active_deployments[deployment_id] = config
        self.traffic_routing[deployment_id] = {
            "primary": "old",
            "canary": config.version_id,
            "traffic_split": config.canary_traffic_percentage / 100.0
        }
        
        return deployment_id
    
    def route_request(self, deployment_id: str) -> str:
        """Determine which version should handle request"""
        if deployment_id not in self.traffic_routing:
            return "primary"
        
        routing = self.traffic_routing[deployment_id]
        
        # Simple probabilistic routing based on traffic split
        if np.random.random() < routing["traffic_split"]:
            return routing["canary"]
        else:
            return routing["primary"]
    
    def promote_canary(self, deployment_id: str) -> bool:
        """Promote canary version to primary"""
        if deployment_id in self.active_deployments:
            config = self.active_deployments[deployment_id]
            # Here you would update the active model version
            del self.active_deployments[deployment_id]
            return True
        return False
    
    def rollback_canary(self, deployment_id: str) -> bool:
        """Rollback canary deployment"""
        if deployment_id in self.active_deployments:
            del self.active_deployments[deployment_id]
            return True
        return False

# ====== Model Performance Tracker ======

class PerformanceTracker:
    """Tracks model performance over time"""
    
    def __init__(self, tracking_path: str = "ml/logs/performance"):
        self.tracking_path = Path(tracking_path)
        self.tracking_path.mkdir(parents=True, exist_ok=True)
    
    def record_metrics(self, version_id: str, metrics: Dict[str, float]):
        """Record performance metrics for a version"""
        timestamp = datetime.now().isoformat()
        
        record = {
            "version_id": version_id,
            "timestamp": timestamp,
            "metrics": metrics
        }
        
        # Append to tracking file
        tracking_file = self.tracking_path / f"{version_id}_performance.jsonl"
        with open(tracking_file, 'a') as f:
            f.write(json.dumps(record) + '\n')
    
    def get_performance_trend(self, version_id: str, 
                             limit: int = 100) -> List[Dict]:
        """Get performance trend for a version"""
        tracking_file = self.tracking_path / f"{version_id}_performance.jsonl"
        
        if not tracking_file.exists():
            return []
        
        records = []
        with open(tracking_file) as f:
            for line in f:
                records.append(json.loads(line))
        
        return records[-limit:]

if __name__ == "__main__":
    # Example usage
    registry = ModelRegistry()
    print(f"Active version: {registry.active_version}")
    print(f"Total versions: {len(registry.versions)}")
    
    # Example A/B test
    ab_manager = ABTestManager()
    test_id = ab_manager.start_ab_test("v1.0", "v2.0")
    print(f"Started A/B test: {test_id}")
