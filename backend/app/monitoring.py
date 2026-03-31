"""
Prometheus metrics middleware and utilities for FastAPI application.
"""
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi import Request, Response
from fastapi.routing import APIRoute
import time
import logging
from typing import Callable

logger = logging.getLogger(__name__)

# Define metrics
request_count = Counter(
    'app_requests_total',
    'Total requests to the application',
    ['method', 'endpoint', 'status']
)

request_duration = Histogram(
    'app_request_duration_seconds',
    'Request duration in seconds',
    ['method', 'endpoint'],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)
)

active_requests = Gauge(
    'app_active_requests',
    'Number of active requests'
)

# Search metrics
search_latency = Histogram(
    'search_latency_seconds',
    'Search latency in seconds',
    ['search_type'],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 5.0)
)

search_total = Counter(
    'search_total',
    'Total searches performed',
    ['search_type', 'status']
)

# Click-through rate metrics
clicks_total = Counter(
    'clicks_total',
    'Total clicks on recommendations',
    ['recommendation_type']
)

impressions_total = Counter(
    'impressions_total',
    'Total recommendations shown',
    ['recommendation_type']
)

# Training metrics
training_duration = Histogram(
    'training_duration_seconds',
    'Model training duration in seconds',
    buckets=(60, 120, 300, 600, 1200, 3600)
)

training_status = Counter(
    'training_total',
    'Total training runs',
    ['status']  # success, failure
)

# Database metrics
db_query_duration = Histogram(
    'db_query_duration_seconds',
    'Database query duration',
    ['query_type'],
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1.0)
)

# Authentication metrics
auth_attempts = Counter(
    'auth_attempts_total',
    'Total authentication attempts',
    ['method', 'status']  # login, signup; success, failure
)

# Model metrics
model_inference_duration = Histogram(
    'model_inference_duration_seconds',
    'Model inference duration',
    ['model_name'],
    buckets=(0.01, 0.05, 0.1, 0.5, 1.0)
)

recommendations_generated = Counter(
    'recommendations_generated_total',
    'Total recommendations generated',
    ['model_version']
)


async def metrics_middleware(request: Request, call_next: Callable) -> Response:
    """
    Middleware to track request metrics.
    """
    method = request.method
    endpoint = request.url.path
    
    active_requests.inc()
    start_time = time.time()
    
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception as e:
        status = 500
        raise
    finally:
        duration = time.time() - start_time
        active_requests.dec()
        
        # Record metrics
        request_count.labels(
            method=method,
            endpoint=endpoint,
            status=status
        ).inc()
        
        request_duration.labels(
            method=method,
            endpoint=endpoint
        ).observe(duration)
    
    return response


async def metrics_endpoint(request: Request) -> Response:
    """
    Prometheus metrics endpoint.
    """
    metrics = generate_latest()
    return Response(
        content=metrics,
        status_code=200,
        media_type=CONTENT_TYPE_LATEST
    )


class SearchMetrics:
    """Helper class for search-related metrics."""
    
    @staticmethod
    def record_search(search_type: str, duration: float, success: bool = True):
        """Record search metrics."""
        search_latency.labels(search_type=search_type).observe(duration)
        search_total.labels(
            search_type=search_type,
            status='success' if success else 'failure'
        ).inc()
    
    @staticmethod
    def record_ctr(recommendation_type: str, clicked: bool):
        """Record click-through metrics."""
        if clicked:
            clicks_total.labels(recommendation_type=recommendation_type).inc()
        impressions_total.labels(recommendation_type=recommendation_type).inc()


class TrainingMetrics:
    """Helper class for training-related metrics."""
    
    @staticmethod
    def record_training(duration: float, success: bool = True):
        """Record training metrics."""
        training_duration.observe(duration)
        training_status.labels(status='success' if success else 'failure').inc()


class DatabaseMetrics:
    """Helper class for database-related metrics."""
    
    @staticmethod
    def record_query(query_type: str, duration: float):
        """Record database query metrics."""
        db_query_duration.labels(query_type=query_type).observe(duration)


class AuthMetrics:
    """Helper class for authentication metrics."""
    
    @staticmethod
    def record_attempt(method: str, success: bool):
        """Record authentication attempt."""
        auth_attempts.labels(
            method=method,
            status='success' if success else 'failure'
        ).inc()


class ModelMetrics:
    """Helper class for model-related metrics."""
    
    @staticmethod
    def record_inference(model_name: str, duration: float):
        """Record model inference metrics."""
        model_inference_duration.labels(model_name=model_name).observe(duration)
    
    @staticmethod
    def record_recommendations(model_version: str, count: int = 1):
        """Record recommendations generated."""
        recommendations_generated.labels(model_version=model_version).inc(count)
