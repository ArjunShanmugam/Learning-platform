"""
Health check and status endpoints for the application.
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime
import redis
import weaviate

from ..database import get_db
from ..monitoring import (
    request_count, active_requests, search_total,
    training_status, auth_attempts
)

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health_check(db: Session = Depends(get_db)):
    """
    Basic health check endpoint.
    Returns 200 if service is healthy.
    """
    try:
        # Check database connection
        db.execute("SELECT 1")
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "learning-platform-backend"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/detailed")
async def detailed_health(db: Session = Depends(get_db)):
    """
    Detailed health status with component checks.
    """
    status = {
        "timestamp": datetime.utcnow().isoformat(),
        "components": {}
    }
    
    # Database check
    try:
        db.execute("SELECT 1")
        status["components"]["database"] = "healthy"
    except Exception as e:
        status["components"]["database"] = f"unhealthy: {str(e)}"
    
    # Could add Redis, Weaviate checks here if configured
    
    # Overall status
    status["overall"] = "healthy" if all(
        v == "healthy" for v in status["components"].values()
    ) else "degraded"
    
    return status


@router.get("/metrics-summary")
async def metrics_summary():
    """
    Return a summary of key metrics.
    """
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "metrics": {
            "active_requests": active_requests._value.get(),
            "total_requests": sum(
                v for k, v in request_count._metrics.items()
            ),
            "total_searches": sum(
                v for k, v in search_total._metrics.items()
            ),
            "training_runs": sum(
                v for k, v in training_status._metrics.items()
            ),
            "auth_attempts": sum(
                v for k, v in auth_attempts._metrics.items()
            )
        }
    }


@router.get("/readiness")
async def readiness_check(db: Session = Depends(get_db)):
    """
    Kubernetes readiness probe endpoint.
    Returns 200 when service is ready to receive traffic.
    """
    try:
        db.execute("SELECT 1")
        return {"ready": True}
    except Exception:
        return {"ready": False}


@router.get("/liveness")
async def liveness_check():
    """
    Kubernetes liveness probe endpoint.
    Returns 200 if service is alive (even if not ready).
    """
    return {"alive": True}
