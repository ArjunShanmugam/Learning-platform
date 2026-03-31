import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from .database import engine, Base
from .scheduler import start_scheduler
from .routes import (
    auth_routes, 
    course_routes, 
    interaction_routes,  
    log_routes, 
    recommend_routes, 
    search_routes, 
    skill_routes,
    autosuggest_routes,
    admin_routes
)

# Load environment variables
load_dotenv()

# Create database tables and start scheduler
@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    try:
        scheduler = start_scheduler()
    except Exception as e:
        scheduler = None
    
    yield
    
    if scheduler:
        try:
            scheduler.shutdown()
        except Exception as e:
            pass

# Initialize FastAPI app
app = FastAPI(
    title="Learning Platform API",
    description="API for the Learning Platform",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan
)

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition", "Content-Length"],
    max_age=600
)

# Include routers
app.include_router(auth_routes.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(course_routes.router, prefix="/api", tags=["Courses"])
app.include_router(interaction_routes.router, prefix="/api/interactions", tags=["Interactions"])
app.include_router(log_routes.router, prefix="/api")
app.include_router(recommend_routes.router, prefix="/api/recommendations", tags=["Recommendations"])
app.include_router(search_routes.router, prefix="/api")
app.include_router(skill_routes.router, prefix="/api")
app.include_router(autosuggest_routes.router, prefix="/api")
app.include_router(admin_routes.router, prefix="/api")

# Health check endpoint
@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "version": "1.0.0",
        "environment": os.getenv("ENVIRONMENT", "development")
    }

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Welcome to Learning Platform API",
        "docs": "/api/docs",
        "version": "1.0.0"
    }

# Custom exception handler for validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors(), "body": exc.body},
    )

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    try:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": str(exc)},
        )
    except (TypeError, ValueError) as serialization_error:
        # Handle serialization errors (e.g., bytes in exception)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error occurred"},
        )