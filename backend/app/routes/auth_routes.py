from datetime import timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Request, Form
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..database import get_db
from ..models import User, UserProfile
from ..auth.oauth2 import (
    get_password_hash,
    verify_password,
    create_token_response,
    get_current_user,
    get_current_active_user
)
from ..schemas.token import Token, TokenData
from ..schemas.user import UserCreate, UserInDB, UserResponse

router = APIRouter(prefix="", tags=["authentication"])

class LoginForm:
    def __init__(
        self,
        username: str = Form(...),
        password: str = Form(...)
    ):
        self.username = username
        self.password = password

@router.post("/login", response_model=Token)
@router.post("/token", response_model=Token)  # Keep for backward compatibility
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """
    OAuth2 compatible token login, get an access token for future requests
    """
    # Find user by email
    user = db.query(User).filter(
        User.email == form_data.username
    ).first()
    
    # Verify user exists and password is correct
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Check if user is active
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Inactive user"
        )
    
    # Create and return token response
    return create_token_response(user)

class UserSignupRequest(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)
    full_name: Optional[str] = Field(None, min_length=1, max_length=255)

@router.post("/signup", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def signup(
    request: Request,
    email: Optional[str] = Form(None),
    password: Optional[str] = Form(None),
    full_name: Optional[str] = Form(None),
    db: Session = Depends(get_db)
):
    """
    Create a new user account
    """
    # Handle both JSON and form data
    try:
        # Try to get JSON data first
        import json
        body = await request.body()
        if body:
            data = json.loads(body.decode('utf-8'))
            email = data.get('email', email)
            password = data.get('password', password)
            full_name = data.get('full_name', full_name)
    except:
        # If JSON parsing fails, use form data (already provided)
        pass
    
    # Validate required fields
    if not email or not password:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email and password are required"
        )
    
    # Check if user already exists
    existing_user = db.query(User).filter(User.email == email).first()
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    try:
        # Create new user
        hashed_password = get_password_hash(password)
        user = User(
            email=email,
            hashed_password=hashed_password,
            role="user",  # Default role
            is_active=True
        )
        
        db.add(user)
        db.commit()
        db.refresh(user)
        
        # Create user profile
        profile = UserProfile(
            user_id=user.id,
            full_name=full_name if full_name else None,
            bio="",
            profile_picture=""
        )
        db.add(profile)
        db.commit()
        
        return {
            "id": user.id,
            "email": user.email,
            "is_active": user.is_active,
            "role": user.role,
            "created_at": user.created_at.isoformat() if user.created_at else None,
            "profile": {
                "full_name": profile.full_name,
                "bio": profile.bio,
                "profile_picture": profile.profile_picture
            }
        }
        
    except IntegrityError as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Error creating user"
        )


@router.get("/me")
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    """
    Get current user details
    """
    return {
        "id": current_user.id,
        "email": current_user.email,
        "is_active": current_user.is_active,
        "role": current_user.role,
        "created_at": current_user.created_at.isoformat() if current_user.created_at else None,
        "profile": {
            "full_name": current_user.profile.full_name if current_user.profile else "",
            "bio": current_user.profile.bio if current_user.profile else "",
            "profile_picture": current_user.profile.profile_picture if current_user.profile else ""
        }
    }

@router.get("/users/{user_id}/profile")
async def get_user_profile(user_id: int, db: Session = Depends(get_db)):
    """
    Get user profile including skill level
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    profile = db.query(UserProfile).filter(UserProfile.user_id == user_id).first()
    
    return {
        "user_id": user.id,
        "email": user.email,
        "skill_level": profile.skill_level if profile else "Beginner",
        "career_path": profile.career_path if profile else "General",
        "full_name": profile.full_name if profile else "",
        "bio": profile.bio if profile else "",
        "profile_picture": profile.profile_picture if profile else ""
    }