from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Generator, TypeVar
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session
import os
from dotenv import load_dotenv
import hashlib
import hmac

from ..models.user import User

load_dotenv()

from ..database import SessionLocal, get_db
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-here-please-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "1440"))  # 24 hours by default
HASH_SALT = os.getenv("HASH_SALT", "learning-platform-salt-key")

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="auth/login",
    auto_error=False
)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against a hash using HMAC-SHA256."""
    try:
        # Generate hash of the plain password using the same salt
        computed_hash = hashlib.pbkdf2_hmac(
            'sha256',
            plain_password.encode('utf-8'),
            HASH_SALT.encode('utf-8'),
            100000
        ).hex()
        # Compare with stored hash
        return hmac.compare_digest(computed_hash, hashed_password)
    except Exception as e:
        return False

def get_password_hash(password: str) -> str:
    """Generate a password hash using PBKDF2-HMAC-SHA256."""
    try:
        hashed = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            HASH_SALT.encode('utf-8'),
            100000
        ).hex()
        return hashed
    except Exception as e:
        return ""

def create_access_token(
    data: Dict[str, Any], 
    expires_delta: Optional[timedelta] = None
) -> str:
    """Create a JWT access token."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta if expires_delta 
        else timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire, "type": "access"})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(
    request: Request,
    token: Optional[str] = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> 'User':
    """Get the current user from the JWT token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    if not token:
        raise credentials_exception
        
    try:
        # Decode the token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        
        # Check token type
        if payload.get("type") != "access":
            raise credentials_exception
            
        email: str = payload.get("sub")
        if email is None:
            raise credentials_exception
            
        # Get user from database
        user = db.query(User).filter(User.email == email).first()
        if user is None:
            raise credentials_exception
            
        # Add user to request state for logging/monitoring
        request.state.user = user
        
        return user
        
    except JWTError as e:
        raise credentials_exception

async def get_current_active_user(
    current_user: 'User' = Depends(get_current_user)
) -> 'User':
    """Get the current active user."""
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user"
        )
    return current_user

def create_token_response(user: 'User') -> Dict[str, Any]:
    """Create a token response for a user."""
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": user.email,
            "user_id": user.id,
            "role": user.role
        },
        expires_delta=access_token_expires
    )
    
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "user_id": user.id,
        "email": user.email,
        "role": user.role,
        "expires_in": int(access_token_expires.total_seconds())
    }

async def get_current_user_optional(
    token: Optional[str] = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> Optional['User']:
    """Get the current user optionally - returns None if no token or invalid."""
    if not token:
        return None
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email: str = payload.get("sub")
        if email is None:
            return None
        
        user = db.query(User).filter(User.email == email).first()
        return user
    except JWTError:
        return None

async def require_admin(
    current_user: 'User' = Depends(get_current_user)
) -> 'User':
    """Dependency that requires admin role."""
    if current_user.role != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user
