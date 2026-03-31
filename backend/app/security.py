"""
Security hardening utilities for FastAPI application.
"""
from fastapi import Request, HTTPException, status
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import os
import re
from typing import Optional
import logging

logger = logging.getLogger(__name__)

limiter = Limiter(key_func=get_remote_address)


class SecurityHeaders:
    """Security headers to add to all responses."""
    
    SECURITY_HEADERS = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block",
        "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
        "Content-Security-Policy": "default-src 'self'",
        "Referrer-Policy": "strict-origin-when-cross-origin",
    }


class InputValidation:
    """Input validation and sanitization utilities."""
    
    @staticmethod
    def sanitize_email(email: str) -> str:
        """Validate and sanitize email."""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, email):
            raise ValueError("Invalid email format")
        return email.lower().strip()
    
    @staticmethod
    def sanitize_string(value: str, max_length: int = 500) -> str:
        """Sanitize string input."""
        if not isinstance(value, str):
            raise ValueError("Input must be string")
        
        value = value.strip()
        
        if len(value) > max_length:
            raise ValueError(f"Input exceeds maximum length of {max_length}")
        
        # Remove potentially harmful characters
        dangerous_chars = ['<', '>', '"', "'", ';', '--', '/*', '*/']
        for char in dangerous_chars:
            if char in value:
                logger.warning(f"Dangerous character detected: {char}")
                raise ValueError("Invalid character in input")
        
        return value
    
    @staticmethod
    def sanitize_number(value: int, min_val: int = 0, max_val: int = 1000000) -> int:
        """Validate number input."""
        if not isinstance(value, int):
            raise ValueError("Input must be integer")
        
        if not (min_val <= value <= max_val):
            raise ValueError(f"Value must be between {min_val} and {max_val}")
        
        return value


class EnvironmentSecurity:
    """Secure environment variable management."""
    
    @staticmethod
    def get_required_env(key: str) -> str:
        """Get required environment variable."""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable {key} not set")
        return value
    
    @staticmethod
    def get_optional_env(key: str, default: str = "") -> str:
        """Get optional environment variable."""
        return os.getenv(key, default)
    
    @staticmethod
    def get_bool_env(key: str, default: bool = False) -> bool:
        """Get boolean environment variable."""
        value = os.getenv(key, str(default)).lower()
        return value in ('true', '1', 'yes', 'on')


class PasswordSecurity:
    """Password validation and security utilities."""
    
    @staticmethod
    def validate_password(password: str) -> bool:
        """
        Validate password strength.
        Requirements:
        - Minimum 8 characters
        - At least one uppercase letter
        - At least one lowercase letter
        - At least one digit
        - At least one special character
        """
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters long")
        
        if not any(c.isupper() for c in password):
            raise ValueError("Password must contain at least one uppercase letter")
        
        if not any(c.islower() for c in password):
            raise ValueError("Password must contain at least one lowercase letter")
        
        if not any(c.isdigit() for c in password):
            raise ValueError("Password must contain at least one digit")
        
        special_chars = r'[!@#$%^&*(),.?":{}|<>]'
        if not re.search(special_chars, password):
            raise ValueError("Password must contain at least one special character")
        
        return True


class SQLInjectionProtection:
    """SQL injection prevention utilities."""
    
    DANGEROUS_KEYWORDS = [
        'DROP', 'DELETE', 'INSERT', 'UPDATE', 'UNION',
        'SELECT', 'EXEC', 'EXECUTE', 'ALTER', 'CREATE'
    ]
    
    @staticmethod
    def check_for_sql_injection(value: str) -> bool:
        """Check if input contains potential SQL injection."""
        value_upper = value.upper()
        
        for keyword in SQLInjectionProtection.DANGEROUS_KEYWORDS:
            if keyword in value_upper:
                logger.warning(f"Potential SQL injection detected: {keyword}")
                return True
        
        return False


class CSRFProtection:
    """CSRF token validation (if not using SameSite cookies)."""
    
    @staticmethod
    def generate_csrf_token() -> str:
        """Generate CSRF token."""
        import secrets
        return secrets.token_urlsafe(32)
    
    @staticmethod
    def validate_csrf_token(request: Request, token: str) -> bool:
        """Validate CSRF token."""
        session_token = request.session.get('csrf_token')
        return session_token == token


class AuditLog:
    """Audit logging for security events."""
    
    @staticmethod
    def log_security_event(event_type: str, user_id: Optional[int] = None, 
                          details: dict = None, ip_address: str = None):
        """Log security event for auditing."""
        log_entry = {
            "event_type": event_type,
            "user_id": user_id,
            "ip_address": ip_address,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details or {}
        }
        logger.warning(f"SECURITY_EVENT: {log_entry}")


# Exception handlers
async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """Handle rate limit exceeded."""
    logger.warning(f"Rate limit exceeded for {request.client.host}")
    return HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Rate limit exceeded. Please try again later."
    )
