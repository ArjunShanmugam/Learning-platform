from datetime import datetime
from sqlalchemy import Column, Integer, DateTime
from sqlalchemy.ext.declarative import declared_attr, declarative_base
from sqlalchemy.orm import relationship

# Create a base class for declarative class definitions
Base = declarative_base()

class TimestampMixin:
    """Mixin that adds timestamp columns to models."""
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True)

class BaseModel(TimestampMixin):
    """Base model class that includes common functionality."""
    __abstract__ = True
    id = Column(Integer, primary_key=True, autoincrement=True)

    @declared_attr
    def __tablename__(cls):
        """
        Generate __tablename__ automatically from class name.
        Converts CamelCase class name to snake_case table name.
        Example: UserProfile -> user_profiles
        """
        import re
        # Convert CamelCase to snake_case and add 's' for pluralization
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', cls.__name__)
        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()
        return f"{name}s"

    def to_dict(self):
        """Convert model instance to dictionary."""
        result = {}
        for key in self.__mapper__.c.keys():
            if key in ['created_at', 'updated_at']:
                value = getattr(self, key)
                result[key] = value.isoformat() if value else None
            else:
                result[key] = getattr(self, key)
        return result

    def update(self, **kwargs):
        """Update model instance with given attributes."""
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
        return self