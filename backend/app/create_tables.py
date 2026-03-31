# backend/app/create_tables.py
"""
Safely create DB tables. Run as:
    python -m app.create_tables
"""

def create():
    try:
        from .db import engine
        from .models.base import Base
        from . import models
    except Exception as e:
        import sys
        print("Failed while importing DB or models in create_tables:", file=sys.stderr)
        print(repr(e), file=sys.stderr)
        raise

    try:
        Base.metadata.create_all(bind=engine)
    except Exception as e:
        import sys
        print("Failed while creating tables:", file=sys.stderr)
        print(repr(e), file=sys.stderr)
        raise

if __name__ == "__main__":
    create()
