import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DOTENV_PATH = os.path.join(ROOT_DIR, ".env")

if not os.path.exists(DOTENV_PATH):
    alt = os.path.abspath(os.path.join(ROOT_DIR, "..", ".env"))
    if os.path.exists(alt):
        DOTENV_PATH = alt

load_dotenv(dotenv_path=DOTENV_PATH, override=False)

DB_USER = os.getenv("DB_USER", "learning_user")
# docker-compose uses DB_PASSWORD; local .env uses DB_PASS — support both
DB_PASS = os.getenv("DB_PASSWORD") or os.getenv("DB_PASS", "learning_pass")
DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "learning")

try:
    SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=False, future=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base()
except Exception as e:
    print("Failed to create DB engine in app.db:", e, file=sys.stderr)
    raise

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
