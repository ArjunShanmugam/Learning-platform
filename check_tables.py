#!/usr/bin/env python3

from app.db import engine
from sqlalchemy import text

def check_tables():
    try:
        with engine.connect() as conn:
            result = conn.execute(text('SHOW TABLES'))
            tables = [row[0] for row in result]
            print("Tables in database:")
            for table in tables:
                print(f"  - {table}")
            
            # Check if users table exists
            if 'users' in tables:
                print("\n✅ Users table exists")
                # Check users count
                count_result = conn.execute(text('SELECT COUNT(*) FROM users'))
                user_count = count_result.scalar()
                print(f"   Users count: {user_count}")
            else:
                print("\n❌ Users table does NOT exist")
                
            # Check if courses table exists
            if 'courses' in tables:
                print("✅ Courses table exists")
                # Check courses count
                count_result = conn.execute(text('SELECT COUNT(*) FROM courses'))
                course_count = count_result.scalar()
                print(f"   Courses count: {course_count}")
            else:
                print("❌ Courses table does NOT exist")
                
    except Exception as e:
        print(f"Error checking tables: {e}")

if __name__ == "__main__":
    check_tables()
